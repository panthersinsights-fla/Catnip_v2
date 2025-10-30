import httpx
import pandas as pd
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, SecretStr, root_validator
from pandera import DataFrameModel
from pandera.typing import DataFrame
import time
import json

############################################
### DOCUMENTATION ##########################
############################################
"""
Tableau REST API Documentation:
- Authentication: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_auth.htm
- Schedules: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm
- Jobs: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm
- Workbooks: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_workbooks_and_views.htm
- Projects: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm
- Users: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm
- Permissions: https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm
"""
############################################
### CLASS ##################################
############################################

class FLA_Tableau(BaseModel):
    """
    Tableau REST API wrapper following catnip style class creation.
    
    Supports both personal access token and username/password authentication.
    Provides methods for managing schedules, jobs, workbooks, projects, users, and permissions.
    """
    
    # Authentication options
    server_url: str
    site_id: Optional[str] = None  # If None, uses default site
    personal_access_token_name: Optional[str] = None
    personal_access_token: Optional[SecretStr] = None
    username: Optional[str] = None
    password: Optional[SecretStr] = None
    
    # Optional configuration
    api_version: str = "3.22"  # Tableau REST API version
    input_schema: DataFrameModel = None
    auth_token: Optional[str] = None
    
    @root_validator
    def validate_auth_method(cls, values):
        """Ensure either personal access token OR (username, password) are provided."""
        pat_name = values.get('personal_access_token_name')
        pat_token = values.get('personal_access_token')
        username = values.get('username')
        password = values.get('password')
        
        has_pat = pat_name is not None and pat_token is not None
        has_credentials = username is not None and password is not None
        
        if not has_pat and not has_credentials:
            raise ValueError(
                "Must provide either:\n"
                "  - personal_access_token_name and personal_access_token, OR\n"
                "  - username and password"
            )
        
        return values
    
    @property
    def _base_url(self) -> str:
        """Base URL for Tableau REST API."""
        return f"{self.server_url}/api/{self.api_version}"
    
    @property
    def _site_url(self) -> str:
        """Site-specific URL."""
        # If we have a concrete site_id, use it; otherwise default site
        if self.site_id:
            return f"{self._base_url}/sites/{self.site_id}"
        return f"{self._base_url}/sites/default"
    
    @property
    def _authenticate(self) -> str:
        """Get or create Tableau auth token via JSON sign-in for both PAT and user/pass."""
        if self.auth_token:
            return self.auth_token
        return self._sign_in()
    
    def _sign_in(self) -> str:
        """Sign in using JSON (supports PAT or username/password) and cache token."""
        if self.personal_access_token and self.personal_access_token_name:
            payload: Dict[str, Any] = {
                "credentials": {
                    "personalAccessTokenName": self.personal_access_token_name,
                    "personalAccessTokenSecret": self.personal_access_token.get_secret_value(),
                    "site": {"contentUrl": ""}
                }
            }
        else:
            payload = {
                "credentials": {
                    "name": self.username,
                    "password": self.password.get_secret_value(),
                    "site": {"contentUrl": ""}
                }
            }

        response = httpx.post(
            f"{self._base_url}/auth/signin",
            json=payload,
            headers={"Content-Type": "application/json", "Accept": "application/json"}
        )
        response.raise_for_status()
        data = response.json()
        credentials = data.get("credentials", {})
        self.auth_token = credentials.get("token")
        # If site_id not provided, capture from sign-in response
        site_info = credentials.get("site", {})
        if not self.site_id and site_info.get("id"):
            self.site_id = site_info.get("id")
        return self.auth_token
    
    @property
    def _base_headers(self) -> Dict[str, str]:
        """Base headers for JSON API requests."""
        return {
            "X-Tableau-Auth": self._authenticate,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    ############################################
    ### SCHEDULES ENDPOINTS ####################
    ############################################
    
    def get_schedules(self) -> pd.DataFrame:
        """
        Get all schedules (extract refresh and subscription schedules).
        
        Returns:
            DataFrame with schedule information including name, state, frequency, etc.
        """
        df = self._get_paginated_df("schedules")
        return self._create_dataframe(df)
    
    def get_extract_refresh_tasks(self) -> pd.DataFrame:
        """
        Get extract refresh tasks for the site.
        
        Returns:
            DataFrame with extract refresh task information.
        """
        df = self._get_paginated_df("tasks/extractRefreshes")
        return self._create_dataframe(df)
    
    ############################################
    ### JOBS ENDPOINTS #########################
    ############################################
    
    def get_jobs(self) -> pd.DataFrame:
        """
        Get all active jobs on the site.
        
        Returns:
            DataFrame with job information including type, status, and duration.
        """
        df = self._get_paginated_df("jobs")
        # Derive duration where possible if fields exist
        if 'startedAt' in df.columns and 'completedAt' in df.columns:
            with pd.option_context('mode.chained_assignment', None):
                try:
                    df['duration_seconds'] = (
                        pd.to_datetime(df['completedAt']) - pd.to_datetime(df['startedAt'])
                    ).dt.total_seconds()
                except Exception:
                    pass
        return self._create_dataframe(df)
    
    def get_job_details(self, job_id: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific job.
        
        Args:
            job_id: The ID of the job to query
            
        Returns:
            Dictionary with detailed job information
        """
        response = httpx.get(
            f"{self._site_url}/jobs/{job_id}",
            headers=self._base_headers
        )
        response.raise_for_status()
        data = response.json()
        df = self._normalize_json(data)
        return df.iloc[0].to_dict() if not df.empty else {}
    
    ############################################
    ### CONTENT ENDPOINTS ######################
    ############################################
    
    def get_workbooks(self, include_usage: bool = True) -> pd.DataFrame:
        """
        Get all workbooks on the site.
        
        Args:
            include_usage: Whether to include usage statistics
            
        Returns:
            DataFrame with workbook information
        """
        params = {}
        if include_usage:
            params['includeUsage'] = 'true'
        
        response = httpx.get(
            f"{self._site_url}/workbooks",
            headers=self._base_headers,
            params=params
        )
        response.raise_for_status()
        
        data = response.json()
        return self._create_dataframe(self._normalize_json(data))
    
    def get_projects(self) -> pd.DataFrame:
        """
        Get all projects on the site.
        
        Returns:
            DataFrame with project information
        """
        df = self._get_paginated_df("projects")
        return self._create_dataframe(df)
    
    def get_views(self, include_usage: bool = True) -> pd.DataFrame:
        """
        Get all views on the site.
        
        Args:
            include_usage: Whether to include usage statistics
            
        Returns:
            DataFrame with view information
        """
        params = {}
        if include_usage:
            params['includeUsage'] = 'true'
        
        response = httpx.get(
            f"{self._site_url}/views",
            headers=self._base_headers,
            params=params
        )
        response.raise_for_status()
        
        df = self._get_paginated_df("views", params=params)
        return self._create_dataframe(df)
    
    ############################################
    ### UPDATE ENDPOINTS #######################
    ############################################
    
    def update_workbook_name(self, workbook_id: str, new_name: str) -> Dict[str, Any]:
        """
        Update the name of a workbook.
        
        Args:
            workbook_id: The ID of the workbook to update
            new_name: The new name for the workbook
            
        Returns:
            Dictionary with updated workbook information
        """
        payload = {"workbook": {"name": new_name}}
        response = httpx.put(
            f"{self._site_url}/workbooks/{workbook_id}",
            json=payload,
            headers=self._base_headers
        )
        response.raise_for_status()
        data = response.json()
        df = self._normalize_json(data)
        return df.iloc[0].to_dict() if not df.empty else {}
    
    ############################################
    ### PERMISSIONS ENDPOINTS ##################
    ############################################
    
    def get_workbook_permissions(self, workbook_id: str) -> pd.DataFrame:
        """
        Get permissions for a specific workbook.
        
        Args:
            workbook_id: The ID of the workbook
            
        Returns:
            DataFrame with permission information
        """
        response = httpx.get(
            f"{self._site_url}/workbooks/{workbook_id}/permissions",
            headers=self._base_headers
        )
        response.raise_for_status()
        data = response.json()
        df = self._normalize_json(data)
        # Ensure workbook_id is present if missing
        if not df.empty and 'workbook_id' not in df.columns:
            df['workbook_id'] = workbook_id
        return self._create_dataframe(df)
    
    def get_project_permissions(self, project_id: str) -> pd.DataFrame:
        """
        Get permissions for a specific project.
        
        Args:
            project_id: The ID of the project
            
        Returns:
            DataFrame with permission information
        """
        response = httpx.get(
            f"{self._site_url}/projects/{project_id}/permissions",
            headers=self._base_headers
        )
        response.raise_for_status()
        data = response.json()
        df = self._normalize_json(data)
        if not df.empty and 'project_id' not in df.columns:
            df['project_id'] = project_id
        return self._create_dataframe(df)
    
    ############################################
    ### USERS AND GROUPS ENDPOINTS #############
    ############################################
    
    def get_users(self) -> pd.DataFrame:
        """
        Get all users on the site.
        
        Returns:
            DataFrame with user information
        """
        df = self._get_paginated_df("users")
        return self._create_dataframe(df)
    
    def get_groups(self) -> pd.DataFrame:
        """
        Get all groups on the site.
        
        Returns:
            DataFrame with group information
        """
        df = self._get_paginated_df("groups")
        return self._create_dataframe(df)
    
    def get_group_users(self, group_id: str) -> pd.DataFrame:
        """
        Get users in a specific group.
        
        Args:
            group_id: The ID of the group
            
        Returns:
            DataFrame with user information for the group
        """
        response = httpx.get(
            f"{self._site_url}/groups/{group_id}/users",
            headers=self._base_headers
        )
        response.raise_for_status()
        data = response.json()
        df = self._normalize_json(data)
        if not df.empty and 'group_id' not in df.columns:
            df['group_id'] = group_id
        return self._create_dataframe(df)
    
    ############################################
    ### HELPER METHODS #########################
    ############################################
    
    def _create_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create DataFrame with optional schema validation.
        
        Args:
            df: DataFrame to process
            
        Returns:
            Processed DataFrame
        """
        if self.input_schema:
            return DataFrame[self.input_schema](df)
        else:
            return df
    
    def sign_out(self) -> None:
        """
        Sign out from Tableau Server.
        """
        if self.auth_token:
            httpx.post(
                f"{self._base_url}/auth/signout",
                headers=self._base_headers
            )
            self.auth_token = None

    def _normalize_json(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        Normalize a Tableau JSON response into a flat DataFrame.
        - If the payload contains a list under a nested dict (common pattern), normalize that list.
        - If the payload is a single dict, return a single-row DataFrame.
        - Uses a best-effort heuristic to find the first list of dicts.
        """
        # If top-level is a list
        if isinstance(data, list):
            return pd.json_normalize(data, sep='_') if data else pd.DataFrame()

        # If top-level dict contains a nested list (e.g., {"workbooks": {"workbook": [ ... ]}})
        if isinstance(data, dict):
            # Direct list under any key
            for key, value in data.items():
                if isinstance(value, list):
                    return pd.json_normalize(value, sep='_') if value else pd.DataFrame()
                if isinstance(value, dict):
                    # One level deeper lists
                    for _, inner in value.items():
                        if isinstance(inner, list):
                            return pd.json_normalize(inner, sep='_') if inner else pd.DataFrame()
            # Fallback: single object
            return pd.json_normalize(data, sep='_')

        # Unknown structure fallback
        return pd.DataFrame()

    def _get_paginated_df(self, relative_path: str, params: Optional[Dict[str, Any]] = None, page_size: int = 1000) -> pd.DataFrame:
        """
        Fetch all pages for a given relative REST path and return a concatenated DataFrame.
        Uses Tableau pagination fields: pagination.pageNumber, pagination.pageSize, pagination.totalAvailable.
        Falls back to stopping when the current page returns fewer than page_size rows.
        """
        params = params.copy() if params else {}
        page_number = 1
        frames: List[pd.DataFrame] = []

        while True:
            page_params = {**params, 'pageSize': page_size, 'pageNumber': page_number}
            response = httpx.get(
                f"{self._site_url}/{relative_path}",
                headers=self._base_headers,
                params=page_params
            )
            response.raise_for_status()
            data = response.json()
            df_page = self._normalize_json(data)

            if df_page is None or df_page.empty:
                break

            frames.append(df_page)

            pagination = data.get('pagination', {}) if isinstance(data, dict) else {}
            total_available = pagination.get('totalAvailable')
            page_size_actual = pagination.get('pageSize', page_size)
            page_number_actual = pagination.get('pageNumber', page_number)

            # Stop if we've reached the end by totalAvailable
            if isinstance(total_available, int) and isinstance(page_size_actual, int) and isinstance(page_number_actual, int):
                if page_number_actual * page_size_actual >= total_available:
                    break
            else:
                # Fallback: if fewer rows than requested, assume last page
                if len(df_page) < page_size:
                    break

            page_number += 1

        if frames:
            return pd.concat(frames, ignore_index=True)
        return pd.DataFrame()