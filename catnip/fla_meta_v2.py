from pydantic import BaseModel, SecretStr
from typing import Optional

import hmac
import hashlib
import time
import json
from typing import Dict, List, Any, Literal, Generator

import httpx
import pandas as pd


class MetaConfig(BaseModel):
    """Configuration for the Meta Marketing API."""
    app_id: SecretStr
    app_secret: SecretStr
    ad_account_id: SecretStr
    api_version: str = "v20.0"

    # Optional fields for specific use cases
    user_id: Optional[SecretStr] = None
    page_id: Optional[SecretStr] = None



class MetaAPIError(Exception):
    """Custom exception for Meta API errors."""
    def __init__(self, message: str, status_code: int, response_body: str):
        self.message = message
        self.status_code = status_code
        self.response_body = response_body
        super().__init__(f"[{status_code}] {message}: {response_body}")



class MetaClient:
    """A client for interacting with the Meta Marketing API."""

    BASE_URL = "https://graph.facebook.com"
    BATCH_SIZE = 5000  # A more conservative batch size

    def __init__(self, config: MetaConfig, access_token: SecretStr):
        self.config = config
        self.access_token = access_token
        self._http_client = httpx.Client(base_url=f"{self.BASE_URL}/{self.config.api_version}")

    def _get_appsecret_proof(self) -> str:
        """Generates the appsecret_proof for authenticated calls."""
        return hmac.new(
            self.config.app_secret.get_secret_value().encode('utf8'),
            self.access_token.get_secret_value().encode('utf8'),
            hashlib.sha256
        ).hexdigest()

    def _base_params(self) -> Dict[str, str]:
        """Returns the base parameters required for every authenticated request."""
        return {
            "access_token": self.access_token.get_secret_value(),
            "appsecret_proof": self._get_appsecret_proof(),
        }

    def _request(
        self,
        method: Literal["GET", "POST", "DELETE"],
        path: str,
        **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Makes a request to the Meta API, handling authentication and error handling.
        """
        # Ensure base params are included
        params = self._base_params()
        if 'params' in kwargs:
            params.update(kwargs.pop('params'))
        
        try:
            response = self._http_client.request(method, path, params=params, **kwargs)
            response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx responses
            return response.json()
        except httpx.HTTPStatusError as e:
            raise MetaAPIError(
                message=f"API request to '{e.request.url}' failed",
                status_code=e.response.status_code,
                response_body=e.response.text,
            ) from e
        except json.JSONDecodeError as e:
            raise MetaAPIError(
                message="Failed to decode JSON from API response",
                status_code=response.status_code,
                response_body=response.text
            ) from e

    # --- Audience Methods ---

    def create_audience(self, name: str, description: str) -> Dict[str, Any]:
        """Creates a new custom audience."""
        path = f"/{self.config.ad_account_id.get_secret_value()}/customaudiences"
        data = {
            "name": name,
            "subtype": "CUSTOM",
            "description": description,
            "customer_file_source": "USER_PROVIDED_ONLY",
        }
        return self._request("POST", path, data=data)

    def get_audience_info(self, audience_id: str) -> Dict[str, Any]:
        """Retrieves information about a specific custom audience."""
        path = f"/{audience_id}"
        params = {
            "fields": "operation_status,time_updated,approximate_count_lower_bound,approximate_count_upper_bound",
        }
        return self._request("GET", path, params=params)

    # --- User Management Methods with Abstraction ---

    def _batch_user_operation(
        self,
        audience_id: str,
        df: pd.DataFrame,
        endpoint_path: str,
        http_method: Literal["POST", "DELETE"],
        extra_payload: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """A generic helper for batched user operations (add, delete, replace)."""
        responses = []
        schema = [col.upper() for col in df.columns]
        
        for batch_df in self._iter_batches(df):
            payload = {
                "schema": schema,
                "data": self._hash_dataframe_rows(batch_df)
            }
            if extra_payload:
                payload.update(extra_payload)

            # Meta API is quirky: payload is a JSON-encoded string within form-data
            data = {"payload": json.dumps(payload)}
            
            # The 'usersreplace' endpoint needs session data as separate form fields
            if 'session' in payload:
                data['session'] = json.dumps(payload['session'])

            path = f"/{audience_id}/{endpoint_path}"
            
            response = self._request(http_method, path, data=data)
            responses.append(response)
            time.sleep(1)  # Be a good API citizen

        return responses

    def add_audience_users(self, audience_id: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Adds users to a custom audience in batches."""
        # Note: The original 'upload_audience_users' used a batch API, which is more complex.
        # This is a simpler, more common single-endpoint-per-batch approach.
        # If batching multiple *different* API calls is required, a separate method is better.
        return self._batch_user_operation(audience_id, df, "users", "POST")

    def delete_audience_users(self, audience_id: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Deletes users from a custom audience in batches."""
        return self._batch_user_operation(audience_id, df, "users", "DELETE")
    
    def replace_audience_users(self, audience_id: str, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Replaces all users in a custom audience, using a session for atomicity."""
        responses = []
        session_id = int(time.time())
        schema = [col.upper() for col in df.columns]
        
        batches = list(self._iter_batches(df))
        num_batches = len(batches)

        for i, batch_df in enumerate(batches):
            session_payload = {
                "session_id": session_id,
                "batch_seq": i + 1,
                "last_batch_flag": i == num_batches - 1,
            }
            payload = {
                "schema": schema,
                "data": self._hash_dataframe_rows(batch_df),
            }
            
            data = {
                "payload": json.dumps(payload),
                "session": json.dumps(session_payload),
            }
            
            path = f"/{audience_id}/usersreplace"
            response = self._request("POST", path, data=data)
            responses.append(response)
            time.sleep(3) # A longer sleep for replacement operations is wise.
        
        return responses


    # --- Lead-Gen Methods ---

    def get_leadgen_forms(self, page_id: str, params: Dict = None) -> Dict[str, Any]:
        """Gets all lead generation forms for a given page."""
        if params is not None:
            return self._request("GET", f"/{page_id}/leadgen_forms", params=params)
        return self._request("GET", f"/{page_id}/leadgen_forms")

    def get_form_submissions(self, form_id: str) -> Dict[str, Any]:
        """Gets all lead submissions for a specific form."""
        return self._request("GET", f"/{form_id}/leads")


    # --- Helper & Utility Methods ---

    @staticmethod
    def _hash_value(value: Any) -> str:
        """SHA256 hashes a value as required by the Meta API."""
        return hashlib.sha256(str(value).encode('utf-8')).hexdigest()

    def _hash_dataframe_rows(self, df: pd.DataFrame) -> List[List[str]]:
        """Hashes all values in a DataFrame for API submission."""
        return [[self._hash_value(v) for v in row] for row in df.values.tolist()]

    def _iter_batches(self, df: pd.DataFrame) -> Generator[pd.DataFrame, None, None]:
        """Yields DataFrame chunks of BATCH_SIZE."""
        for i in range(0, len(df), self.BATCH_SIZE):
            yield df.iloc[i : i + self.BATCH_SIZE]