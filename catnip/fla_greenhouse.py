from typing import List, Optional, Dict, Any
from fla_requests import FLA_Requests
import requests
import time # For rate limiting (basic example)
import base64

# from pydantic import BaseModel, SecretStr
# from pandera import DataFrameModel
# import httpx

# class FLA_Greenhouse(BaseModel):

#     api_key: SecretStr

#     ## Import Pandera Schema
#     input_schema: DataFrameModel = None

#     @property
#     def _headers(self) -> Dict:
#         return {
#             "Authorization": f"Basic {self.api_key.get_secret_value()}"
#         }
    
#     @property
#     def _base_url(self) -> str:
#         return "https://harvest.greenhouse.io/v1"
    
#     @property
#     def _rate_limit_delay(self) -> int:
#         return 0.1
    
#     @property
#     def _session(self) -> httpx.Client:
#         return FLA_Requests().create_session()

class FLA_Greenhouse:
    BASE_URL = "https://harvest.greenhouse.io/v1"
    RATE_LIMIT_DELAY = 0.1 # seconds between requests

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = FLA_Requests().create_session()
        self.headers = { "Authorization": f'Basic {base64.b64encode(f"{self.api_key}:".encode()).decode()}'}
        self.last_request_time = 0

    def _get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        # Basic rate limiting
        time_since_last_request = time.monotonic() - self.last_request_time
        if time_since_last_request < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - time_since_last_request)

        url = f"{self.BASE_URL}/{endpoint}"
        response = self.session.get(url, headers=self.headers, params=params)
        self.last_request_time = time.monotonic()
        response.raise_for_status()
        return response.json()

    def _get_all_pages(self, endpoint: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Fetches all pages for a given endpoint.
        Greenhouse API uses Link headers for pagination.
        """
        all_data = []
        next_page_url = f"{self.BASE_URL}/{endpoint}"
        
        while next_page_url:
            response = self.session.get(next_page_url, headers=self.headers, params=params if next_page_url == f"{self.BASE_URL}/{endpoint}" else None)
            all_data.extend(response)
            
            next_page_url = None
            if 'link' in response.headers:
                links = requests.utils.parse_header_links(response.headers['link'])
                for link in links:
                    if link.get('rel') == 'next':
                        next_page_url = link['url']
                        break
            params = None # Clear params for subsequent pages if they are already in the next_page_url
        return all_data

    def get_all_jobs(self, **kwargs) -> List[Dict]:
        return self._get_all_pages("jobs", params=kwargs)

    def get_job(self, job_id: int, **kwargs) -> List[Dict]:
        return self._get(f"jobs/{job_id}", params=kwargs)
    
    def get_all_candidates(self, **kwargs) -> List[Dict]:
        return self._get_all_pages("candidates", params=kwargs)

    def get_candidate(self, candidate_id: int, **kwargs) -> List[Dict]:
        return self._get(f"candidates/{candidate_id}", params=kwargs)
    
    def get_all_applications(self, **kwargs) -> List[Dict]:
        return self._get_all_pages("applications", params=kwargs)

    def get_application(self, application_id: int, **kwargs) -> List[Dict]:
        return self._get(f"applications/{application_id}", params=kwargs)

    def get_all_job_posts(self, **kwargs) -> List[Dict]:
        return self._get_all_pages("job_posts", params=kwargs)

    def get_job_post(self, job_id: int, **kwargs) -> List[Dict]:
        return self._get(f"job_posts/{job_id}", params=kwargs)
    
    def download_attachment(self, attachment_url: int) -> bytes:
        response = self.session.get(attachment_url, headers=self.headers)
        response.raise_for_status()
        return response.content