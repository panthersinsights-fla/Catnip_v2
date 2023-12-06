from pydantic import BaseModel, SecretStr
from typing import List, Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

from catnip.fla_requests import FLA_Requests
from urllib.parse import urlparse

from datetime import datetime

import hashlib
import hmac

class FLA_Purple(BaseModel):

    public_key:     SecretStr
    private_key:    SecretStr
    venue_id:       int

    # pandera schema
    input_schema:   DataFrameModel = None 

    @property
    def _base_url(self) -> str:
        return f"https://region1.purpleportal.net/api/company/v1/venue/{self.venue_id}"
    
    @property
    def _base_headers(self) -> Dict:
        return {
            "Date": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
            "Content-Type": "application/json"
        }
    
    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_visitors(self, start_date: datetime, end_date: datetime = datetime.now()) -> pd.DataFrame:
        
        url = f"{self._base_url}/visitors?from={start_date.strftime('%Y%m%d')}&to={end_date.strftime('%Y%m%d')}"
        # url = f"{self._base_url}/visitors"

        headers = self._base_headers
        headers['X-API-Authorization'] = self._get_encrypted_signature(headers, url)

        with FLA_Requests().create_session() as session:

            response = session.get(
                url = url,
                headers = headers,
                # params = {
                #     "from": start_date.strftime("%Y%m%d"),
                #     "to": end_date.strftime("%Y%m%d")
                # }
            )

        print(response); print(response.json())

        return None

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _get_encrypted_signature(self, headers: Dict, url: str) -> str:

        signature: str = "\n".join([
            headers['Content-Type'], 
            urlparse(url).hostname, 
            f"{urlparse(url).path}?{urlparse(url).query}", 
            headers['Date'], 
            "", 
            ""
        ])

        signature_bytes = hmac.new(self.private_key.get_secret_value().encode(), signature.encode(), hashlib.sha256).digest()
        encrypted_signature = f"{self.public_key.get_secret_value()}:{signature_bytes.hex()}"

        return encrypted_signature