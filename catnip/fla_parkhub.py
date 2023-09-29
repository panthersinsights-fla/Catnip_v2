from pydantic import BaseModel, SecretStr
from typing import Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from base64 import b64encode
from time import sleep


class FLA_Parkhub(BaseModel):

    username: SecretStr
    password: SecretStr

    api_key: SecretStr
    organization_id: SecretStr
    landmark_id: SecretStr
    lot_dict: Dict = {}

    sandbox_source: str = "partner-integration"

    ## Import Pandera Schema
    input_schema: DataFrameModel = None

    @property
    def _encoded_user_pw(self) -> str:
        return b64encode(f"{self.username.get_secret_value()}:{self.password.get_secret_value()}".encode()).decode()

    @property
    def _headers(self) -> Dict:
        return {
            "Authorization": f"Basic {self._encoded_user_pw}", 
            "Content-Type": "application/json",
            "x-api-key": f"{self.api_key.get_secret_value()}"
        }
    
    @property
    def _base_url(self) -> str:
        return "https://partners.v2.parkhub.com"


    #######################
    ### CLASS FUNCTIONS ###
    #######################
    
    def get_events(self) -> pd.DataFrame:

        '''
            - Need to get correct URL
        '''
        response = self._get_response(
            url = f"{self._base_url}/events/{self.organization_id}"
        )

        if self.input_schema:
            return DataFrame[self.input_schema](response.json()['events'])
        else:
            return pd.DataFrame(response.json()['events'])

    def get_lots(self) -> pd.DataFrame:

        response = self._get_response(
            url = f"{self._base_url}/lots/{self.organization_id}"
        )

        if self.input_schema:
            return DataFrame[self.input_schema](response.json()['lots'])
        else:
            return pd.DataFrame(response.json()['lots'])

    
    def get_reporting(self, date_from: str = "2021-07-01") -> pd.DataFrame:

        file_identifier = self._get_response(
            url = f"{self._base_url}/report/{self.organization_id.get_secret_value()}",
            params = {"dateFrom": date_from}
        ).json()['fileIdentifier']

        response = self._get_response(
            url = f"{self._base_url}/report/{self.organization_id.get_secret_value()}/status/{file_identifier}",
        )

        while response.json()['status'] == "PENDING":

            print("Processing - Requesting..")
            print(f"Status: {response.json()['status']}")

            response = self._get_response(
                url = f"{self._base_url}/report/{self.organization_id.get_secret_value()}/status/{file_identifier}",
            )

            if response.json()['status'] == "COMPLETED":
                
                if self.input_schema:
                    return DataFrame[self.input_schema](pd.read_csv(response.json()['url']))
                else:
                    return pd.read_csv(response.json()['url'])
            
            sleep(2)

        return None
    
    def get_site_status(self) -> Dict:

        response = self._get_response(
            url = f"{self._base_url}/status"
        )

        return response.json()
    

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _create_session(self) -> requests.Session:

        session = requests.Session()
        retry = Retry(total = 5, backoff_factor = 0.5)
        adapter = HTTPAdapter(max_retries = retry)
        session.mount('https://', adapter)

        return session

    def _get_response(self, url: str, params: Dict = {}) -> requests.Response:

        with self._create_session() as session:

            response = session.get(
                url = url,
                headers = self._headers,
                params = params
            )

        return response