from pydantic import BaseModel
from typing import Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class FLA_Tradable_Bits(BaseModel):

    api_key: str
    api_secret: str 

    ## Import Pandera Schema
    input_schema: DataFrameModel = None

    @property
    def _headers(self) -> Dict:
        return {
            'Api-Key': self.api_key, 
            'Api-Secret': self.api_secret
        }
    
    @property
    def _base_url(self) -> str:
        return "https://tradablebits.com/api/v1/crm/"


    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_campaigns(self) -> pd.DataFrame:

        return self._get_dataframe(self._get_response(url = f"{self._base_url}/campaigns"))
    

    def get_fans(self) -> pd.DataFrame:

        ## Initial Request
        response = self._get_response(url=f"{self._base_url}/fans")
        search_uid = response.json()['meta']['search_uid']

        df = self._get_dataframe(response=response)

        ## Retrieve all
        while response.json()['data']:

            response = self._get_response(
                url=f"{self._base_url}/fans", 
                params={'search_uid': search_uid}
            )

            new_df = self._get_dataframe(response=response)
            df = pd.concat([df, new_df], ignore_index = True)

        return df 


    def get_activities(self, max_activity_id: int = None) -> pd.DataFrame:

        if max_activity_id:

            ## Initial Request
            response = self._get_response(url=f"{self._base_url}/activities")
            df = self._get_dataframe(response=response)

            ## Retrieve all
            while response.json()['data']:

                response = self._get_response(
                    url=f"{self._base_url}/activities", 
                    params={'max_activity_id': response.json()['meta']['min_activity_id']}
                )

                new_df = self._get_dataframe(response=response)
                df = pd.concat([df, new_df], ignore_index = True)

        else:

            response = self._get_response(
                url = f"{self._base_url}/activities", 
                params={'min_activity_id': str(max_activity_id)}
            )

            df = self._get_dataframe(response=response)

            ## Retrieve all
            while response.json()['data']:

                response = self._get_response(
                    url=f"{self._base_url}/activities", 
                    params={'min_activity_id': response.json()['meta']['max_activity_id']}
                )

                new_df = self._get_dataframe(response=response)
                df = pd.concat([df, new_df], ignore_index = True)


        return df 
    
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

        return self._create_session().get(url, headers=self._headers, params=params)
    
    def _get_dataframe(self, response: requests.Response) -> pd.DataFrame:

        if "data" in response.json():
            if self.input_schema:
                return DataFrame[self.input_schema](pd.json_normalize(response.json()['data']))
            else:
                return pd.json_normalize(response.json()['data'])
        else:
            if self.input_schema:
                return DataFrame[self.input_schema](pd.json_normalize(response.json()))
            else:
                return pd.json_normalize(response.json())