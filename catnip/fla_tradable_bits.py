from pydantic import BaseModel, SecretStr
from typing import Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from datetime import datetime

class FLA_Tradable_Bits(BaseModel):

    api_key: SecretStr
    api_secret: SecretStr 

    ## Import Pandera Schema
    input_schema: DataFrameModel = None

    @property
    def _headers(self) -> Dict:
        return {
            'Api-Key': self.api_key.get_secret_value(), 
            'Api-Secret': self.api_secret.get_secret_value()
        }
    
    @property
    def _base_url(self) -> str:
        return "https://tradablebits.com/api/v1/crm/"


    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_businesses(self) -> pd.DataFrame:

        return self._get_dataframe(self._get_response(url = f"{self._base_url}/businesses"))
    
    def get_campaigns(self) -> pd.DataFrame:

        return self._get_dataframe(self._get_response(url = f"{self._base_url}/campaigns"))
    

    def get_fans(self) -> pd.DataFrame:

        ### Initial Request #####################################
        response = self._get_response(
            url=f"{self._base_url}/fans", 
            params={"limit": 200}
        )

        search_uid = response.json()['meta']['search_uid']
        responses = response.json()['data']

        ### Request rest #######################################
        while response.json()['data']:

            response = self._get_response(
                url=f"{self._base_url}/fans", 
                params={
                    "limit": 200,
                    "search_uid": search_uid
                }
            )

            if response.json()['data']:
                responses = [*responses, *response.json()['data']]    
            
        print(f"# responses: {len(responses)}")

        ## 2024119 - adhoc removal
        def modify_birth_dates(dict_list):
            for dictionary in dict_list:
                if 'birth_date' in dictionary:
                    birth_date = dictionary['birth_date']
                    if not pd.isnull(birth_date):
                        try:
                            date = datetime.strptime(birth_date, '%Y-%m-%d')
                            if date.year < 1900:
                                dictionary['birth_date'] = None
                        except ValueError:
                            # Handle invalid date formats
                            dictionary['birth_date'] = None
                            pass
            return dict_list
        
        responses = modify_birth_dates(responses)

        ### Create dataframe ####################################
        if self.input_schema:
            df = DataFrame[self.input_schema](pd.json_normalize(responses))
        else:
            df = pd.json_normalize(responses)
        
        return df 


    def get_activities(self, max_activity_id: int = None) -> pd.DataFrame:

        ### Initial request ######################################
        response = self._get_response(
            url=f"{self._base_url}/activities",
            params={
                "limit": 200,
                "min_activity_id": max_activity_id if max_activity_id is not None else 1
            }
        )

        #df = self._get_dataframe(response=response)
        responses = response.json()['data']
        min_activity_id = response.json()['meta']['max_activity_id']

        ### Request rest #########################################
        while response.json()['data']:

            response = self._get_response(
                url=f"{self._base_url}/activities",
                params={
                    "limit": 200,
                    "min_activity_id": min_activity_id
                }
            )

            min_activity_id = response.json()['meta']['max_activity_id']

            if response.json()['data']:
                responses = [*responses, *response.json()['data']] 

        print(f"# responses: {len(responses)}")

        if len(responses) == 0:
            return None
        
        else:

            ### Create dataframe ####################################
            if self.input_schema:
                df = DataFrame[self.input_schema](pd.json_normalize(responses))
            else:
                df = pd.json_normalize(responses)
            
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

        with self._create_session() as session:

            response = session.get(
                url = url,
                headers = self._headers,
                params = params
            )

        return response
    
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