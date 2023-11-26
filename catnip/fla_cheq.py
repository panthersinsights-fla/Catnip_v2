from pydantic import BaseModel, SecretStr
from typing import List, Dict

import pandas as pd
from pandera import DataFrameModel

import httpx
from catnip.fla_requests import FLA_Requests
from datetime import datetime, timedelta

class FLA_Cheq(BaseModel):

    api_key: SecretStr

    # Pandera
    input_schema: DataFrameModel = None

    @property
    def _base_url(self) -> str:
        return "https://api.cheq.tools/api"

    @property
    def _base_headers(self) -> Dict:
        return {
            "Content-Type": "application/json",
            "x-api-key": self.api_key.get_secret_value()
        }
    
    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_sales(
        self,
        start_date: datetime,
        end_date: datetime,
        payment_status: List[int] = list(range(1, 9))
    ) -> List[httpx.Response]:
        
        # initialize
        responses: List[httpx.Response] = []
        end = False 
        page = 1
        data = {
            "start_range": start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "end_range": end_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            "payment_status": payment_status
        }

        # iterate
        while not end:
            with FLA_Requests().create_session() as session:
                
                # request
                print(f"Loading Page #{page}")
                
                response = session.request(
                    method = "GET",
                    url = f"{self._base_url}/orders",
                    headers = self._base_headers,
                    params = {"page": page},
                    json = data
                )

                # update variables
                end = response.json()['end']
                page += 1
                responses.append(response)
            
            if page % 5 == 0:
                print(f"Loading Page #{page}")

        if len(responses) == 1:
            print(responses[0])
            print(responses[0].json())

        return responses

    def get_menu(self) -> pd.DataFrame:

        # initialize
        responses: List[httpx.Response] = []
        end = False 
        page = 1

        while not end:
            with FLA_Requests().create_session() as session:
                
                # request
                print(f"Loading Page #{page}")
                response = session.request(
                    method = "GET",
                    url = f"{self._base_url}/menus",
                    headers = self._base_headers,
                    params = {"page": page}
                )

                # update variables
                end = response.json()['end']
                page += 1
                responses.append(response)
        
        # create dataframe
        df = pd.DataFrame([response.json()['results'] for response in responses])
        print(df)
        
        return df