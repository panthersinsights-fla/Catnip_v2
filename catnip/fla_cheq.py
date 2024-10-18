from pydantic import BaseModel, SecretStr
from typing import List, Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
from catnip.fla_requests import FLA_Requests
from datetime import datetime
import time 

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
        with FLA_Requests().create_session() as session:
            while not end:

                # request
                print(f"Loading Page #{page}")
                
                response = session.request(
                    method = "GET",
                    url = f"{self._base_url}/orders",
                    headers = self._base_headers,
                    params = {"page": page},
                    json = data
                )
                # print(response.json())

                if response.status_code == 503:
                    time.sleep(2)
                    continue

                else:

                    if not response.json()['results']:
                        break

                    try:
                        # update variables
                        end = response.json()['end']
                        page += 1
                        responses.append(response)

                    except Exception as e:
                        print(f"ERROR: {e}")
                        print(response.json())

        return responses

    def get_menus(self) -> pd.DataFrame:

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

                # print(response); print(response.json())

            if response.status_code == 503:
                time.sleep(2)
                continue
            
            try: 
                # update variables
                end = response.json()['end']
                page += 1
                responses.append(response)
            except Exception as e:
                print(response)
                print(response.json())
                print(f"Error: {e}")
                break
        
        # create dataframe
        if self.input_schema:
            df = DataFrame[self.input_schema]([d for response in responses for d in response.json()['results']])
        else:    
            df = pd.DataFrame([d for response in responses for d in response.json()['results']])
        
        print(df)
        
        return df