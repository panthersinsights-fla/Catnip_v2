from pydantic import BaseModel, SecretStr
from typing import Dict, List, Literal

import pandas as pd
from pandera import DataFrameModel

from catnip.fla_requests import FLA_Requests
import httpx
import json 

import time

class FLA_Gameday(BaseModel):

    api_key: SecretStr
    prod_type: Literal["prod", "pre-prod", "dev"]

    input_schema: DataFrameModel = None
    output_schema: DataFrameModel = None

    @property
    def _base_url(self): # api.pre-prod.flapanthersgameday.com
        return f"https://api.{self.prod_type}.flapanthersgameday.com"
    
    
    @property
    def _base_headers(self) -> Dict[str, str]:

        return {
            "x-api-key": self.api_key.get_secret_value(),
            "Content-Type": "application/json"
        }
    
    def post_members(self, df: pd.DataFrame, batch_size: int = 25) -> List[httpx.Response]:

        responses = []

        with FLA_Requests().create_session() as session:

            # iterate over every variable batch_size of records
            for i in range(0, len(df), batch_size):
                
                # create batch
                batch = df.iloc[i:i+batch_size]

                # create payload
                payload = {
                    "members": [
                        {**member_dict} for member_dict in batch.to_dict('records')
                    ]
                }
                payload = json.dumps(payload)

                try:
                    response = session.post(
                        url = f"{self._base_url}/add-members",
                        headers = self._base_headers,
                        data = payload
                    )

                    response.raise_for_status()
                    responses.append(
                        {
                            "iteration": i / batch_size,
                            "status_code": response.status_code,
                            "response": response.json()
                        }
                    )

                except Exception as e:

                    # # Print request details
                    # print("REQUEST:")
                    # print(f"{response.request.method} {response.request.url}")
                    # print(f"Headers: {response.request.headers}")
                    # if response.request.content:
                    #     print(f"Body: {response.request.content.decode('utf-8')}")

                    # # Print response details
                    # print("\nRESPONSE:")
                    # print(f"Status Code: {response.status_code}")
                    # print(f"Headers: {response.headers}")
                    # print(f"Body: {response.text}")

                    print(response.status_code)
                    print(response.json())
                    print(f"Error: {e}")
                
                time.sleep(0.5)

        return responses