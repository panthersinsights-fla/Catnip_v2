from pydantic import BaseModel, SecretStr
from typing import Dict, List

import pandas as pd
from pandera import DataFrameModel

from catnip.fla_requests import FLA_Requests
import httpx
import json 

import time

class FLA_Gameday(BaseModel):

    api_key: SecretStr

    input_schema: DataFrameModel = None
    output_schema: DataFrameModel = None

    @property
    def _base_url(self):
        return "https://api.dev.flapanthersgameday.com"
    
    @property
    def _base_headers(self) -> Dict[str, str]:

        return {
            "x-api-key": self.api_key.get_secret_value(),
            "Content-Type": "application/json"
        }
    
    def post_members(self, df: pd.DataFrame, batch_size: int = 100) -> List[httpx.Response]:

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

                    print(response.status_code)
                    print(response.json())
                    print(f"Error: {e}")
                
                time.sleep(0.5)

        return responses