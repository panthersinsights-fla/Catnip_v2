from pydantic import BaseModel, SecretStr
from typing import Dict, List, Literal

import pandas as pd
from pandera import DataFrameModel

from catnip.fla_requests import FLA_Requests
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
    
    def post_members(self, df: pd.DataFrame, batch_size: int = 25) -> List[Dict]:

        responses = []
        service_rep_fields = ['service_rep_name', 'service_rep_email', 'service_rep_phone']

        with FLA_Requests().create_session() as session:

            # iterate over every variable batch_size of records
            for i in range(0, len(df), batch_size):
                
                # create batch
                batch = df.iloc[i:i+batch_size]

                # create payload
                payload = {
                    "members": [
                        {
                            **{k: v for k, v in row.items() if k not in service_rep_fields},
                            "serviceRepresentative": {
                                "fullName": row["service_rep_name"],
                                "emailAddress": row["service_rep_email"],
                                "phoneNumber": row["service_rep_phone"]
                            }
                        }
                        for row in batch.to_dict('records')
                    ]
                }
                payload = json.dumps(payload)

                try:
                    response = None
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

                    print(f"FAILED ITERATION: {i / batch_size}")

                    # Print request details
                    print("REQUEST:")
                    if response is not None:
                        print(f"{response.request.method} {response.request.url}")
                        print(f"Headers: {response.request.headers}")
                    print(f"Body: {payload}")
                    if response.request.content:
                        print(f"Body: {response.request.content.decode('utf-8')}")

                    # Print response details
                    print("\nRESPONSE:")
                    if response is not None:
                        print(f"Status Code: {response.status_code}")
                        print(f"Headers: {response.headers}")
                        print(f"Body: {response.text}")

                        print(response.status_code)
                    if response is not None:
                        try:
                            if response.headers.get("Content-Type", "").startswith("application/json"):
                                print(response.json())
                            else:
                                print(response.text)
                        except Exception as json_err:
                            print(f"JSON Decode Error: {json_err}")
                    print(f"Error: {e}")

                time.sleep(0.5)

        return responses


    def update_static_user_group(self, group_name: str, email_addresses: List[str], batch_size: int = 99) -> List[Dict]:

        """
        Updates a static user group with the provided email addresses.

        Args:
            group_name (str): The name of the user group to update.
            email_addresses (List[str]): A list of email addresses to add to the group.
            batch_size (int, optional): The number of email addresses to process per batch. Defaults to 99.

        Returns:
            List[Dict]: A list of response dictionaries for each batch processed.
        """

        responses = []

        with FLA_Requests().create_session() as session:

            # iterate over every variable batch_size of records
            for num_iteration, i in enumerate(range(0, len(email_addresses), batch_size)):

                # create batch
                batch = email_addresses[i:i+batch_size]

                # create payload
                payload = {
                    "groupName": group_name,
                    "emailAddresses": batch
                }
                payload = json.dumps(payload)

            
                try:
                    response = None
                    response = session.post(
                        url = f"{self._base_url}/update-static-user-group",
                        headers = self._base_headers,
                        data = payload
                    )

                    response.raise_for_status()
                    responses.append(
                        {
                            "iteration": num_iteration,
                            "status_code": response.status_code,
                            "response": response.json()
                        }
                    )
                
                except Exception as e:

                    print(f"FAILED ITERATION: {num_iteration}")

                    # Print request details
                    print("REQUEST:")
                    if response is not None:
                        print(f"{response.request.method} {response.request.url}")
                        print(f"Headers: {response.request.headers}")
                    print(f"Body: {payload}")
                    if response.request.content:
                        print(f"Body: {response.request.content.decode('utf-8')}")

                    # Print response details
                    print("\nRESPONSE:")
                    if response is not None:
                        print(f"Status Code: {response.status_code}")
                        print(f"Headers: {response.headers}")
                        print(f"Body: {response.text}")

                        print(response.status_code)
                    if response is not None:
                        try:
                            if response.headers.get("Content-Type", "").startswith("application/json"):
                                print(response.json())
                            else:
                                print(response.text)
                        except Exception as json_err:
                            print(f"JSON Decode Error: {json_err}")
                    print(f"Error: {e}")

                time.sleep(0.5)

        return responses