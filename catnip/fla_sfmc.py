from pydantic import BaseModel, SecretStr
from typing import List, Dict, Literal

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
import asyncio
import json


from datetime import datetime

class FLA_Sfmc(BaseModel):

    subdomain: SecretStr
    client_id: SecretStr
    client_secret: SecretStr
    account_id: SecretStr

    @property
    def _base_authentication_uri(self) -> str:
        return f"https://{self.subdomain.get_secret_value()}.auth.marketingcloudapis.com/"
    
    @property
    def _base_rest_uri(self) -> str:
        return f"https://{self.subdomain.get_secret_value()}.rest.marketingcloudapis.com/"
    
    @property
    def _base_headers(self) -> Dict:
        return {"Content-Type": "application/json"}
    
    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def update_data_extension(
        self,
        method: Literal["insert", "upsert"],
        external_key: str,
        df: pd.DataFrame,
        bearer_token: str
    ) -> List[httpx.Response]:
        
        # headers
        headers = self._base_headers
        headers['Authorization'] = f"Bearer {bearer_token}"

        # prepare df
        df = self._convert_datetime_columns(df)

        max_records_per_chunk = 10000
        num_chunks = len(df) // max_records_per_chunk + 1; print(num_chunks)
        smaller_dfs = [df.iloc[i * max_records_per_chunk:(i + 1) * max_records_per_chunk] for i in range(num_chunks)]
        
        results_responses = []

        for i in smaller_dfs:

            # post request
            with self._create_session() as session:

                if method == "insert":
                    
                    print("inserting..")
                    response = session.post(
                        url = f"{self._base_rest_uri}/data/v1/async/dataextensions/key:{external_key}/rows",
                        headers = headers,
                        data = json.dumps({"items": json.loads(i.to_json(orient="records"))})
                    )

                elif method == "upsert":

                    print("upserting..")
                    response = session.put(
                        url = f"{self._base_rest_uri}/data/v1/async/dataextensions/key:{external_key}/rows",
                        headers = headers,
                        data = json.dumps({"items": json.loads(i.to_json(orient="records"))})
                    )
                
                else:
                    raise ValueError(f"Literally an incorrect method. Like, really?! {method} was never going to work.")

            # status id
            print("ASYNC REQUEST:"); print(response); print(response.content); print(response.json())
            request_id = response.json()['requestId']

            # check status
            with self._create_session() as session:

                request_status = "Pending"
                while request_status == "Pending":

                    response = session.get(
                        url = f"{self._base_rest_uri}/data/v1/async/{request_id}/status",
                        headers = headers
                    )
                    print("STATUS REQUEST:"); print(response.json()); print(response)
                    
                    try:
                        request_status = response.json()['requestStatus']
                    except:
                        break

                # get results
                results_responses.append(session.get(
                        url = f"{self._base_rest_uri}/data/v1/async/{request_id}/results",
                        headers = headers
                    )
                )
                print("RESULTS REQUEST:"); print(response.json()); print(response)

        # return request results
        return results_responses 

        
    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _create_session(self) -> httpx.Client:

        transport = httpx.HTTPTransport(retries = 5)
        timeout = httpx.Timeout(30, write=None)
        client = httpx.Client(
            transport = transport, 
            timeout = timeout
        )

        return client
    
    def _get_bearer_token(self) -> str:

        payload = {
            "grant_type": "client_credentials",
            "client_id": self.client_id.get_secret_value(),
            "client_secret": self.client_secret.get_secret_value(),
            "account_id": self.account_id.get_secret_value()
        }

        with self._create_session() as session:
            response = session.post(
                url = f"{self._base_authentication_uri}/v2/token",
                headers = self._base_headers,
                json = payload
            )
        
        return response.json()['access_token']
    
    def _convert_datetime_columns(
        self, 
        df: pd.DataFrame, 
        format_str: str = "%m/%d/%Y, %I:%M %p"
    ) -> pd.DataFrame:
        
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime(format_str)
        
        return df