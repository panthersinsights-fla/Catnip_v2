'''
    - FUNCTIONALITY

        - w/ pandas
            - data_json = data.to_json(orient="records")

        - create data extension
        - retrieve records in data extension
        - update records in data extension
        - delete records in data extension

        - retrieve sent/open/click/bounce/unsub events?
            - would need campaigns/emails
        
        - what is a list?
            - groups of subscribers?
        - what is a subscriber?
            - another name for contact?
'''

'''

    - TO-DO

        - authenticate w/ rest api
        - create test data extensions
        
        - finalize schema
        - write queries and deploy (sfmc_de_)

        - test small payload (post/put)


        - create events descriptions table w/ product id

'''

from pydantic import BaseModel, SecretStr
from typing import List, Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
import asyncio

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

    def insert_into_data_extension(
        self,
        external_key: str,
        df: pd.DataFrame
    ) -> httpx.Response:
        
        # headers
        headers = self._base_headers
        headers['Authorization'] = f"Bearer {self._get_bearer_token()}"

        # post request
        with self._create_session() as session:

            response = session.post(
                url = f"{self._base_rest_uri}/data/v1/async/dataextensions/key:{external_key}/rows",
                headers=headers,
                data = {"items": df.to_json(orient="records")}
            )

        # status id
        request_id = response.json()['requestId']

        # check status
        with self._create_session() as session:

            request_status = "Pending"
            while request_status == "Pending":

                response = session.get(
                    url = f"{self._base_rest_uri}/data/v1/async/{request_id}/status",
                    headers = headers
                )
                request_status = response.json()['requestStatus']

        # return request status response
        return response 

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _create_session(self) -> httpx.Client:

        transport = httpx.HTTPTransport(retries = 5)
        client = httpx.Client(transport = transport, timeout=20)

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
                data = payload
            )
        
        return response.json()['access_token']