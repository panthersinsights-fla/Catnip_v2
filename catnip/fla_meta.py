from pydantic import BaseModel, SecretStr
from typing import Dict, List, Any

import pandas as pd

from catnip.fla_requests import FLA_Requests
from catnip.fla_prefect import FLA_Prefect

import hmac
import hashlib

import time

class FLA_Meta(BaseModel):

    app_id:         SecretStr
    app_secret:     SecretStr
    access_token:   SecretStr
    ad_account_id:  SecretStr

    version: str = "v20.0"
    batch_size: int = 10000 # meta limit on number of users in single batch

    @property
    def _base_url(self) -> str:
        return "https://graph.facebook.com"
    
    @property
    def _app_secret_proof(self) -> str:
        return hmac.new(self.app_secret.get_secret_value().encode('utf8'), self.access_token.get_secret_value().encode('utf8'), hashlib.sha256).hexdigest()

    @property
    def _base_parameters(self) -> Dict:
        return {
            "access_token": self.access_token.get_secret_value(), 
            "appsecret_proof": self._app_secret_proof,
        }
    
    ##########################
    ### AUDIENCE FUNCTIONS ###
    ##########################

    def create_audience(self, name: str, description: str) -> Dict[str, Any]:

        url = f"{self._base_url}/{self.version}/{self.ad_account_id.get_secret_value()}/customaudiences" 
        
        files = {
            "name": (None, name),
            "subtype": (None, "CUSTOM"),
            "description": (None, description),
            "customer_file_source": (None, "USER_PROVIDED_ONLY"),
            "access_token": (None, self._base_parameters['access_token']),
            "appsecret_proof": (None, self._base_parameters['appsecret_proof'])
        }

        with FLA_Requests().create_session() as session:

            response = session.post(
                url=url,
                files=files
            )
        
        return response.json()


    def get_audience_info(self, audience_id: int) -> Dict[str, Any]:
        
        url = f"{self._base_url}/{self.version}/{audience_id}"

        params = {
            "fields": "operation_status,time_updated,approximate_count_lower_bound,approximate_count_upper_bound",
            **self._base_parameters
        }

        with FLA_Requests().create_session() as session:

            response = session.get(
                url=url,
                params=params
            )
        
        return response.json()
    

    def upload_audience_users(self, audience_id: int, df: pd.DataFrame) -> Dict[str, Any]:
        
        url = f"{self._base_url}/{self.version}"
        num_batches = len(df) // self.batch_size + (1 if len(df) % self.batch_size else 0)

        files = {
            "batch": (
                None,
                str([
                    {
                        "method": "POST",
                        "name": f"batch_{i}",
                        "relative_url": f"{audience_id}/users",
                        "body": (
                            "payload=" + str({
                                "schema": [col.upper() for col in df.columns],
                                "data": self._format_df_for_request(df, i)
                            })
                        )
                    }
                    for i in range(num_batches)
                ])
            ),
            "access_token": (None, self._base_parameters['access_token']),
            "appsecret_proof": (None, self._base_parameters['appsecret_proof'])
        }

        with FLA_Requests().create_session() as session:

            response = session.post(
                url=url,
                files=files
            )

            print(response.status_code)
            print(response.text)
    
        return response.json()


    def delete_audience_users(self, audience_id: int, df: pd.DataFrame) -> List[Dict[str, Any]]:
        
        responses = []

        url = f"{self._base_url}/{self.version}/{audience_id}/users"
        num_batches = len(df) //  self.batch_size + (1 if len(df) %  self.batch_size else 0)

        for i in range(num_batches):

            data = {
                "payload": {
                    "schema": [col.upper() for col in df.columns], 
                    "data": self._format_df_for_request(df, i)
                },
                "access_token": (None, self._base_parameters['access_token']),
                "appsecret_proof": (None, self._base_parameters['appsecret_proof'])
            }

            with FLA_Requests().create_session() as session:

                response = session.delete(
                    url= url,
                    params= data,
                    headers= {"Content-Type": "application/x-www-form-urlencoded"}
                )

                print(response.status_code)
                print(response.text)

                responses.append(response.json())
            
            time.sleep(5)
        
        return responses


    def replace_audience_users(self, audience_id: int, df: pd.DataFrame) -> List[Dict[str, Any]]:

        responses = []

        session_id = self._get_session_id()
        print(f"Session ID: {session_id}")

        num_batches = len(df) // self.batch_size + (1 if len(df) % self.batch_size else 0)
        print(f"Num Batches: {num_batches}")

        url = f"{self._base_url}/{self.version}/{audience_id}/usersreplace"
        
        for i in range(num_batches):
            files = {
                "payload": (
                    None,
                    str({
                        "schema": [col.upper() for col in df.columns],
                        "data": self._format_df_for_request(df, i)
                    })
                ),
                "session": (
                    None,
                    str({
                        "session_id": session_id,
                        "batch_seq": i + 1,
                        "last_batch_flag": "true" if i == num_batches - 1 else "false"
                    })
                ),
                "access_token": (None, self._base_parameters['access_token']),
                "appsecret_proof": (None, self._base_parameters['appsecret_proof'])
            }

            with FLA_Requests().create_session() as session:

                response = session.post(
                    url=url, 
                    files=files
                )

                responses.append(response.json())

            time.sleep(3)
            
        return responses
    

    #######################
    ### TOKEN FUNCTIONS ###
    #######################

    def cache_long_lived_token(self) -> str:

        with FLA_Requests().create_session() as session:

            response = session.get(
                url = f"{self._base_url}/{self.version}/oauth/access_token",
                params = {
                    "grant_type": "fb_exchange_token",
                    "client_id": self.app_id.get_secret_value(),
                    "client_secret": self.app_secret.get_secret_value(),
                    "fb_exchange_token": self.access_token.get_secret_value()
                }
            )

        print(response.json())
        bearer_token = response.json()['access_token']

        ## Override Prefect block
        FLA_Prefect().create_secret_block(name = "meta-access-token-long", value = bearer_token)

        return None
    
    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _get_hash_value(self, value: any) -> str:

        value_bytes = str(value).encode('utf-8')
        hash_object = hashlib.sha256()
        hash_object.update(value_bytes)
        
        return hash_object.hexdigest()
    
    def _get_session_id(self) -> int:
        timestamp = int(time.time())       
        return timestamp
    
    def _format_df_for_request(self, df: pd.DataFrame, batch_num: int) -> List[List[str]]:

        return [[self._get_hash_value(v) for v in row] for row in df.iloc[batch_num * self.batch_size:(batch_num+1) * self.batch_size].values.tolist()]