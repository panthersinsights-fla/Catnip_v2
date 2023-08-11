from pydantic import BaseModel, SecretStr
from typing import Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

from prefect.blocks.system import Secret

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class FLA_SeatGeek(BaseModel):

    client_id: SecretStr
    client_secret: SecretStr
    bearer_token: SecretStr | None

    _headers: Dict = {"Accept": "application/json"}

    ## Import Pandera Schema
    input_schema: DataFrameModel = None

    class Config:
        underscore_attrs_are_private = True

    @property
    def _base_url(self) -> str:
        return "https://ringside.seatgeek.com/v1"

    @property
    def _auth_url(self) -> str:
        return "https://auth.seatgeek.com/oauth/token"
    

    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def cache_authentication_token(self) -> None:

        ## Get Bearer token
        self._headers['Content-Type'] = "application/json"
        payload = {
            "client_id": self.client_id.get_secret_value(),
            "client_secret": self.client_secret.get_secret_value(),
            "audience": '/'.join(self._base_url.split('/')[:-1]),
            "grant_type": "client_credentials"
        }
    
        response = self._create_session().post(
            url = self._auth_url,
            headers = self._headers,
            json = payload
        )

        bearer_token = response.json()['access_token']

        ## Override Prefect block
        self._create_secret_block(name = "seatgeek-fla-bearer-token", value = bearer_token)

        return None 


    def get_sales(self, _cursor: str | None) -> pd.DataFrame | None:

        def check_reponse(r: requests.Response) -> None:
            
            if r.status_code != 200:
                raise ConnectionError(f"""
                    Uh-Oh! 😩 
                    Status Code: {r.status_code}
                    Response: {response.json()['message']}
                """)

            return None 

        def clean_response(r: requests.Response) -> pd.DataFrame | None:
            
            response = r.json()        

            response['data'] = [{k[1:] if k.startswith('_') else k.replace('"',''): v for k, v in d.items()} for d in response['data']]
            response['data'] = [{k: v[:19] if k == "transaction_date" else v for k, v in d.items()} for d in response['data']]

            return DataFrame[self.input_schema](response['data'])


        ## Initial request - Headers
        self._headers['Authorization'] = f"Bearer {self.bearer_token.get_secret_value()}"

        ## Initial request - Paramaters
        _params = {"limit": 1000}
        if _cursor is not None:
            _params["cursor"] = _cursor

        ## Try Initial request
        response = self._create_session().get(
            url = f"{self._base_url}/sales",
            headers = self._headers,
            params = _params
        )

        ## Check Response
        check_reponse(response)

        ## Pass Check -> update variables
        df = clean_response(response)
        _has_more = response.json()['has_more']
        _params["cursor"] = response.json()['cursor']; print(_params["cursor"])

        i = 0
        ## Request rest of data
        while _has_more:

            try:

                ## Try Additional Request
                temp_response = self._create_session().get(
                    url = f"{self._base_url}/sales",
                    headers = self._headers,
                    params = _params
                )

                ## Check Reponse
                check_reponse(temp_response)

                ## Pass Check -> update variables
                response = temp_response
                temp_df = clean_response(response)
                df = pd.concat([df, temp_df], ignore_index = True)
                _has_more = response.json()['has_more']
                _params["cursor"]  = response.json()['cursor']; print(_params["cursor"])
                

            except KeyError as e:

                print(f"Response: {temp_response} -- Status Code: {temp_response.status_code}")
                print(f"KeyError: {e}"); print(f"KeyError Args: {e.args}")
                break

            except BaseException as e:

                print(f"Response: {temp_response} -- Status Code: {temp_response.status_code}")
                print(f"Error: {e}"); print(f"Error Args: {e.args}")
                break

            if i % 10 == 0:
                print(i)

            if i > 100:
                break

            i += 1

        ## Update Cursor in Block
        self._create_secret_block(name = "seatgeek-fla-last-cursor-sales", value = response.json()['cursor'])

        return df 
    
    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _create_session(self) -> requests.Session:

        session = requests.Session()
        retry = Retry(total = 5, backoff_factor = 0.2)
        adapter = HTTPAdapter(max_retries = retry)
        session.mount('https://', adapter)

        return session
    
    def _create_secret_block(self, name: str, value: str) -> None:

        secret_block = Secret(value = value)
        secret_block.save(name = name, overwrite = True)

        print(f"Saved to Secret -> {name} 🔒")

        return None 