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
        secret_block = Secret(value = bearer_token)
        secret_block.save(name = "seatgeek-fla-bearer-token", overwrite = True)

        print("Saved Bearer Token to Secret -> seatgeek-fla-bearer-token 🔒")

        return None 


    def get_sales(self) -> pd.DataFrame:

        ## Initial request
        self._headers['Authorization'] = f"Bearer {self.bearer_token.get_secret_value()}"

        response = self._create_session().get(
            url = f"{self._base_url}/sales",
            headers = self._headers
        ).json()

        response['data'] = [{k[1:] if k.startswith('_') else k.replace('"',''): v for k, v in d.items()} for d in response['data']]
        response['data'] = [{k: v[:19] if k == "transaction_date" else v for k, v in d.items()} for d in response['data']]
        df = DataFrame[self.input_schema](response['data'])

        i = 0
        ## Request rest of data
        while "has_more" in response:
            while response['has_more']:

                try:
                    response = self._create_session().get(
                        url = f"{self._base_url}/sales",
                        headers = self._headers,
                        params = {"cursor": response['cursor']}
                    ).json()

                    response['data'] = [{k[1:] if k.startswith('_') else k.replace('"',''): v for k, v in d.items()} for d in response['data']]
                    response['data'] = [{k: v[:19] if k == "transaction_date" else v for k, v in d.items()} for d in response['data']]
                    df = pd.concat([df, DataFrame[self.input_schema](response['data'])], ignore_index = True)

                except KeyError as e:

                    print(response)
                    print(e); print(e.args)

                except BaseException as e:

                    print(response)
                    print(e); print(e.args)

                if i % 100 == 0:
                    print(i)
                if i > 750000:
                    break
                i += 1

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