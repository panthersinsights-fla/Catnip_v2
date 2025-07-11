from pydantic import BaseModel, SecretStr
from typing import Dict, List

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

from prefect.blocks.system import Secret

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta

from prefect.blocks.notifications import MicrosoftTeamsWebhook

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

        print(response.json())

        bearer_token = response.json()['access_token']

        ## Override Prefect block
        self._create_secret_block(name = "seatgeek-fla-bearer-token", value = bearer_token)

        return None 

    def get_attendance(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "attendance", _cursor = cursor)

    def get_attribution(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "attribution", _cursor = cursor)
    
    def get_clients(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "clients", _cursor = cursor)

    def get_installments(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "installments", _cursor = cursor)

    def get_manifests(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "manifests", _cursor = cursor)

    def get_payments(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "payments", _cursor = cursor)

    def get_pricing(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "pricing", _cursor = cursor)

    def get_products(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "products", _cursor = cursor)

    def get_sales(self, cursor: str | None) -> pd.DataFrame | None:

        return self._request_loop(endpoint = "sales", _cursor = cursor)
    

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
    
    def _check_reponse(self, r: requests.Response) -> None:
        
        if r.status_code != 200:
            print(r.text)
            raise ConnectionError(f"""
                Uh-Oh! 😩 
                Status Code: {r.status_code}
                Response: {r.json()['message']}
            """)

        return None 
    
    def _request_loop(
            self,
            endpoint: str, # attendance, clients, installments, manifests, payments, products, sales 
            _cursor: str | None
        ) -> pd.DataFrame | None:

        ### Initial Request ###########################################################
        start_time = datetime.now()
        self._headers['Authorization'] = f"Bearer {self.bearer_token.get_secret_value()}"

        _params = {"limit": 1000}
        if _cursor is not None:
            _params["cursor"] = _cursor

        with self._create_session() as session:
            response = session.get(
                url = f"{self._base_url}/{endpoint}",
                headers = self._headers,
                params = _params
            )

        # Check Response
        self._check_reponse(response)
        print(f"Intial Request: {response}")

        # Pass Check -> update variables
        responses = response.json()['data']
        if len(responses) == 0:
            print("ENPTY 😩")
            print(responses)
            print(response.json())
            return None

        _has_more = response.json()['has_more']
        _params["cursor"] = response.json()['cursor']

        ### Request rest of data #####################################################
        i = 0
        while _has_more:

            try:
                # Try Additional Request
                with self._create_session() as session:
                    temp_response = session.get(
                        url = f"{self._base_url}/{endpoint}",
                        headers = self._headers,
                        params = _params
                    )

                # Check Response
                self._check_reponse(temp_response)

                # Pass Check -> update variables
                response = temp_response
                responses = [*responses, *response.json()['data']]

                _has_more = response.json()['has_more']
                _params["cursor"]  = response.json()['cursor']
                
            except BaseException as e:

                body = f"""
                    Request:
                        url = {self._base_url}/{endpoint}
                        headers = {self._headers}
                        params = {_params}

                    Response: {temp_response}
                    Status Code: {temp_response.status_code}

                    Error: {e}
                """
                print(body)
                teams_webhook_block = MicrosoftTeamsWebhook.load("teams-notification-block")
                teams_webhook_block.notify(body)
                
                break

            if i % 5 == 0:
                print(i)

            if (datetime.now() - start_time) > timedelta(minutes=5):
                break

            i += 1

        ### Create dataframe ###########################################################
        df = self._clean_response(endpoint, responses)

        ### Update Cursor in Block #####################################################
        self._create_secret_block(name = f"seatgeek-fla-last-cursor-{endpoint}", value = response.json()['cursor'])

        return df 
    
    def _clean_response(
            self, 
            endpoint: str, # attendance, clients, installments, manifests, payments, products, sales 
            response: List[Dict]
        ) -> pd.DataFrame | None:

        response = [{k[1:] if k.startswith('_') else k.replace('"',''): v for k, v in d.items()} for d in response]

        if endpoint == "attendance":
            return DataFrame[self.input_schema](response)

        elif endpoint == "attribution":
            return DataFrame[self.input_schema](response)
        
        elif endpoint == "clients":
            response = [{k: v[:19] if k in ["creation_datetime", "account_linked_at"] else v for k, v in d.items()} for d in response]
            return DataFrame[self.input_schema](response)
        
        elif endpoint == "installments":
            response = [{k: v[:19] if k == "execution_date" and v is not None else v for k, v in d.items()} for d in response]
            return DataFrame[self.input_schema](response)
        
        elif endpoint == "manifests":
            response = [{k: v[:19] if k == "creation_datetime" else v for k, v in d.items()} for d in response]
            return DataFrame[self.input_schema](response)

        elif endpoint == "payments":
            response = [{k: v[:19] if k in ["event_datetime_utc", "datetime_utc"] and v is not None else v for k, v in d.items()} for d in response]
            response = [{k: v.replace("$","").replace(",", "") if k in ["debit_amt", "credit_amt", "credit_applied_amnt", "debit_commissions_amount"] and v is not None else v for k, v in d.items()} for d in response]
            return DataFrame[self.input_schema](response)

        elif endpoint == "pricing":
            return DataFrame[self.input_schema](response)

        elif endpoint == "products":
            response = [{k: v[:19] if k == "creation_date" and v is not None else v for k, v in d.items()} for d in response]
            return DataFrame[self.input_schema](response)

        elif endpoint == "sales":
            response = [{k: v[:19] if k == "transaction_date" else v for k, v in d.items()} for d in response]
            response = [{k: v.replace("$","").replace(",", "") if k in ["list_price", "total_price"] and v is not None else v for k, v in d.items()} for d in response]

            ## 20240214 - fix
            response = [{k: v for k, v in d.items() if k != "product_item_id"} for d in response]

            return DataFrame[self.input_schema](response)

        else:
            raise ValueError("Bruh.. Put an endpoint in here 😑")