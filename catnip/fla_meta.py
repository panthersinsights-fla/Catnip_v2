from pydantic import BaseModel, SecretStr
from typing import Dict, List, Any
from collections import Counter

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

from prefect.blocks.system import Secret

from catnip.fla_requests import FLA_Requests
from catnip.fla_prefect import FLA_Prefect

import httpx
import asyncio

import hmac
import hashlib

class FLA_Meta(BaseModel):

    app_id: SecretStr
    app_secret: SecretStr
    access_token: SecretStr

    version: str = "20.0"

    @property
    def _base_url(self) -> str:
        return "https://graph.facebook.com"
    
    @property
    def _app_secret_proof(self) -> str:
        return hmac.new(self.app_secret.get_secret_value().encode('utf8'), self.access_token.get_secret_value().encode('utf8'), hashlib.sha256).hexdigest()
    
    
    def cache_long_lived_token(self) -> str:

        with FLA_Requests().create_session() as session:

            response = session.get(
                url = f"{self._base_url}/v{self.version}/oauth/access_token",
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