import httpx
import pandas as pd

from pydantic import BaseModel, SecretStr, root_validator

from pandera import DataFrameModel
from pandera.typing import DataFrame

import time
import json
from typing import Optional

############################################
### DOCUMENTATION ##########################
############################################
"""
https://developer.yellowdogsoftware.com/fetch/getting-started
"""
############################################
### CLASS ##################################
############################################

class FLA_Yellow_Dog(BaseModel):

   username: Optional[str] = None
   password: Optional[SecretStr] = None
   client_id: Optional[str] = None
   access_token: Optional[SecretStr] = None

   input_schema: DataFrameModel = None

   @root_validator
   def validate_auth_method(cls, values):
      """Ensure either access_token OR (username, password, client_id) are provided."""
      access_token = values.get('access_token')
      username = values.get('username')
      password = values.get('password')
      client_id = values.get('client_id')
      
      has_token = access_token is not None
      has_credentials = all([username, password, client_id])
      
      if not has_token and not has_credentials:
         raise ValueError(
            "Must provide either:\n"
            "  - access_token, OR\n"
            "  - username, password, and client_id"
         )
      
      return values

   @property
   def _base_url(self):
      return "https://fetch.yellowdogsoftware.com/api/v3"

   @property
   def _authenticate(self):
      if self.access_token:
         return self.access_token.get_secret_value()
      
      # If no token, fetch one using credentials
      response = httpx.post(
         "https://auth.yellowdogsoftware.com/token",
         json={
            "userName": self.username,
            "password": self.password.get_secret_value(),
            "clientId": self.client_id
         }
      )
      self.access_token = SecretStr(response.json()['result']["accessToken"])
      return self.access_token.get_secret_value()

   @property
   def _base_headers(self):
      return {
         "Authorization": f"Bearer {self._authenticate()}"
      }

   ############################################
   ### ENDPOINT METHODS #######################
   ############################################

   def get_items(self):

      responses = self._paginate_responses("items")
      
      return self._create_dataframe(
         pd.concat([
            pd.json_normalize(
               data = response,
               sep = '_'
               ) for response in responses
            ])
         )

   def get_recipes(self):

      responses = self._paginate_responses("recipes")
      
      return self._create_dataframe(
         pd.concat([
            pd.json_normalize(
               data = response,
               sep = '_'
               ) for response in responses
            ])
         )
   
   def get_recipe_types(self):

      responses = self._paginate_responses("recipetypes")
      
      return self._create_dataframe(
         pd.concat([
            pd.json_normalize(
               data = response,
               sep = '_'
               ) for response in responses
            ])
         )
   
   def get_dimensions(self):

      responses = self._paginate_responses("dimensions")
      
      return self._create_dataframe(
         pd.concat([
            pd.json_normalize(
               data = response,
               sep = '_'
               ) for response in responses
            ])
         )
   
   ############################################
   ### HELPERS ################################
   ############################################

   def _paginate_responses(self, endpoint: str, page_size: int = 500):

      responses = []
      current_page = 1
      next_page = "next_page_link"

      # next page header is empty string once you reach final page
      while next_page != "":
         print(f"Fetching page {current_page}")
         response = httpx.get(
            url = f"{self._base_url}/{endpoint}?pageSize={page_size}&pageNumber={current_page}",
            headers = self._base_headers
         )
         responses.append(response.json())

         current_page += 1
         next_page = json.loads(response.headers['x-pagination'])['nextPageLink']
         print(next_page)
         # rate limit is two requests per second so sleep to slow down process a little
         time.sleep(.5)

      return responses

   def _create_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:

      if self.input_schema:
         return DataFrame[self.input_schema](df)
      else:
         return pd.DataFrame(df)


