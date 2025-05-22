import pandas as pd

from pandera import DataFrameModel
from pandera.typing import DataFrame

from pydantic import BaseModel, SecretStr
from typing import Dict

from catnip.fla_requests import FLA_Requests

class FLA_Mailchimp(BaseModel):

    api_key: SecretStr
    input_schema: DataFrameModel = None

    @property
    def _data_center(self) -> str:
        return self.api_key.get_secret_value().split("-")[-1] # data center is after dash in api key
    
    @property
    def _base_url(self) -> str:
        return f"https://{self._data_center}.api.mailchimp.com/3.0"
    
    @property
    def _base_headers(self) -> Dict:
        return {"Authorization": f"Bearer {self.api_key.get_secret_value()}"}
    
    ######################
    ### ENDPOINTS ########
    ######################

    def get_lists(self, per_page: int = 1000) -> pd.DataFrame:

        with FLA_Requests().create_session() as session:
            
            response = session.request(
                method = "GET",
                url = f"{self._base_url}/lists?count={per_page}",
                headers = self._base_headers
            )
            
        if self.input_schema:
            df = DataFrame[self.input_schema](pd.json_normalize(response.json(), record_path=['lists'], sep='_'))
        else:
            df = pd.json_normalize(response.json(), record_path=['lists'], sep='_')
        
        return df
    
    def get_list_members(self, list_id: str, per_page: int = 1000) -> pd.DataFrame:

        total_members = self._get_list_member_count(list_id)
        responses = []

        # Pagination occurs through the offset parameter
        with FLA_Requests().create_session() as session:
            for i in range(0, total_members, per_page):
                response = session.request(
                    method = "GET",
                    url = f"{self._base_url}/lists/{list_id}/members?count={per_page}&offset={i}",
                    headers = self._base_headers
                )
                if response.status_code == 200:
                    responses.append(response.json())
                    print(f"List members pulled from page: {int(i/per_page)}")
                else:
                    raise Exception(f"Error: {response.status_code}")
                
        if self.input_schema:
            df = DataFrame[self.input_schema](pd.json_normalize(responses, record_path=['members'], sep='_'))
        else:
            df = pd.json_normalize(responses, record_path=['members'], sep='_')
        
        return df
    
    ######################
    ### HELPERS ##########
    ######################

    def _get_list_member_count(self, list_id: str) -> int:

        with FLA_Requests().create_session() as session:
            response = session.request(
                method = "GET",
                url = f"{self._base_url}/lists/{list_id}/members",
                headers = self._base_headers
            )
        total_members = response.json()['total_items']
        print(F"There are {total_members} members in list_id: {list_id}")

        return total_members
    

