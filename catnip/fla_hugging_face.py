from pydantic import BaseModel, SecretStr
from typing import Dict, List
from catnip.fla_requests import FLA_Requests

class FLA_Hugging_Face(BaseModel):

    api_token: SecretStr
    model_id: str  # i.e. facebook/bart-large-mnli

    @property
    def _headers(self) -> Dict:
        return {"Authorization": f"Bearer {self.api_token.get_secret_value()}"}
    
    @property
    def _base_url(self) -> str:
        return f"https://api-inference.huggingface.co/models/{self.model_id}"

    
    def query_model(self, payload: Dict) -> Dict | List:

        with FLA_Requests().create_session() as session:

            response = session.post(
                url = self._base_url,
                headers = self._headers,
                params = {"wait_for_model": "true"},
                json = payload
            )

        return response.json()