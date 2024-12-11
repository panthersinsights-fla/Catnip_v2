from pydantic import BaseModel, SecretStr
from typing import Dict, Literal
from fla_requests import FLA_Requests

class FLA_Gemini(BaseModel):

    api_key: SecretStr
    model: Literal["gemini-1.5-flash"]

    @property
    def _base_url(self) -> str:
        return "https://generativelanguage.googleapis.com/v1beta"

    @property
    def _base_params(self) -> Dict:
        return {
            "key": self.api_key.get_secret_value() 
        }
    
    @property
    def _base_headers(self) -> Dict:
        return {
            "Content-Type": "application/json", 
        }
    
    def generate_text(self, prompt: str) -> str:

        json_data = {
            'contents': [
                {
                    'parts': [
                        {
                            'text': prompt,
                        },
                    ],
                },
            ],
        }

        with FLA_Requests().create_session() as session:

            response = session.post(
                url = f"{self._base_url}/models/{self.model}:generateContent",
                params = self._base_params, 
                headers = self._base_headers, 
                json = json_data
        )

        if response.status_code != 200:
            print(f"""
                Error: Failed request to {response.url}. 
                Status code: {response.status_code}. 
                Reason: {response.text}.
            """)
            return None

        return response.json()['candidates'][0]['content']['parts'][0]['text']