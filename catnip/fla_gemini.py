from pydantic import BaseModel, SecretStr
from typing import Dict, Literal
from catnip.fla_requests import FLA_Requests
import time

class FLA_Gemini(BaseModel):

    api_key: SecretStr
    model: Literal[
        "gemini-1.5-flash", 
        "gemini-2.0-flash-lite", 
        "gemini-2.0-flash", 
        "gemini-2.5-flash",
        "gemini-2.5-flash-lite", 
        "gemini-2.5-pro"
    ]

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

        retries = 0
        max_retries = 5
        delay = 2

        while retries < max_retries:
            try:
                with FLA_Requests().create_session() as session:

                    response = session.post(
                        url = f"{self._base_url}/models/{self.model}:generateContent",
                        params = self._base_params, 
                        headers = self._base_headers, 
                        json = json_data
                )
                
                response.raise_for_status()

                return response.json()['candidates'][0]['content']['parts'][0]['text']
            
            except Exception as e:

                print(f"""
                    Exception: {e}
                    Error: Failed request to {response.url}. 
                    Status code: {response.status_code}. 
                    Reason: {response.text}.
                """)

                print(f"Waiting {delay} seconds...")
                time.sleep(delay)
                delay *= 2
                retries += 1
        
        print(f"Failed to get a successful response after {max_retries} retries.")
        return None