from pydantic import BaseModel
from pymsteams import connectorcard

class FLA_Teams(BaseModel):

    webhook: str 

    @property
    def _my_message(self) -> connectorcard:
        return connectorcard(self.webhook)
    
    
    def send_message(self, body_text: str) -> None:

        self._my_message.text(body_text)
        self._my_message.send()

        return None