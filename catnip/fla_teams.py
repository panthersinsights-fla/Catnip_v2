from pydantic import BaseModel
from pymsteams import connectorcard

class FLA_Teams(BaseModel):

    webhook: str 

    _my_message: connectorcard

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, **data):

        super().__init__(**data)
        self._my_message = connectorcard(self.webhook)


    def send_message(self, body_text: str) -> None:

        self._my_message.text(body_text)
        self._my_message.send()

        return None