from pydantic import BaseModel
import pymsteams

class FLA_Teams(BaseModel):

    webhook: str 

    def __post_init__(self):

        self.my_message = pymsteams.connectorcard(self.webhook)


    def send_message(self, body_text: str) -> None:

        self.my_message.text(body_text)
        self.my_message.send()

        return None