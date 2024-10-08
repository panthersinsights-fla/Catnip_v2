from pydantic import BaseModel, SecretStr
from typing import List, Dict

import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

class FLA_Email(BaseModel):

    sender_email: str
    sender_email_pw: SecretStr

    ######################
    ### USER FUNCTIONS ###
    ######################

    def send_email(
        self,
        receiver_emails: List[str],
        subject: str,
        body: str,
        cc_list: List[str] = [],
        df_attachments: Dict[str, pd.DataFrame] = None # Dictionary of df's, with key as filename in email send
    ) -> None:

        ## Create email structure
        message = MIMEMultipart()

        # sender
        message['From'] = self.sender_email

        # receiver
        message['To'] = ", ".join(receiver_emails)
        
        # subject
        message['Subject'] = subject

        # cc
        if cc_list:
            message['Cc'] = ", ".join(cc_list)
        
        # all senders
        to_addrs = [*receiver_emails, *cc_list]

        # body
        message.attach(MIMEText(body, "html"))


        # Add attachements, if necessary 
        if df_attachments:

            for key, value in df_attachments.items():

                csv_part = MIMEApplication(self._convert_df_to_csv_string(value))
                csv_part.add_header(
                    "Content-Disposition", 
                    "attachment", 
                    filename = f"{key}.csv"
                )

                message.attach(csv_part)


        ## And finally, send
        with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
            server.starttls()
            server.login(self.sender_email, self.sender_email_pw.get_secret_value())
            server.sendmail(self.sender_email, to_addrs, message.as_string())
            server.quit()

        return None

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _convert_df_to_csv_string(self, df: pd.DataFrame) -> str:

        return df.to_csv(index = False)