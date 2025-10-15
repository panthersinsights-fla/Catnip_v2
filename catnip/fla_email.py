from pydantic import BaseModel, SecretStr
from typing import List, Dict

import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email import encoders

import os

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
        df_attachments: Dict[str, pd.DataFrame] = None, # Dictionary of df's, with key as filename in email send
        file_attachments_filenames: List[str] = None, # List of file paths to attach
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


        # File attachments, if necessary
        if file_attachments_filenames:

            for file_path in file_attachments_filenames:

                with open(file_path, "rb") as attachment:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(attachment.read())
                    
                    # Encode file in ASCII characters to send by email    
                    encoders.encode_base64(part)
                    
                    # Add header as key/value pair to attachment part
                    part.add_header(
                        "Content-Disposition",
                        f"attachment; filename= {os.path.basename(file_path)}",
                    )
                    message.attach(part)

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