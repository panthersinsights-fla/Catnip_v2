from pydantic import BaseModel, SecretStr
from typing import List

import pandas as pd
from io import BytesIO

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
        receiver_email: str | List[str],
        subject: str,
        body: str,
        cc_list: List[str] = [],
        df_attachments: List[pd.DataFrame] = None 
    ) -> None:

        ## Create email structure
        message = MIMEMultipart()

        # sender
        message['From'] = self.sender_email

        # receiver
        if isinstance(receiver_email, List):
            message['To'] = ", ".join(receiver_email)
        else:
            message['To'] = receiver_email
        
        # subject
        message['Subject'] = subject

        # cc
        if cc_list:
            print('adding cc..')
            message['Cc'] = ", ".join(cc_list)
        
        # all senders
        to_addrs = [*receiver_email, *cc_list]; print(f"to_addrs:{to_addrs}")

        # body
        message.attach(MIMEText(body, "html"))


        # ## Add attachements, if necessary 
        # if self.df_attachments:
            
        #     csv_file_objects = self._convert_df_attachments_to_file_objects()

        #     for index, csv_data in enumerate(csv_file_objects):

        #         csv_part = MIMEApplication(csv_data)
        #         csv_part.add_header(
        #             "Content-Disposition", 
        #             "attachment", 
        #             filename = f"data_{index}.csv"
        #         )

        #         message.attach(csv_part)


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

    def _convert_df_attachments_to_file_objects(self) -> List[BytesIO]:

        csv_file_objects = []

        for df in self.df_attachments:

            csv_buffer = BytesIO()

            df.to_csv(csv_buffer)
            csv_buffer.seek(0)

            csv_file_objects.append(csv_buffer)

        return csv_file_objects