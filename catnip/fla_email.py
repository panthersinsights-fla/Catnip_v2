from pydantic import BaseModel
from typing import List

import pandas as pd
from io import BytesIO

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

class FLA_Email(BaseModel):

    sender_email: str
    sender_email_pw: str

    receiver_email: str 

    subject: str
    body: str
    
    cc_list: List[str] | None
    df_attachments: List[pd.DataFrame] | None 


    ######################
    ### USER FUNCTIONS ###
    ######################

    def send_email(self) -> None:

        ## Create email structure
        message = MIMEMultipart()

        message['From'] = self.sender
        message['To'] = self.receiver
        message['Subject'] = self.subject
        message['Cc'] = cc_stringlist

        message.attach(MIMEText(self.body, "html"))


        ## Add cc's
        to_addrs = [self.receiver_email]
        if self.cc_list:
            cc_stringlist = ", ".join(self.cc)
            to_addrs += cc_stringlist


        ## Add attachements, if necessary 
        if self.df_attachments:
            
            csv_file_objects = self._convert_df_attachments_to_file_objects()

            for index, csv_data in enumerate(csv_file_objects):

                csv_part = MIMEApplication(csv_data)
                csv_part.add_header(
                    "Content-Disposition", 
                    "attachment", 
                    filename = f"data_{index}.csv"
                )

                message.attach(csv_part)


        ## And finally, send
        with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
            server.starttls()
            server.login(self.sender_email, self.sender_email_pw)
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