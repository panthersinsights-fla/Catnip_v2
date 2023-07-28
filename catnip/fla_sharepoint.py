from pydantic import BaseModel
from typing import List

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

import pandas as pd
from datetime import datetime

from io import BytesIO
import os
import tempfile


class FLA_Sharepoint(BaseModel):

    ## Windows Login Credentials
    username: str 
    password: str 

    _site_url: str 
    _my_credentials: UserCredential
    _my_ctx: ClientContext

    class Config:
        underscore_attrs_are_private = True

    def __init__(self, **data):

        super().__init__(**data)
        
        self._site_url = "https://floridapanthers.sharepoint.com/sites/SP-BS/"
        
        self._my_credentials = UserCredential(self.username, self.password)
        self._my_ctx = ClientContext(self._site_url).with_credentials(self._my_credentials)

    ######################
    ### USER FUNCTIONS ###
    ######################

    def upload_dataframe(
        self,
        df: pd.DataFrame,
        folder_path: str,
        file_name: str,
        as_csv: bool = True,
        as_xml: bool = False,
        as_excel: bool = False,
        add_log_date: bool = False 
    ) -> str:
        
        ## Convert dataframe
        if as_csv:
            file = BytesIO()
            df.to_csv(file, index = False, encoding = "utf-8")
            file.seek(0)

            file_suffix = "csv"

        elif as_xml:
            file = BytesIO()
            df.to_xml(file)
            file.seek(0)

            file_suffix = "xml"

        elif as_excel:
            file = BytesIO()
            df.to_excel(file)
            file.seek(0)

            file_suffix = "xlsx"

        else:
            print("You Failed! Please select a file type to write to!")

        ## Connect folder
        this_folder = self._my_ctx.web.get_folder_by_server_relative_path(f"Shared Documents/Data Science/{folder_path}")

        ## Filename
        if add_log_date:
            file_name = f"{datetime.strftime(datetime.now(), '%Y%m%d')}-{file_name}.{file_suffix}"
        else:
            file_name = f"{file_name}.{file_suffix}"

        ## Upload file
        target_file = this_folder.upload_file(file_name, file).execute_query()

        ## Return target path
        print(target_file.serverRelativeUrl)
        return target_file.serverRelativeUrl 
    

    def download_file(
        self,
        folder_path: str,
        file_name: str,
        is_csv: bool = False,
        is_xml: bool = False,
        is_excel: bool = False,
        is_text: bool = False,
        sheet_name: str | None = None,
        skiprows: int | None = None
    ) -> pd.DataFrame | str | None:
        
        ## Determine File Type
        if is_csv:
            file_suffix = "csv"
        elif is_xml:
            file_suffix = "xml"
        elif is_excel:
            file_suffix = "xlsx"
        elif is_text:
            file_suffix = "txt"
        else:
            print("You Failed! Please select a file type!")
            return None

        ## Create path strings
        file_url = f"Shared Documents/Data Science/{folder_path}/{file_name}.{file_suffix}"
        download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(file_url))

        ## Download to temp file location
        with open(download_path, "wb") as local_file:
            self._my_ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()

        ## Read in based on file type
        if is_csv:
            file = pd.read_csv(download_path, skiprows = skiprows)

        elif is_xml:
            file = pd.read_xml(download_path)

        elif is_excel:
            file = pd.read_excel(
                download_path, 
                sheet_name = sheet_name,
                skiprows = skiprows
            )

        elif is_text:
            with open(download_path, 'r') as f:
                file = f.read()

        else:
            print("I'm Lazy! Please select a file type to download file as!")

        return file 