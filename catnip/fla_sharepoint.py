from pydantic import BaseModel
from typing import List

from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame
from datetime import datetime

from io import BytesIO
import os
import tempfile


class FLA_Sharepoint(BaseModel):

    ## Windows Login Credentials
    username: str 
    password: str 

    ## Import Pandera Schema
    input_schema: DataFrameModel = None
    output_schema: DataFrameModel = None

    @property
    def _site_url(self) -> str:
        return "https://floridapanthers.sharepoint.com/sites/SP-BS/"
    
    @property
    def _my_credentials(self) -> UserCredential:
        return UserCredential(self.username, self.password)
    
    @property
    def _my_ctx(self) -> ClientContext:
        return ClientContext(self._site_url).with_credentials(self._my_credentials)


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

        ## Update dataframe
        df['processed_date'] = datetime.utcnow()
        if self.output_schema:
            df = df.reindex(columns = [*self.output_schema.__dict__['__annotations__']])

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
            raise SyntaxError("You Failed! 👎 Please select a file type to write to!")

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
        skiprows: int | None = None, # pd.read_csv option
        thousands: str = ",", # pd.read_csv option
        quotechar: str | None = None # pd.read_csv option
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
            raise SyntaxError("You Failed! 👎 Please select a file type!")

        ## Create path strings
        file_url = f"Shared Documents/Data Science/{folder_path}/{file_name}.{file_suffix}"
        download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(file_url))

        ## Download to temp file location
        with open(download_path, "wb") as local_file:
            self._my_ctx.web.get_file_by_server_relative_url(file_url).download(local_file).execute_query()

        ## Read in based on file type
        if is_csv:

            if self.input_schema:
                file = DataFrame[self.input_schema](pd.read_csv(download_path, skiprows = skiprows, quotechar = quotechar, thousands = thousands))
            else:
                file = pd.read_csv(download_path, skiprows = skiprows, quotechar = quotechar, thousands = thousands)

        elif is_xml:

            if self.input_schema:
                file = DataFrame[self.input_schema](pd.read_xml(download_path))
            else:
                file = pd.read_xml(download_path)

        elif is_excel:

            if self.input_schema:
                file = DataFrame[self.input_schema](
                    pd.read_excel(
                        download_path, 
                        sheet_name = sheet_name,
                        skiprows = skiprows
                    )
                )
            else:
                file = pd.read_excel(
                    download_path, 
                    sheet_name = sheet_name,
                    skiprows = skiprows
                )

        elif is_text:
            with open(download_path, 'r') as f:
                file = f.read()

        else:
            raise SyntaxError("I'm Lazy! 🥱 Please select a file type to download file as!")

        return file 