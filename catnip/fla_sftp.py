from pydantic import BaseModel, SecretStr
from typing import Optional, List, Dict

from paramiko import Transport, SFTPClient, SFTPError
import os
from io import StringIO

import pandas as pd
import polars as pl
from pandera import DataFrameModel
from pandera.typing import DataFrame

from datetime import datetime

class FLA_Sftp(BaseModel):

    host: str
    username: str
    password: SecretStr
    remote_path: str = None

    port: int = 22

    ## Import Pandera Schema
    input_schema: DataFrameModel = None
    output_schema: DataFrameModel = None

    ######################
    ### USER FUNCTIONS ###
    ######################

    def get_all_filenames_in_directory(self) -> List:

        ## Create connection
        conn = self._create_connection()

        ## Get filenames
        filenames = conn.listdir(self.remote_path)
        
        ## Close connection
        conn.close()

        return filenames


    def file_exists(self, conn: SFTPClient) -> bool:

        ## Check file existence
        try:
            conn.stat(self.remote_path)
            return True

        ## Else return error message
        except SFTPError:
            print(f"The specified file on this '{self.remote_path}' remote_path does not exist.")
            return False


    def download_csv(
        self,
        encoding: str = "utf-8",
        to_replace: Dict[str, str] = None,
        using_polars: bool = False,
        **kwargs
    ) -> pd.DataFrame | pl.LazyFrame:

        # Create a connection to the remote file
        conn = self._create_connection()
        
        try:
            # Check if the file exists
            if self.file_exists(conn):
                with conn.open(self.remote_path) as file:
                    file.prefetch()  # Prefetch file content if supported
                    
                    # Read the content from the file
                    content = file.read()
                    
                    # Decode the content if it is in bytes
                    if isinstance(content, bytes):
                        content = content.decode(encoding)

                    # Perform string replacements if specified
                    if to_replace is not None:
                        for key, value in to_replace.items():
                            print(f"Replacing: {key} with {value}")
                            content = content.replace(key, value)
                    
                    # Wrap the modified content into a StringIO object
                    file = StringIO(content)
                    
                    # Read the CSV content into a DataFrame
                    if using_polars:
                        print(f"Polar Bear Scanning {self.remote_path}")
                        df = pl.scan_csv(file, **kwargs)
                    elif self.input_schema:
                        df = DataFrame[self.input_schema](pd.read_csv(file, **kwargs))
                    else:
                        df = pd.read_csv(file, **kwargs)
        
        except Exception as e:
            print(f"ERROR: {e}")
        
        finally:
            # Ensure the connection is closed in all cases
            conn.close()
        
        return df


    def download_file(self, temp_filename: str = None) -> str:

        ## Create connection
        conn = self._create_connection()

        ## Create local path string
        if temp_filename is None:
            temp_filename = f"{self.remote_path.split('/')[-1]}"
        local_path = f"{os.getcwd()}/{temp_filename}.{self.remote_path.split('.')[-1]}"

        ## Download file and write to local path
        conn.get(self.remote_path, local_path)

        ## Close connection
        conn.close()

        ## Notify path
        print(f"Moved {self.remote_path} to {local_path}")

        ## Return written path string
        return local_path
    

    def upload_csv(self, df: pd.DataFrame) -> None:

        ## Update dataframe
        df['processed_date'] = datetime.utcnow()
        if self.output_schema:
            df = self.output_schema.validate(df)
            df = df.reindex(columns = [*self.output_schema.to_schema().columns])

        ## Create connection
        conn = self._create_connection()

        ## Upload csv
        with conn.open(self.remote_path, "w") as file:
            file.write(df.to_csv(index = False))

        ## Close connection
        conn.close()

        return None


    def remove_file(self):

        ## Create connection
        conn = self._create_connection()

        ## Remove file at remote path
        conn.remove(self.remote_path)
    
        ## Close connection
        conn.close()

        return None

    def get_all_files_info(self) -> pd.DataFrame:

        # Establish SFTP connection
        connection = self._create_connection()
        
        # List to store file information
        files_data = []
            
        # Recursive function to retrieve file details
        def traverse_directory(path, top_level):
            try:
                for item in connection.listdir_attr(path):
                    item_path = f"{path}/{item.filename}"
                    print(item_path)
                    if any(substring in item_path for substring in ["cache", "optimus", "pgp"]):
                        print(f"Skipped {path}")
                        continue
                    # If it's a directory, traverse into it
                    if item.st_mode & 0o40000:  # Check if directory
                        traverse_directory(item_path, top_level)
                    else:
                        # Calculate size in GB
                        size_bytes = item.st_size
                        size_gb = size_bytes / (1024 ** 3)
                        
                        # Extract file name and type
                        file_name = item.filename
                        file_type = os.path.splitext(item.filename)[1].replace('.', '')  # Remove dot

                        # Collect file data
                        files_data.append({
                            'file_path': item_path,
                            'file_name': file_name,
                            'file_type': file_type,
                            'file_size_bytes': size_bytes,
                            'file_size_gb': size_gb,
                            'top_level_directory': top_level
                        })
            except FileNotFoundError as e:
                print(f"Path does not exist: {path}")
                print(e)

        # Start traversal from root (or a specific directory if required)
        root_path = '/'  # Specify directory as needed
        for directory in connection.listdir(root_path):
            # Traverse each top-level directory
            top_level_path = f"{root_path}/{directory}"
            traverse_directory(top_level_path, directory)
        
        # Close the SFTP connection
        connection.close()

        # Convert to DataFrame
        return pd.DataFrame(files_data)

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _create_connection(self) -> Optional[SFTPClient]:

        ## Establish transport object
        transport = Transport((self.host, self.port))
        transport.connect(username = self.username, password = self.password.get_secret_value())

        ## Create SFTP connection object
        connection = SFTPClient.from_transport(transport)        

        return connection