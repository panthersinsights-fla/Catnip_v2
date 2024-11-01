from pydantic import BaseModel, SecretStr
from typing import Optional, List, Dict

from paramiko import Transport, SFTPClient, SFTPError
import os
from io import StringIO

import pandas as pd
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
        separator: str = ",", 
        encoding: str = "utf-8",
        to_replace: Dict[str, str] = None,
        quotechar: str = None
    ) -> pd.DataFrame:

        ## Create connection
        conn = self._create_connection()

        ## Download csv as dataframe
        if self.file_exists(conn):
            with conn.open(self.remote_path) as file:

                file.prefetch()

                if encoding != "utf-8":
                    file = StringIO(file.read().decode(encoding=encoding))

                if to_replace is not None:
                    if encoding == "utf-8":
                        file = StringIO(file.read().decode(encoding=encoding))
                    content = file.getvalue()
                    for key, value in to_replace.items():
                        modified_content = content.replace(key, value)
                    file.seek(0)
                    file.write(modified_content)
                    file.seek(0)

                try:

                    if self.input_schema:
                        if quotechar is not None:
                            df = DataFrame[self.input_schema](pd.read_csv(file, sep=separator, quotechar=quotechar))
                        else:
                            df = DataFrame[self.input_schema](pd.read_csv(file, sep=separator, engine='python', on_bad_lines='skip'))
                    else:
                        if quotechar is not None:
                            df = pd.read_csv(file, sep=separator, quotechar=quotechar)
                        else:
                            df = pd.read_csv(file, sep=separator)


                except Exception as e:
                    print(f"ERROR: {e}")

        ## Close connection
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

        def sftp_exists(sftp, path):
            try:
                sftp.stat(path)
                return True
            except FileNotFoundError:
                return False
            
        # Recursive function to retrieve file details
        def traverse_directory(path, top_level):
            if not sftp_exists(connection, path):
                print(f"Path does not exist: {path}")
                return
            for item in connection.listdir_attr(path):
                item_path = f"{path}/{item.filename}"
                print(item_path)
                if item.filename == ".cache":
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