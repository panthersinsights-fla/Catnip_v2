from pydantic import BaseModel, SecretStr
from pandera import DataFrameModel
from pandera.typing import DataFrame
import pandas as pd
import pyodbc
from typing import Literal
import traceback

from prefect.blocks.system import Secret
from catnip.fla_teams import FLA_Teams

# set default variable
pyodbc.lowercase = True

class FLA_Odbc(BaseModel):

    host: SecretStr
    username: SecretStr
    password: SecretStr
    driver: str
    database_name: SecretStr = None
    sid: SecretStr = None

    input_schema: DataFrameModel = None

    connection_type: Literal["kore", "retailpro"]

    @property
    def _connection_string(self) -> str:

        if self.connection_type == "kore":

            return repr(f"""
                DRIVER={self.driver};
                SERVER={self.host.get_secret_value()};
                DATABASE={self.database_name.get_secret_value()};
                UID={self.username.get_secret_value()};
                PWD={self.password.get_secret_value()};
                CHARSET=UTF8
            """).strip().replace("\\n", "").replace("  ", "").replace("'", "")
        
        elif self.connection_type == "retailpro":

            return repr(f"""
                DRIVER={self.driver};
                DBQ=//{self.host.get_secret_value()}:1521/{self.sid.get_secret_value()};
                UID={self.username.get_secret_value()};
                PWD={self.password.get_secret_value()}
            """).strip().replace("\\n", "").replace("  ", "").replace("'", "")
    

    # def query_database(self, sql_string: str) -> pd.DataFrame:

    #     connection = pyodbc.connect(self._connection_string, readonly = True)

    #     if self.input_schema:
    #         try:
    #             df = DataFrame[self.input_schema](pd.read_sql(sql_string, connection))
    #         except Exception as e:
    #             # print(list(pd.read_sql(sql_string, connection).columns))
    #             print("Error while validating schema:")
    #             print(f"SQL Query: {sql_string}")
    #             print(f"Error Message: {e}")
    #             print("Traceback:")
    #             traceback.print_exc()
    #             raise
    #     else:
    #         try:
    #             df = pd.read_sql(sql_string, connection)
    #         except Exception as e:
    #             print("Error while validating schema:")
    #             print(f"SQL Query: {sql_string}")
    #             print(f"Error Message: {e}")
    #             print("Traceback:")
    #             traceback.print_exc()
    #             raise

    #     return df

    def query_database(self, sql_string: str) -> pd.DataFrame:
        """
        Execute a SQL query and return the result as a Pandas DataFrame.
        Inspects and logs problematic data in case of encoding issues.
        """
        try:
            # Establish the connection
            connection = pyodbc.connect(self._connection_string, readonly=True)

            # Fetch data using a cursor
            cursor = connection.cursor()
            cursor.execute(sql_string)

            # Inspect raw data for encoding issues
            rows = cursor.fetchall()
            problematic_rows = []
            for i, row in enumerate(rows):
                try:
                    # Attempt to print each row to check for encoding issues
                    print(f"Row {i}: {row}")
                except UnicodeDecodeError as e:
                    print(f"Encoding issue in row {i}: {e}")
                    problematic_rows.append((i, row))
            
            # Log problematic rows if any
            if problematic_rows:
                log_content = "Problematic Rows:\n"
                for index, row in problematic_rows:
                    log_content += f"Row {index}: {row}\n"
                    # send content to teams
                    FLA_Teams(**self.get_teams_credentials()).send_message(log_content)
                
                raise UnicodeDecodeError("Encoding issues detected. See problematic_data.log for details.")

            # Load the full data into a DataFrame
            df = pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description])

            return df

        except UnicodeDecodeError as e:
            print("UnicodeDecodeError encountered:")
            print(f"SQL Query: {sql_string}")
            print(f"Error Message: {e}")
            print("Traceback:")
            traceback.print_exc()
            raise

        except Exception as e:
            print("Unexpected error:")
            print(f"SQL Query: {sql_string}")
            print(f"Error Message: {e}")
            print("Traceback:")
            traceback.print_exc()
            raise

    def get_teams_credentials():

        credentials = {
            "webhook": Secret.load("teams-webhook-url").get()
        }

        return credentials