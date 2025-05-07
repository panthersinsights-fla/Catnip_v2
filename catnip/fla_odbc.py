from pydantic import BaseModel, SecretStr
from pandera import DataFrameModel
from pandera.typing import DataFrame
import pandas as pd
import pyodbc
from typing import Literal
import traceback

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

        try:
            # Establish the connection
            connection = pyodbc.connect(self._connection_string, readonly=True)
            connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
            connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
            connection.setencoding(encoding='utf-8')

            # Fetch data using a cursor
            cursor = connection.cursor()
            cursor.execute(sql_string)

            # Inspect raw data for encoding issues
            rows = cursor.fetchall()

            # Load the full data into a DataFrame
            if self.input_schema:
                df = DataFrame[self.input_schema](pd.DataFrame.from_records(rows, columns=[desc[0] for desc in cursor.description]))
            else:
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