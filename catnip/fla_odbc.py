from pydantic import BaseModel, SecretStr
from pandera import DataFrameModel
from pandera.typing import DataFrame
import pandas as pd
import pyodbc

# set default variable
pyodbc.lowercase = True

class FLA_Odbc(BaseModel):

    host: SecretStr
    username: SecretStr
    password: SecretStr
    driver: str
    database_name: SecretStr

    input_schema: DataFrameModel = None

    @property
    def _connection_string(self) -> str:

        return repr(f"""
            DRIVER={self.driver};
            SERVER={self.host.get_secret_value()};
            DATABASE={self.database_name.get_secret_value()};
            UID={self.username.get_secret_value()};
            PWD={self.password.get_secret_value()}
        """).strip().replace("\\n", "").replace("  ", "").replace("'", "")
    
    
    def query_database(self, sql_string: str) -> pd.DataFrame:

        connection = pyodbc.connect(self._connection_string, readonly = True)

        if self.input_schema:
            try:
                df = DataFrame[self.input_schema](pd.read_sql(sql_string, connection))
            except Exception as e:
                print(list(pd.read_sql(sql_string, connection).columns))
                print(f"{e}")
        else:
            try:
                df = pd.read_sql(sql_string, connection)
            except Exception as e:
                print(f"{e}")

        return df