from pydantic import BaseModel, SecretStr
from typing import List, Literal

from pandera import DataFrameModel
from pandera.polars import DataFrameModel as PolarsDataFrameModel

from pandera.typing import DataFrame
from pandera.typing.polars import DataFrame as PolarsDataFrame


class FLA_Redshift_v2(BaseModel):

    ## Database Info
    dbname: SecretStr
    host: SecretStr
    port: int
    user: SecretStr
    password: SecretStr

    ## S3 Bucket Info
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    bucket: SecretStr
    subdirectory: SecretStr

    ## Options
    verbose: bool = True

    ## Import Pandera Schema
    input_schema: DataFrameModel | PolarsDataFrameModel = None
    output_schema: DataFrameModel | PolarsDataFrameModel = None