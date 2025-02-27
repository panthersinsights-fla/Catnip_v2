import pandas as pd
import polars as pl

from pandera import DataFrameModel
from pandera.polars import DataFrameModel as PolarsDataFrameModel

from pandera.typing import DataFrame
from pandera.typing.polars import DataFrame as PolarsDataFrame

from pydantic import BaseModel, SecretStr
from typing import List, Literal

from datetime import datetime, timezone
import re
from io import StringIO, BytesIO
from uuid import uuid4
from os import getcwd
import traceback
import sys

import psycopg2
from boto3 import resource

from catnip.lookups import REDSHIFT_RESERVED_WORDS


class FLA_Redshift(BaseModel):

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
    
    ######################
    ### USER FUNCTIONS ###
    ######################

    def write_to_warehouse(
            self,
            df: pd.DataFrame | pl.LazyFrame,
            table_name: str,
            file_type: Literal["csv", "parquet"] = "csv",   # New parameter to determine what type of file is written to s3 bucket
            to_append: bool = False,
            column_data_types: List[str] | None = None,
            varchar_max_list: List = [],
            index: bool = False,
            save_local: bool = False,
            delimiter: str = ",",
            quotechar: str = '"',
            dateformat: str = "auto",
            timeformat: str = "auto",
            region: str = "",
            diststyle: str = "even",
            distkey: str = "",
            sort_interleaved: bool = False,
            sortkey: str = "",
            parameters: str = "BLANKSASNULL",
            using_polars: bool = False              # New parameter to determine whether we are using polars or pandas
    ) -> None:
        
        """ 
            Function that takes in a pandas dataframe and writes to redshift table name specified
                - can append to existing table w/ to_append
                - can change column data_types
        """

        ## Create nulls for string fields is using polars
        if using_polars:
            df = df.select([pl.col(col).replace("", None).alias(col) if dtype in [pl.Utf8, pl.String] else pl.col(col).alias(col) for col, dtype in zip(df.collect_schema().names(), df.collect_schema().dtypes())])

        ## Validate & reorder column names
        self._validate_column_names(df, using_polars)
        if self.output_schema:
            if using_polars:
                ## Temporarily convert LazyFrame to DataFrame so that we can validate schema
                df = self.output_schema.validate(df.collect())
                df = df.lazy()
                df = df.select([x for x in [*self.output_schema.to_schema().columns] if x in df.collect_schema().names()])
            else:
                df = self.output_schema.validate(df)
                df = df.reindex(columns = [x for x in [*self.output_schema.to_schema().columns] if x in df.columns.to_list()])
        
        ## Create processed date field
        if using_polars:
            df = df.with_columns(pl.lit(datetime.now(timezone.utc)).cast(pl.Datetime).alias("processed_date"))
        else:
            df['processed_date'] = datetime.now(timezone.utc)

        ## Write to S3
        redshift_table_name = f"custom.{table_name}"
        filename = f"{redshift_table_name}-{uuid4()}.{file_type}"

        if using_polars:
            self._polars_to_s3(
                df = df,
                filename = filename,
                save_local = save_local,
                delimiter = delimiter,
                file_type= file_type
            )
        else:
            self._pandas_to_s3(
                df = df,
                filename = filename,
                index = index,
                save_local = save_local,
                delimiter = delimiter,
                file_type = file_type
            )

        ## Create empty table in Redshift
        if not to_append:
            self._create_redshift_table(
                df = df,
                redshift_table_name = redshift_table_name,
                column_data_types = column_data_types,
                varchar_max_list = varchar_max_list,
                index = index,
                diststyle = diststyle,
                distkey = distkey,
                sort_interleaved = sort_interleaved,
                sortkey = sortkey,
                using_polars = using_polars
            )

        ## S3 to Redshift
        self._s3_to_redshift(
            redshift_table_name = redshift_table_name,
            filename = filename,
            delimiter = delimiter,
            quotechar = quotechar,
            dateformat = dateformat,
            timeformat = timeformat,
            region = region,
            parameters = parameters,
            file_type = file_type
        )

        return None 


    def query_warehouse(self, sql_string: str) -> pd.DataFrame:
        
        with self._connect_to_redshift() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_string)
                columns_list = [desc[0] for desc in cursor.description]

                if self.input_schema:
                    df = DataFrame[self.input_schema](cursor.fetchall(), columns = columns_list)
                else:
                    df = pd.DataFrame(cursor.fetchall(), columns = columns_list)

        ## close up shop
        cursor.close()
        conn.commit()
        conn.close()

        return df 

    
    def execute_and_commit(self, sql_string: str) -> None:
        
        # connect
        conn = self._connect_to_redshift()
        
        # set cursor
        cursor = conn.cursor()
        conn.autocommit = True
        
        # execute and commit
        cursor.execute(sql_string)

        # close up shop
        cursor.close()
        conn.close()

        return None

    def upsert_to_warehouse(
        self,
        target_table: str,
        select_query: str,
        first_fill: bool = False,
        primary_key: str = None,
        base_table: str = None, # if first_fill = True, need value
        sortkey: str = None,
        post_delete_staging_where_clause: str = None
    ) -> None:
        
        def _drop_table(is_staging: bool) -> None:

            q = f"""
                DROP TABLE IF EXISTS custom.{target_table}{"_staging" if is_staging else ""}
            """
            cursor.execute(q)
            conn.commit()

            return None 
        
        def _create_table_from_information_schema(is_staging: bool) -> None:

            # info_schema_column_encodings = {
            #     "bigint": "AZ64",
            #     "boolean": "RAW",
            #     "character varying": "LZO",
            #     "double precision": "RAW",
            #     "integer": "AZ64",
            #     "real": "RAW",
            #     "timestamp without time zone": "AZ64",
            #     "timestamp with time zone": "AZ64"
            # }

            if base_table is None and not is_staging:
                raise SyntaxError("Need a base table in here bruh, if this is a first fill")
            
            # get columns and data types
            q = f"""
                SELECT
                    column_name,
                    CASE
                        WHEN udt_name = 'varchar' THEN upper(udt_name) || '(' || character_maximum_length || ')'
                        ELSE upper(udt_name)
                    END AS "data_type"
                FROM
                    information_schema.columns 
                WHERE 
                    table_name = '{target_table if is_staging else base_table}'
                ORDER BY 
                    ordinal_position;
            """
            cursor.execute(q)
            column_info = cursor.fetchall()
            
            # Step 2: Manually create the staging table with specified encodings
            columns = [x[0] for x in column_info]
            column_data_types = [x[1] for x in column_info]
            encoded_values = self._get_encoded_values(column_data_types)

            q = f"""
                CREATE TABLE custom.{target_table}{"_staging" if is_staging else ""} 
                    ({
                        ', '.join([f'{c} {dt} ENCODE {e}' for c, dt, e in zip(columns, column_data_types, encoded_values)])
                    })
                DISTSTYLE EVEN
                {f" SORTKEY ({sortkey})" if sortkey else ""};
            """
            cursor.execute(q)
            conn.commit()

            return None
        
        def _insert_into_table(is_staging: bool) -> None:

            q = f"""
                INSERT INTO custom.{target_table}{"_staging" if is_staging else ""} (
                    {select_query}
                );
            """
            cursor.execute(q)
            conn.commit()

            return None



        # connect
        conn = self._connect_to_redshift()
        
        # set cursor
        cursor = conn.cursor()
        conn.autocommit = True

        # drop table
        _drop_table(is_staging = not first_fill)

        # create table from base/target
        _create_table_from_information_schema(is_staging = not first_fill)

        # insert into table with select query
        _insert_into_table(is_staging = not first_fill)

        if not first_fill:

            # delete rows from the production table where primary key matches with staging
            q = f"""
                DELETE FROM
                    custom.{target_table}
                USING
                    custom.{target_table}_staging
                WHERE
                    custom.{target_table}.{primary_key} = custom.{target_table}_staging.{primary_key};
            """
            cursor.execute(q)
            conn.commit()
            
            # update staging table (if necessary)
            if post_delete_staging_where_clause:
                q = f"""
                    DELETE FROM 
                        custom.{target_table}_staging
                    {post_delete_staging_where_clause};
                """
                cursor.execute(q)
                conn.commit()

            # append data from staging table back to the production table
            q = f"""
                ALTER TABLE custom.{target_table} APPEND FROM custom.{target_table}_staging;
            """
            cursor.execute(q)
            conn.commit()
            
            # drop the staging table
            q = f"""
                DROP TABLE custom.{target_table}_staging;
            """
            cursor.execute(q)
            conn.commit()

        return None 
    
    ##########################
    ### CONNECTION HELPERS ###
    ##########################

    def _connect_to_redshift(self):

        conn = psycopg2.connect(
            dbname = self.dbname.get_secret_value(),
            host = self.host.get_secret_value(),
            port = self.port,
            user = self.user.get_secret_value(),
            password = self.password.get_secret_value()
        )

        return conn 
    
    def _connect_to_s3(self):

        return resource(
            "s3",
            aws_access_key_id = self.aws_access_key_id.get_secret_value(),
            aws_secret_access_key = self.aws_secret_access_key.get_secret_value()
        )
    

    ##########################
    ### PANDAS TO REDSHIFT ###
    ##########################

    ## validate column names
    def _validate_column_names(self, df: pd.DataFrame | pl.LazyFrame, using_polars: bool = False) -> None:

        if using_polars:
            cols = [str(x).lower() for x in df.collect_schema().names()]
        else:
            cols = [str(x).lower() for x in df.columns]

        ## Check reserved words
        reserved_words = self._get_redshift_reserved_words()

        for col in cols:

            try:
                assert col not in reserved_words
            except AssertionError:
                raise ValueError(f"DataFrame column name {col} is a reserved word in Redshift! 😩")

        ## Check for spaces
        pattern = re.compile(r'\s')

        for col in cols:

            try:
                assert not pattern.search(col)
            except AssertionError:
                raise ValueError(f"DataFrame column name {col} has a space! 😩 Remove spaces from column names and retry!")

        return None

    ## Redshift reserved words
    def _get_redshift_reserved_words(self) -> List[str]:

        reserved_words = [r.strip().lower() for r in REDSHIFT_RESERVED_WORDS]

        return reserved_words
    
    ## pandas to s3
    def _pandas_to_s3(
            self,
            df: pd.DataFrame,
            filename: str,
            index: bool,
            save_local: bool,
            delimiter: str,
            file_type: Literal["csv", "parquet"]
    ) -> None:
        
        if save_local:
            if file_type == "csv":
                df.to_csv(filename, index = index, sep = delimiter)
            elif file_type == "parquet":
                print(filename)
                df.to_parquet(filename, index = index, engine = 'pyarrow', coerce_timestamps = 'us')
            if self.verbose:
                ## add logger!
                print(f"Save file {filename} in {getcwd()} 🙌")

        if file_type == "csv":
            buffer = StringIO()
            df.to_csv(buffer, index = index, sep = delimiter)
        elif file_type == "parquet":
            buffer = BytesIO()
            df.to_parquet(buffer, index = index, engine = 'pyarrow', coerce_timestamps = 'us')

        (self._connect_to_s3()
            .Bucket(self.bucket.get_secret_value())
            .put_object(
                Key = f"{self.subdirectory.get_secret_value()}/{filename}",
                Body = buffer.getvalue()
            )
        )
        
        if self.verbose:
            ## add logger!
            print(f"Saved file {filename} in bucket {self.subdirectory.get_secret_value()} 🙌")

        return None

    def _polars_to_s3(
            self,
            df: pl.LazyFrame,
            filename: str,
            save_local: bool,
            delimiter: str,
            file_type: Literal["csv", "parquet"]
    ) -> None:
        
        if save_local:
            if file_type == "csv":
                df.sink_csv(filename, separator = delimiter)
            elif file_type == "parquet":
                df.sink_parquet(filename)
            else:
                print("Invalid file type")
            if self.verbose:
                ## add logger!
                print(f"Save file {filename} in {getcwd()} 🙌")

        if file_type == "csv":
            buffer = StringIO()
            df.collect().write_csv(buffer, separator = delimiter)
        elif file_type == "parquet":
            buffer = BytesIO()
            df.collect().write_parquet(buffer, use_pyarrow = True)

        (self._connect_to_s3()
            .Bucket(self.bucket.get_secret_value())
            .put_object(
                Key = f"{self.subdirectory.get_secret_value()}/{filename}",
                Body = buffer.getvalue()
            )
        )
        
        if self.verbose:
            ## add logger!
            print(f"Saved file {filename} in bucket {self.subdirectory.get_secret_value()} 🙌")

        return None

    ## s3 to redshift
    def _s3_to_redshift(
            self,
            redshift_table_name: str,
            filename: str,
            delimiter: str,
            quotechar: str,
            dateformat: str,
            timeformat: str,
            region: str,
            parameters: str,
            file_type: Literal["csv", "parquet"]
    ) -> None:
        
        ## Construct query
        bucket_file_name = f"s3://{self.bucket.get_secret_value()}/{self.subdirectory.get_secret_value()}/{filename}"
        authorization_string = f"""
            access_key_id '{self.aws_access_key_id.get_secret_value()}'
            secret_access_key '{self.aws_secret_access_key.get_secret_value()}'
        """
        if file_type == "csv":
            s3_to_sql = f""" 
                COPY {redshift_table_name}
                FROM '{bucket_file_name}'
                DELIMITER '{delimiter}'
                IGNOREHEADER 1
                CSV QUOTE AS '{quotechar}'
                DATEFORMAT '{dateformat}'
                TIMEFORMAT '{timeformat}'
                {authorization_string}
                {parameters}
                {f" REGION '{region}'" if region else ""}
                ;
            """
        elif file_type == "parquet":
            s3_to_sql = f"""
                COPY {redshift_table_name}
                FROM '{bucket_file_name}'
                {f" REGION '{region}'" if region else ""}
                FORMAT AS PARQUET
                {authorization_string}
                ;
            """

        ## Execute & commit
        if self.verbose:
            # add logger!
            def mask_credentials(s: str) -> str:
                s = re.sub('(?<=access_key_id \')(.*)(?=\')', '*'*8, s)
                s = re.sub('(?<=secret_access_key \')(.*)(?=\')', '*'*8, s)
                return s 
            
            masked_credential_string = mask_credentials(s3_to_sql)
            print(f"{masked_credential_string}")
            print("Filling the table into Redshift! 🤞")


        with self._connect_to_redshift() as conn:
            with conn.cursor() as cursor:

                try:
                    cursor.execute(s3_to_sql)
                    conn.commit()
                except Exception as e:
                    print(f"ERROR: {e}")
                    traceback.print_exc(file=sys.stdout)
                    conn.rollback()
                    raise

        ## close up shop
        cursor.close()
        conn.commit()
        conn.close()

        return None 

    ## data types to redshift data types
    def _get_column_data_types(
            self, 
            df: pd.DataFrame | pl.LazyFrame, 
            varchar_max_list: List, 
            index: bool,
            using_polars: bool
    ) -> List[str]:
        
        def _pd_dtype_to_redshift_dtype(dtype: str) -> str:

            if dtype.startswith("int64"):
                return "INT8"
            elif dtype.startswith("int"):
                return "INT"
            elif dtype.startswith("float"):
                return "FLOAT8"
            elif dtype.startswith("datetime"):
                if len(dtype.split(",")) > 1:
                    return "TIMESTAMPTZ"
                else:
                    return "TIMESTAMP"
            elif dtype in ["bool", "boolean"]:
                return "BOOL"
            else:
                return "VARCHAR(256)"
        
        if using_polars:
            column_data_types = [_pd_dtype_to_redshift_dtype(str(dtype).lower()) for dtype in df.collect_schema().dtypes()]
                    
            max_indexes = [idx for idx, val in enumerate(df.collect_schema().names()) if val in varchar_max_list]
            column_data_types = ["VARCHAR(MAX)" if idx in max_indexes else val for idx, val in enumerate(column_data_types)]

        else:
            column_data_types = [_pd_dtype_to_redshift_dtype(str(dtype.name).lower()) for dtype in df.dtypes.values]
        
            max_indexes = [idx for idx, val in enumerate(list(df.columns)) if val in varchar_max_list]
            column_data_types = ["VARCHAR(MAX)" if idx in max_indexes else val for idx, val in enumerate(column_data_types)]
        
        if index:
            column_data_types.insert(0, _pd_dtype_to_redshift_dtype(df.index.dtype.name))

        return column_data_types

    def _get_encoded_values(
            self,
            column_data_types: List[str]
    ) -> List[str]:
        
        encoding_dict = {
            "INT8": "AZ64", 
            "INT": "AZ64", 
            "FLOAT8": "RAW", 
            "TIMESTAMP": "AZ64", 
            "TIMESTAMPTZ": "AZ64", 
            "BOOL": "RAW",
            "VARCHAR": "LZO"
        }
        
        return [encoding_dict[dt.split("(")[0]] for dt in column_data_types]
    
    ## create redshift table
    def _create_redshift_table(
            self,
            df: pd.DataFrame | pl.LazyFrame,
            redshift_table_name: str,
            column_data_types: List[str] | None,
            varchar_max_list: List,
            index: bool,
            diststyle: str,
            distkey: str,
            sort_interleaved: bool,
            sortkey: str,
            using_polars: bool
    ) -> None:

        ## Adjust for potential index
        
        if using_polars:
            columns = df.collect_schema().names()
        else:
            columns = list(df.columns)
        if index:
            if df.index.name:
                columns.insert(0, df.index.name)
            else:
                columns.insert(0, "index")

        ## Get column data types
        if column_data_types is None:
            column_data_types = self._get_column_data_types(df = df, varchar_max_list = varchar_max_list, index = index, using_polars = using_polars)

        ## Get encoded values
        encoded_values = self._get_encoded_values(column_data_types)

        ## Create table query
        create_table_query = f""" 
            
            create table {redshift_table_name}
                ({
                    ", ".join([f"{c} {dt} ENCODE {e}" for c, dt, e in zip(columns, column_data_types, encoded_values)])
                })

        """

        if not distkey:
            if diststyle not in ["even", "all"]:
                raise ValueError("dist_style must be either 'even' or 'all'! C'mon!")
            else:
                create_table_query += f" diststyle {diststyle}"
        else:
            create_table_query += f" distkey({distkey})"
        

        if len(sortkey) > 0:
        
            if sort_interleaved:
                create_table_query += " interleaved"

            create_table_query += f" sortkey({sortkey})"

        ## Execute & commit
        if self.verbose:
            print(create_table_query)
            print("Creating a table in Redshift! 🤞")

        with self._connect_to_redshift() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {redshift_table_name}")
                cursor.execute(create_table_query)
                conn.commit()

        ## close up shop
        cursor.close()
        conn.commit()
        conn.close()

        return None