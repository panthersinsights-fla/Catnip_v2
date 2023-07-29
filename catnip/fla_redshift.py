import pandas as pd

from pydantic import BaseModel, SecretStr
from typing import List

from datetime import datetime
import re
from io import StringIO
from uuid import uuid4
from os import getcwd
import traceback
import sys

import psycopg2

from boto3 import resource
from boto3.resources.factory import ServiceResource

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
    
    ######################
    ### USER FUNCTIONS ###
    ######################

    def write_to_warehouse(
            self,
            df: pd.DataFrame,
            table_name: str,
            to_append: bool = False,
            column_data_types: List[str] | None = None,
            is_varchar_max: bool = False,
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
            parameters: str = "BLANKSASNULL"
    ) -> None:
        
        """ 
            Function that takes in a pandas dataframe and writes to redshift table name specified
                - can append to existing table w/ to_append
                - can change column data_types
        """

        ## Create processed date field
        df['processed_date'] = datetime.utcnow()

        ## Validate column names
        self._validate_column_names(df)

        ## Write to S3
        redshift_table_name = f"custom.{table_name}"
        csv_name = f"{redshift_table_name}-{uuid4()}.csv"

        self._pandas_to_s3(
            df = df,
            csv_name = csv_name,
            index = index,
            save_local = save_local,
            delimiter = delimiter
        )

        ## Create empty table in Redshift
        if not to_append:
            self._create_redshift_table(
                df = df,
                redshift_table_name = redshift_table_name,
                column_data_types = column_data_types,
                is_varchar_max = is_varchar_max,
                index = index,
                diststyle = diststyle,
                distkey = distkey,
                sort_interleaved = sort_interleaved,
                sortkey = sortkey
            )

        ## S3 to Redshift
        self._s3_to_redshift(
            redshift_table_name = redshift_table_name,
            csv_name = csv_name,
            delimiter = delimiter,
            quotechar = quotechar,
            dateformat = dateformat,
            timeformat = timeformat,
            region = region,
            parameters = parameters
        )

        return None 


    def query_warehouse(self, sql_string: str) -> pd.DataFrame:
        
        with self._connect_to_redshift() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_string)
                columns_list = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(cursor.fetchall(), columns = columns_list)

        ## close up shop
        cursor.close()
        conn.commit()
        conn.close()

        return df 

    
    def execute_and_commit(self, sql_string: str) -> None:
        
        with self._connect_to_redshift() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_string)
                conn.commit()

        ## close up shop
        cursor.close()
        conn.commit()
        conn.close()

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
    def _validate_column_names(self, df: pd.DataFrame) -> None:

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
            csv_name: str,
            index: bool,
            save_local: bool,
            delimiter: str
    ) -> None:
        
        if save_local:
            df.to_csv(csv_name, index = index, sep = delimiter)
            if self.verbose:
                ## add logger!
                print(f"Save file {csv_name} in {getcwd()} 🙌")

        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index = index, sep = delimiter)

        self._connect_to_s3()\
            .Bucket(self.bucket.get_secret_value())\
                .put_object(
                    Key = f"{self.subdirectory.get_secret_value()}/{csv_name}",
                    Body = csv_buffer.getvalue()
                )
        
        if self.verbose:
            ## add logger!
            print(f"Saved file {csv_name} in bucket {self.subdirectory.get_secret_value()} 🙌")

        return None

    ## s3 to redshift
    def _s3_to_redshift(
            self,
            redshift_table_name: str,
            csv_name: str,
            delimiter: str,
            quotechar: str,
            dateformat: str,
            timeformat: str,
            region: str,
            parameters: str
    ) -> None:
        
        ## Construct query
        bucket_file_name = f"s3://{self.bucket.get_secret_value()}/{self.subdirectory.get_secret_value()}/{csv_name}"
        authorization_string = f"""
            access_key_id '{self.aws_access_key_id.get_secret_value()}'
            secret_access_key '{self.aws_secret_access_key.get_secret_value()}'
        """

        s3_to_sql = f""" 
            COPY {redshift_table_name}
            FROM '{bucket_file_name}'
            DELIMITER '{delimiter}'
            IGNOREHEADER 1
            CSV QUOTE AS {quotechar}
            DATEFORMAT '{dateformat}'
            TIMEFORMAT '{timeformat}'
            {authorization_string}
            {parameters}
            {f" REGION '{region}'" if region else ""}
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
            print(f"Filling the table into Redshift! 🤞")


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
            df: pd.DataFrame, 
            is_varchar_max: bool, 
            index: bool
    ) -> List[str]:
        
        def _pd_dtype_to_redshift_dtype(dtype: str) -> str:

            if dtype.startswith("int64"):
                return "BIGINT"
            elif dtype.startswith("int"):
                return "INTEGER"
            elif dtype.startswith("float"):
                return "FLOAT"
            elif dtype.startswith("datetime"):
                return "TIMESTAMP"
            elif dtype == "bool":
                return "BOOLEAN"
            else:
                return "VARCHAR(MAX)"
        
        column_data_types = [_pd_dtype_to_redshift_dtype(str(dtype.name).lower()) for dtype in df.dtypes.values]

        if not is_varchar_max:
            column_data_types = ["VARCHAR(256)" if x == "VARCHAR(MAX)" else x for x in column_data_types]
        
        if index:
            column_data_types.insert(0, _pd_dtype_to_redshift_dtype(df.index.dtype.name))

        return column_data_types

    
    ## create redshift table
    def _create_redshift_table(
            self,
            df: pd.DataFrame,
            redshift_table_name: str,
            column_data_types: List[str] | None,
            is_varchar_max: bool,
            index: bool,
            diststyle: str,
            distkey: str,
            sort_interleaved: bool,
            sortkey: str
    ) -> None:

        ## Adjust for potential index
        columns = list(df.columns)
        if index:
            if df.index.name:
                columns.insert(0, df.index.name)
            else:
                columns.insert(0, "index")

        ## Get column data types
        if column_data_types is None:
            column_data_types = self._get_column_data_types(df = df, is_varchar_max = is_varchar_max, index = index)

        ## Create table query
        create_table_query = f""" 
            
            create table {redshift_table_name}
                ({
                    ", ".join([f"{x} {y}" for x, y in zip(columns, column_data_types)])
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