from pydantic import BaseModel

from pandera import DataFrameModel
import pandas as pd

class FLA_Gcs(BaseModel):
    
    '''
        - requires credentials embedded into environment
    '''

    input_schema: DataFrameModel = None 
    output_schema: DataFrameModel = None 

    def df_to_gcs(
        self,
        df: pd.DataFrame,
        file_name: str,
        bucket_path: str,
        as_csv: bool = True,
        with_index: bool = False
    ) -> str:
        
        # create path
        suffix = ".csv" if as_csv else ""
        full_path = f"gs://{bucket_path}/{file_name}{suffix}"

        # validate
        if self.output_schema:
            self.output_schema.validate(df)
        
        # upload
        df.to_csv(full_path, index = with_index)

        # notify
        message = f"Dataframe uploaded to {full_path}"
        return message

