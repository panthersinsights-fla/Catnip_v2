from pydantic import BaseModel, SecretStr

from catnip.fla_requests import FLA_Requests

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

from datetime import datetime
import time 

from typing import Dict
from io import StringIO

class FLA_Stripe(BaseModel):

    api_key: SecretStr

    # pandera schema
    input_schema: DataFrameModel

    @property
    def _base_url(self) -> str:
        return "https://api.stripe.com/v1"
    
    @property
    def _headers(self) -> Dict:
        return {"Authorization": f"Bearer {self.api_key.get_secret_value()}"}
    

    def get_report_run(
        self,
        report_type: str,
        start_date: datetime,
        end_date: datetime = datetime.now()
    ) -> pd.DataFrame:

        # set parameters
        parameters = {
            "report_type": report_type,
            "parameters[columns][]": [*self.input_schema.to_schema().columns],
            "parameters[interval_start]": int(start_date.timestamp()),
            "parameters[interval_end]": int(round(end_date.timestamp()))
        }
        print(parameters)
        
        # post report run
        with FLA_Requests().create_session() as session:
            response = session.post(
                url = f"{self._base_url}/reporting/report_runs",
                headers = self._headers,
                data = parameters
            )

        print(response)

        # pause and set attempt count
        time.sleep(1)
        attempts = 0

        # check status and keep getting until report retrieved
        while response.json()['status'] == "pending":

            with FLA_Requests().create_session() as session:
                response = session.get(
                    url = f"{self._base_url}/reporting/report_runs/{response.json()['id']}",
                    headers = self._headers
                )
            
            print(f"attempt: {attempts}")
            print(response)

            # check retries
            attempts += 1
            if attempts > 3:
                raise RuntimeError("Too many attempts - try again later brohem")

            # pause
            time.sleep(5)


        # retrieve data
        with FLA_Requests().create_session() as session:
            response = session.get(
                url = response['result']['url'],
                headers = self._headers
            )
        
        print("data")
        print(response)

        data = response.content.decode('utf-8')
        df = DataFrame[self.input_schema](pd.read_csv(StringIO(data)))

        return df