from pydantic import BaseModel, SecretStr

from pandera import DataFrameModel
from pandera.typing import DataFrame
import pandas as pd

from catnip.fla_requests import FLA_Requests
import asyncio
import httpx

from datetime import datetime, timedelta
from typing import Dict, List
import time

class FLA_Bump(BaseModel):

    access_token: SecretStr
    
    # Pandera
    input_schema: DataFrameModel = None

    @property
    def _base_url(self) -> str:
        return "https://core-xt-api.bump5050.net"
    
    @property
    def _base_headers(self) -> Dict:
        return {"Authorization" : f"Bearer {self.access_token.get_secret_value()}"}
    
    ##############################################
    ### CLASS FUNCTIONS ##########################
    ##############################################

    def get_event_details(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        per_page: int = 5000, 
        chunk_size: int = 5
    ) -> pd.DataFrame:

        ## base request parameters
        url = f"{self._base_url}/reports/events/details"
        params = {
            "minDate": self._convert_dt(start_date), 
            "maxDate": self._convert_dt(end_date), 
            "count": per_page
        }

        ## get total pages
        total_pages = self._get_total_pages(url, params)

        ## get chunked responses
        results = self._get_responses_with_chunking(
            url=url,
            params=params,
            total_pages=total_pages,
            chunk_size=chunk_size
        )

        ## return dataframe
        return self._create_dataframe(
            pd.json_normalize(
                results,
                record_path= ["items"],
                sep= "_"
            )
        )

    def get_locations(self, per_page: int = 5000) -> pd.DataFrame:

        url = f"{self._base_url}/reports/locations"
        params = {"count": per_page}

        total_pages = self._get_total_pages(url, params)
        params_list = [{"count": per_page, "page": page} for page in range(1,total_pages+1)]
        results = asyncio.run(self._get_results(url, params_list))

        return self._create_dataframe(
            pd.json_normalize(
                results,
                record_path= ["items"],
                sep= "_"
            )
        )

    def get_nonprofits(self, per_page: int = 5000) -> pd.DataFrame:

        url = f"{self._base_url}/reports/nonprofits"
        params = {"count": per_page}

        total_pages = self._get_total_pages(url, params)
        params_list = [{"count": per_page, "page": page} for page in range(1,total_pages+1)]
        results = asyncio.run(self._get_results(url, params_list))

        return self._create_dataframe(
            pd.json_normalize(
                results,
                record_path= ["items"],
                sep= "_"
            )
        )

    def get_customers(self, per_page: int = 5000, chunk_size: int = 5) -> pd.DataFrame:

        ## base request parameters
        url = f"{self._base_url}/reports/customers"
        params = {"count": per_page}

        ## get total pages
        total_pages = self._get_total_pages(url, params)

        ## get chunked responses
        results = self._get_responses_with_chunking(
            url=url,
            params=params,
            total_pages=total_pages,
            chunk_size=chunk_size
        )
        
        ## return dataframe
        return self._create_dataframe(
            pd.json_normalize(
                results,
                record_path= ["items"],
                sep= "_"
            )
        )

    def get_sales(
        self, 
        start_date: datetime, 
        end_date: datetime, 
        per_page: int = 5000, 
        chunk_size: int = 5
    ) -> pd.DataFrame:
        
        ## base request parameters
        url = f"{self._base_url}/reports/sales"
        params = {
            "minDate": self._convert_timestamp_sales(start_date, True), 
            "maxDate": self._convert_timestamp_sales(end_date, False), 
            "count": per_page
        }
        
        ## get total pages
        total_pages = self._get_total_pages(url, params)

        ## get chunked responses
        results = self._get_responses_with_chunking(
            url=url,
            params=params,
            total_pages=total_pages,
            chunk_size=chunk_size
        )

        ## return dataframe
        return self._create_dataframe(
            pd.json_normalize(
                results,
                record_path= ["items"],
                sep= "_"
            )
        )
    
    ##############################################
    ### ASYNC FUNCTIONS ##########################
    ##############################################

    async def _get_async_request(
        self, 
        url: str, 
        params: Dict
    ) -> httpx.Response:

        max_retries = 3
        delay = 60
        last_exception = None

        # error handling, allows for three retries, with 60 second sleep between failures
        for attempt in range(1, max_retries+1):
            print(f"""
                    Running {url}: 
                    Params: {params}
                    Attempt: {attempt} of {max_retries}
                """)
            try:
            
                async with FLA_Requests().create_async_session() as session:
                    response = await session.get(
                        url = url,
                        headers = self._base_headers,
                        params = params
                    )   
                response.raise_for_status()  # Raise an error for bad responses
                return response.json()
            
            except Exception as e:
                print(f"""
                        Attempt {attempt} at 
                        URL: {url}
                        Params: {params}
                        Status Code: {response.status_code}
                        Failed with error: {e}
                        Response Text: {response.text}
                    """)
                
                print(f"Retrying in {delay} seconds...")
                last_exception = e
                
                await asyncio.sleep(delay)

        # If all retries failed, raise the last encountered exception
        if last_exception:
            raise last_exception
        else:
            # This case should ideally not be reached if max_retries > 0,
            # as either a successful response is returned or an exception is raised.
            # But as a fallback:
            raise Exception(f"Failed to fetch {url} after {max_retries} attempts. Unknown error state.")
            
    async def _gather_responses(self, url: str, params_list: List[Dict]) -> List[Dict]:
        tasks = [self._get_async_request(url, params) for params in params_list]
        tasks = [task for task in tasks if task is not None]
        return await asyncio.gather(*tasks)

    async def _get_results(self, url: str, params_list: List[Dict] = [{}]) -> List[Dict]:
        return await self._gather_responses(url, params_list)
    
    ##############################################
    ### HELPER FUNCTIONS #########################
    ##############################################

    def _convert_dt(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    def _convert_timestamp_sales(self, dt: datetime, is_start: pd.BooleanDtype):
        '''
        - is_start param exists so that timestamp range can go from beginning of start date to the very end of end date
        - only used for sales endpoint
        '''

        if is_start:
            target_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            target_dt = dt + timedelta(days=1, seconds=-1)
        
        formatted_date = target_dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        return formatted_date
    
    def _create_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:

        if self.input_schema:
            return DataFrame[self.input_schema](df)
        else:
            return pd.DataFrame(df)
        
    def _get_total_pages(self, url: str, params: Dict) -> httpx.Response:
        '''
        - total pages only availble in response from page 1 request
        - if not key 'totalPages' then response is only one page
        '''
    
        params['page'] = 1

        with FLA_Requests().create_session() as session:
            response = session.get(
                url = url,
                headers = self._base_headers,
                params = params
            )
        
        result = response.json()
    
        if 'totalPages' in result.keys():
            return result['totalPages']
        else:
            return 1
        
    def _get_responses_with_chunking(
        self,
        url: str,
        params: Dict,
        total_pages: int,
        chunk_size: int
    ) -> List[Dict]:

        ## chunking process
        if total_pages > chunk_size:
                results = []
                print(f"There are {total_pages} pages")
                temp_chunk_size = chunk_size
                for i in range(0, total_pages, chunk_size):
                    print(f"Grabbing pages {i+1}-{temp_chunk_size} of {total_pages}")
                    params_list = [{**params, "page": page} for page in range(1+i,temp_chunk_size+1)]
                    results_temp = asyncio.run(self._get_results(url, params_list))
                    results.extend(results_temp)
                    temp_chunk_size = temp_chunk_size + chunk_size
                    if total_pages <= temp_chunk_size:
                        temp_chunk_size = total_pages
                    time.sleep(45)
        else:
            params_list = [{**params, "page": page} for page in range(1,total_pages+1)]
            results = asyncio.run(self._get_results(url, params_list))

        return results