from pydantic import BaseModel, SecretStr
from typing import Dict, List, Any
from collections import Counter

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
import asyncio
from urllib3.util.retry import Retry

class FLA_Formstack(BaseModel):

    api_token: SecretStr
    input_schema: DataFrameModel = None 

    @property
    def _headers(self) -> Dict:
        return {
            "Authorization": f"Bearer {self.api_token.get_secret_value()}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    @property
    def _base_url(self) -> str:
        return "https://www.formstack.com/api/v2/"

    '''
        - remove spaces in column names in response
    '''

    #######################
    ### CLASS FUNCTIONS ###
    #######################    

    def get_form_submissions(
        self,
        form_id: int,
        min_time: str = None
    ) -> pd.DataFrame:

        ## initialize
        url = f"{self._base_url}/forms/{form_id}/submission.json"
        params = {
            "data": True,
            "expand_data": True,
            **({"min_time": min_time} if min_time is not None else {}),
            "per_page": 100
        }

        ## run async requests
        df = asyncio.run(
            self._request_loop(
                url = url,
                params = params,
                response_key = "submissions"
            )
        )

        ## create response columns
        if len(df.index) != 0:
            df = self._create_response_columns(df)

        return df 


    def get_forms_in_folder(self, folder_id: int) -> pd.DataFrame:

        ## initialize
        url = f"{self._base_url}/forms.json"
        params = {
            "folder": folder_id,
            "per_page": 100
        }

        ## run async requests
        return asyncio.run(
            self._request_loop(
                url = url,
                params = params,
                response_key = "forms"
            )
        )
    
    def get_all_submissions_in_folder(
        self,
        folder_id: int,
        skip_these_form_ids: List[int]
    ) -> pd.DataFrame:
        
        ## get all form ids
        form_ids = [*Counter([x for x in self.get_forms_in_folder(folder_id=folder_id)['id']])] # counter gets unique values in a cute way
        form_ids = [item for item in form_ids if item not in skip_these_form_ids] # skip these form id's

        ## get every page of form submissions per id
        urls = [f"{self._base_url}/forms/{x}/submission.json" for x in form_ids]
        urls = self._chunk_list(urls)

        # request loop v3 for each batch
        df_list = [asyncio.run(self._async_gather_urls(url_list=url_list)) for url_list in urls]
        df_list = [item for sublist in df_list for item in sublist]

        # concat everything together
        df = pd.concat(df_list, ignore_index = True)

        ## create response columns
        if len(df.index) != 0:
            df = self._create_response_columns(df)

        return df 

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _chunk_list(self, l: List[Any], chunk_size: int) -> List[List[Any]]:

        for i in range(0, len(l), chunk_size):
            yield l[i:i + chunk_size]

    def _create_async_session(self) -> httpx.AsyncClient:

        retry = Retry(total = 5, backoff_factor = 0.5)
        transport = httpx.AsyncHTTPTransport(retries = retry)
        client = httpx.AsyncClient(transport = transport)

        return client

    def _create_session(self) -> httpx.Client:

        retry = Retry(total = 5, backoff_factor = 0.5)
        transport = httpx.HTTPTransport(retries = retry)
        client = httpx.Client(transport = transport)

        return client
    
    async def _get_async_request(
        self, 
        url: str, 
        params: Dict,
        page: int = None
    ) -> httpx.Response:

        print(f"Running {url}: {page}")
        async with self._create_async_session() as session:
            response = await session.get(
                url = url,
                headers = self._headers,
                params = {
                    **({"page": page} if page is not None else {}),
                    **params
                }
            )

        return response

    async def _async_gather_pages(
        self, 
        url: str,
        params: Dict, 
        start_page: int, 
        end_page: int
    ) -> List[httpx.Response]:

        responses = [self._get_async_request(url=url, params=params, page=i) for i in range(start_page, end_page)]

        return await asyncio.gather(*responses)

    async def _async_gather_urls(self, url_list: List[str], params: Dict) -> List[pd.DataFrame]:
        
        responses = [self._request_loop(url, params, response_key="submissions") for url in url_list]

        return await asyncio.gather(*responses)
    
    async def _request_loop(
        self, 
        url: str, 
        params: Dict, 
        response_key: str, 
        batch_size: int = 25
    ) -> pd.DataFrame:

        def _create_dataframe(response: httpx.Response) -> pd.DataFrame:

            try:
                data = response.json()[response_key]
                data = [{key.replace(' ', '_').strip().lower(): value for key, value in d.items()} for d in data]

                if self.input_schema:
                    return DataFrame[self.input_schema](data)
                else:
                    return pd.DataFrame(data)
            
            except Exception as e:
                print(e)
                print(url)
                # print(pd.DataFrame(data))
                
        ### Initial Request ##############################################
        with self._create_session() as session:
            response = session.get(
                url = url,
                headers = self._headers,
                params = params
            )

        df = _create_dataframe(response=response)
        num_pages = response.json()['pages']; print(f"# Pages: {num_pages}")
        responses = [response]

        ### Request Rest ##################################################
        batches = [min(start + batch_size, num_pages+1) for start in range(2, num_pages+1, batch_size)]
        batches = [2] + batches if num_pages > 1 else batches

        for i in range(1, len(batches)):
            
            responses = [
                *responses,
                *await self._async_gather_pages(
                    url = url, 
                    params = params,
                    start_page = batches[i-1], 
                    end_page = batches[i]
                )
            ]

        ### Create dataframe ###############################################
        responses = [_create_dataframe(r) for r in responses]
        df = pd.concat(responses, ignore_index = True)
        df['request_url'] = url

        return df 
    
    def _create_response_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        df_expanded = pd.json_normalize(df['data'])
        df_result = pd.concat([df.drop(columns=['data']), df_expanded], axis=1)
        
        return df_result