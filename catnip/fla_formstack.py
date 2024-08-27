from pydantic import BaseModel, SecretStr
from typing import Dict, List, Any
from collections import Counter

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
import asyncio

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

        - INGESTION
            - FORMS 
                - create table formstack_forms for all forms in relevant folders
                - refresh daily
            - FOLDERS 
                - create table formstack_folders for all forms in relevant folders
                - refresh daily
            - SUBMISSIONS
                - add form id as field to raw data
                - daily etl
                    - get last submission timestamp for every form
                    - query every form for submissions greater than
                    - concat and append to raw formstack_submissions
            - custom submission tables
                - query for raw data by folder/form id
                - transform
                    - create response columns
                    - etc etc
                - load/append to v table
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
        url = f"{self._base_url}/form/{form_id}/submission.json"
        params = {
            "data": "true",
            "expand_data": "true",
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

        df['form_id'] = form_id

        return df 

    def get_forms(self) -> pd.DataFrame:

        ## initialize
        url = f"{self._base_url}/form.json"
        params = {"per_page": 100}

        ## run async requests
        return asyncio.run(
            self._request_loop(
                url = url,
                params = params,
                response_key = "forms"
            )
        )

    def get_folders(self) -> pd.DataFrame:

        ## initialize
        url = f"{self._base_url}/folder.json"
        params = {"per_page": 100}

        ## run async requests
        return asyncio.run(
            self._request_loop(
                url = url,
                params = params,
                response_key = "folders"
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

        # request loop for each batch
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

    def _chunk_list(self, ll: List[Any], chunk_size: int):

        for i in range(0, len(ll), chunk_size):
            yield ll[i:i + chunk_size]

    def _create_async_session(self) -> httpx.AsyncClient:

        transport = httpx.AsyncHTTPTransport(retries = 5)
        client = httpx.AsyncClient(transport = transport, timeout=45)

        return client

    def _create_session(self) -> httpx.Client:

        transport = httpx.HTTPTransport(retries = 5)
        client = httpx.Client(transport = transport, timeout=45)

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
                
        ### Initial Request ##############################################
        with self._create_session() as session:
            response = session.get(
                url = url,
                headers = self._headers,
                params = params
            )

        try:
            if response.json()["total"] == 0:
                return pd.DataFrame()
        except Exception as e:
            print(response.json())
            raise Exception(f"Error: {e}")
        
        # create response list
        responses = [response]

        # check for additional pages
        if "pages" in response.json():

            # get num pages
            num_pages = response.json()['pages']
            print(f"# Pages: {num_pages}")

            # batch future requests
            batches = [min(start + batch_size, num_pages+1) for start in range(2, num_pages+1, batch_size)]
            batches = [2] + batches if num_pages > 1 else batches

            ### Request Rest ##################################################
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
        responses = [item for response in responses for item in response.json()[response_key]]
        print(f"# Reponses: {len(responses)}")
        print(type(responses))

        if self.input_schema:
            df = DataFrame[self.input_schema](responses)
        else:
            df = pd.DataFrame(responses)

        return df 
    
    def _create_response_columns(self, df: pd.DataFrame) -> pd.DataFrame:

        df_expanded = pd.json_normalize(df['data'])
        df_result = pd.concat([df.drop(columns=['data']), df_expanded], axis=1)
        
        return df_result