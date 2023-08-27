from pydantic import BaseModel, SecretStr
from typing import List, Any

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
import asyncio
from urllib3.util.retry import Retry

from datetime import datetime


class FLA_Big_Commerce(BaseModel):

    store_hash: SecretStr
    api_token: SecretStr
    input_schema: DataFrameModel = None

    @property
    def _base_url(self) -> str:
        return f"https://api.bigcommerce.com/stores/{self.store_hash.get_secret_value()}"
    
    @property
    def _headers(self) -> str:
        return {
            "X-Auth-Token": self.api_token.get_secret_value(),
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_brands(self) -> pd.DataFrame:

        return asyncio.run(self._request_loop_v3(endpoint = "catalog/brands"))

    def get_catalog_products(self) -> pd.DataFrame:
        
        return asyncio.run(self._request_loop_v3(endpoint = "catalog/products"))

    def get_customers(self) -> pd.DataFrame:

        return asyncio.run(self._request_loop_v3(endpoint = "customers"))
    
    def get_customer_groups(self) -> pd.DataFrame:

        return asyncio.run(self._request_loop_v2(endpoint = "customer_groups", batch_size=5))

    def get_product_categories(self) -> pd.DataFrame:

        return asyncio.run(self._request_loop_v3(endpoint= "catalog/categories"))

    def get_order_products(self, order_ids: List[int]) -> pd.DataFrame:

        # create urls
        urls = [f"orders/{x}/products" for x in order_ids]; print(urls)

        # break into batches
        urls = self._chunk_list(urls, 25)

        # request loop v2 for each batch
        df_list = []
        start_time = datetime.now()
        for index, url_list in enumerate(urls):
            print(index)
            print(f"{datetime.now() - start_time}")
            df_list.append(asyncio.run(self._async_gather_urls_v2(url_list=url_list)))
            print(f"{datetime.now() - start_time}")

        # df_list = [asyncio.run(self._async_gather_urls_v2(url_list=url_list)) for url_list in urls]
        df_list = [item for sublist in df_list for item in sublist]

        # concat everything together
        df = pd.concat(df_list, ignore_index = True)

        return df
    
    def get_orders(self) -> pd.DataFrame:

        return asyncio.run(self._request_loop_v2(endpoint = "orders", batch_size = 35))

    def get_transactions(self, order_ids: List[int]) -> pd.DataFrame:

        # create urls
        urls = [f"orders/{x}/transactions" for x in order_ids]; print(urls)

        # break into batches
        urls = self._chunk_list(urls, 25)

        # request loop v3 for each batch
        df_list = [asyncio.run(self._async_gather_urls_v3(url_list=url_list)) for url_list in urls]
        df_list = [item for sublist in df_list for item in sublist]

        # concat everything together
        df = pd.concat(df_list, ignore_index = True)

        return df
    
    def get_variants(self) -> pd.DataFrame:

        return asyncio.run(self._request_loop_v3(endpoint = "catalog/variants"))


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
    
    async def _get_async_request(self, url: str, page: int = None) -> httpx.Response:

        # print(f"Running {url}: {page}")
        async with self._create_async_session() as session:
            response = await session.get(
                url = url,
                headers = self._headers,
                params = {
                    **({"page": page} if page is not None else {}),
                    "limit": 50
                }
            )

        return response

    async def _async_gather_pages(self, url: str, start_page: int, end_page: int) -> List[httpx.Response]:

        responses = [self._get_async_request(url=url, page=i) for i in range(start_page, end_page)]

        return await asyncio.gather(*responses)

    async def _async_gather_urls_v2(self, url_list: List[str]) -> List[pd.DataFrame]:

        responses = [self._request_loop_v2(endpoint=url, batch_size=2) for url in url_list]

        return await asyncio.gather(*responses)
    
    async def _async_gather_urls_v3(self, url_list: List[str]) -> List[pd.DataFrame]:

        responses = [self._request_loop_v3(endpoint=url, batch_size=2) for url in url_list]

        return await asyncio.gather(*responses)
    
    ##########
    ### V3 ###
    ##########

    async def _request_loop_v3(self, endpoint: str, batch_size: int = 25) -> pd.DataFrame:

        def _create_dataframe(response: httpx.Response) -> pd.DataFrame:

            if self.input_schema:
                return DataFrame[self.input_schema](response.json()['data'])
            else:
                return pd.DataFrame(response.json()['data'])
            
        print(endpoint)    
        ### Initial Request ##############################################
        with self._create_session() as session:
            response = session.get(
                url = f"{self._base_url}/v3/{endpoint}",
                headers = self._headers
            )

        df = _create_dataframe(response=response)
        num_pages = response.json()['meta']['pagination']['total_pages']; print(f"# Pages: {num_pages}")
        responses = [response]

        ### Request Rest ##################################################
        batches = [min(start + batch_size, num_pages+1) for start in range(2, num_pages+1, batch_size)]
        batches = [2] + batches if num_pages > 1 else batches; print(f"Batches: {batches}")

        for i in range(1, len(batches)):

            # print(f"start_page: {batches[i-1]}") 
            # print(f"end_page: {batches[i]}")
            
            responses = [
                *responses,
                *await self._async_gather_pages(
                    url = f"{self._base_url}/v3/{endpoint}", 
                    start_page = batches[i-1], 
                    end_page = batches[i]
                )
            ]

        ### Create dataframe ###############################################
        try: 
            responses = [_create_dataframe(r) for r in responses]
        except Exception as e:
            print([r.json() for r in responses])
            raise TypeError(f"WHY NO DATA 😭")
        
        df = pd.concat(responses, ignore_index = True)

        return df 

    ##########
    ### V2 ###
    ##########

    async def _request_loop_v2(self, endpoint: str, batch_size: int = 10) -> pd.DataFrame:

        def _create_dataframe(response: httpx.Response) -> pd.DataFrame:

            if self.input_schema:
                return DataFrame[self.input_schema](response.json())
            else:
                return pd.DataFrame(response.json())

        ### Initial Request ##############################################
        with self._create_session() as session:
            response = session.get(
                url = f"{self._base_url}/v2/{endpoint}",
                headers = self._headers
            )

        df = _create_dataframe(response=response)
        responses = [response]
        
        ### Request Rest ##################################################
        count = 2
        while all(r.status_code == 200 for r in responses):

            responses = [
                *responses,
                *await self._async_gather_pages(
                    url = f"{self._base_url}/v2/{endpoint}", 
                    start_page = count, 
                    end_page = (count + batch_size)
                )
            ]

            if count > 25:
                break

            count += batch_size

        ### Create dataframe ###############################################
        responses = [_create_dataframe(response = r) for r in responses if r.status_code == 200]
        df = pd.concat(responses, ignore_index = True)

        return df