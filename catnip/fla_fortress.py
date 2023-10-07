from pydantic import BaseModel, SecretStr
from typing import List, Any, Dict

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
import asyncio
from urllib3.util.retry import Retry

from datetime import datetime
import base64


class FLA_Fortress(BaseModel):

    api_key:        SecretStr
    username:       SecretStr
    password:       SecretStr
    
    input_schema:   DataFrameModel = None

    @property
    def _base_url(self) -> str:
        return "https://panthers.fortressus.com/FGB_WebApplication/FGB/Production/api/CRM"

    @property
    def _headers(self) -> Dict:
        credentials = f"{self.username.get_secret_value()}:{self.password.get_secret_value()}"
        return {
            "Authorization": f"Basic {base64.b64encode(credentials.encode()).decode()}",
            "Content-Type": "application/json"
        }

    def _get_base_payload(self) -> Dict:
        return {
            "Header": {
                "Client_AppID": "com.panthers",
                "Client_APIKey": self.api_key.get_secret_value(),
                "Client_AgencyCode": "Panthers",
                "UniqID": 1
            },
            "PageSize": 1000
        }

    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_attendance(self, from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:

        return self._request_loop(
            endpoint = "TimeAttendanceInformation_Paging/", 
            base_payload = {
                **self._get_base_payload(),
                "FromDateTime": from_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                "ToDateTime": to_datetime.strftime("%Y-%m-%dT%H:%M:%S")
            }
        )
    
    def get_events(self, from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:

        return self._request_loop(
            endpoint = "EventInformation_PagingStatistics/", 
            base_payload = {
                **self._get_base_payload(),
                "FromDateTime": from_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                "ToDateTime": to_datetime.strftime("%Y-%m-%dT%H:%M:%S")
            }
        )
    
    def get_members(self, from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:

        return self._request_loop(
            endpoint = "MemberInformation_PagingStatistics/", 
            base_payload = {
                **self._get_base_payload(),
                "FromDateTime": from_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                "ToDateTime": to_datetime.strftime("%Y-%m-%dT%H:%M:%S")
            }
        )
    
    def get_tickets(self, from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:

        return self._request_loop(
            endpoint = "TicketInformation_PagingStatistics/", 
            base_payload = {
                **self._get_base_payload(),
                "FromDateTime": from_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                "ToDateTime": to_datetime.strftime("%Y-%m-%dT%H:%M:%S")
            }
        )

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _create_async_session(self) -> httpx.AsyncClient:

        retry = Retry(total=10, backoff_factor=1, status=10, status_forcelist=[406])
        transport = httpx.AsyncHTTPTransport(retries = retry)
        client = httpx.AsyncClient(transport = transport, timeout=90)

        return client

    def _create_session(self) -> httpx.Client:

        retry = Retry(total = 5, backoff_factor = 0.5)
        transport = httpx.HTTPTransport(retries = retry)
        client = httpx.Client(transport = transport)

        return client
    
    async def _get_async_request(self, url: str, payload: Dict) -> httpx.Response:

        print(f"Running {url}: {payload['PageNumber']}")
        print(self._headers)
        print(payload)
        async with self._create_async_session() as session:
            response = await session.post(
                url = url,
                headers = self._headers,
                json = payload
                # data = payload
            )
    # retries = 0
    # async with httpx.AsyncClient() as client:
    #     while retries < max_retries:
    #         try:
    #             response = await client.get(url)
    #             response.raise_for_status()
    #             return response
    #         except httpx.HTTPError as e:
    #             # Handle specific HTTP errors if needed
    #             print(f"Request failed with status code {e.response.status_code}")
    #             retries += 1
    #             await asyncio.sleep(2 ** retries)  # Exponential backoff (2^n seconds)
    #             continue
    #     else:
    #         # Max retries exceeded, raise an exception or return an error response
    #         raise Exception("Max retries exceeded")

        print(response.status_code); print(len(response.json()['data']))
        return response

    async def _async_gather_pages(self, url: str, base_payload: Dict, start_page: int, end_page: int) -> List[httpx.Response]:

        responses = [self._get_async_request(url=url, payload={**base_payload, "PageNumber": i}) for i in range(start_page, end_page)]

        return await asyncio.gather(*responses)

    async def _request_loop(
        self, 
        endpoint: str,
        base_payload: Dict, 
        batch_size: int = 5
    ) -> pd.DataFrame:

        def _create_dataframe(response: httpx.Response) -> pd.DataFrame:

            try:
                if self.input_schema:
                    return DataFrame[self.input_schema](response.json()['data'])
                else:
                    return pd.DataFrame(response.json()['data'])
            
            except Exception as e:
                print(e)
                print(endpoint)
                print(pd.DataFrame(response.json()['data']))
                
        ### Initial Request ##############################################
        with self._create_session() as session:
            response = session.post(
                url = f"{self._base_url}/{endpoint}",
                headers = self._headers,
                json = {**base_payload, "PageNumber": 1}
            )

        num_pages = response.json()['statistics']['numberOfPages']; print(f"# Pages: {num_pages}")
        responses = [response]

        ### Request Rest ##################################################
        batches = [min(start + batch_size, num_pages+1) for start in range(2, num_pages+1, batch_size)]
        batches = [2] + batches if num_pages > 1 else batches

        for i in range(1, len(batches)):

            print(f"start_page: {batches[i-1]}") 
            print(f"end_page: {batches[i]}")
            
            responses = [
                *responses,
                *await self._async_gather_pages(
                    url = f"{self._base_url}/{endpoint}", 
                    base_payload = base_payload,
                    start_page = batches[i-1], 
                    end_page = batches[i]
                )
            ]

            if i > 5:
                break

        ### Create dataframe ###############################################
        print(len(responses))
        responses = [_create_dataframe(r) for r in responses]
        df = pd.concat(responses, ignore_index = True)

        return df 