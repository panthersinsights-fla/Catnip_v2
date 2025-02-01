from pydantic import BaseModel, SecretStr, Field

from pandera import DataFrameModel
from pandera.typing import DataFrame
import pandas as pd

from catnip.fla_requests import FLA_Requests
import asyncio
import httpx

from datetime import datetime
from typing import Dict, List, Literal, Any
import time

'''
    RATE LIMITS:
    The API is limited per-resource and per-user. 
    Limits are generous for most endpoints and should not impede use cases.

    For all users the API is by default limited to 15 requests per resource in a 15 minutes window.

    For instance, a user has run during the last minute 3 requests to /teams?sport=:sportId and 12 requests to /teams/:teamId. 
    Then the teams resource is unavailable until the next request window. 
'''
class FLA_Blinkfire(BaseModel):

    api_token: SecretStr
    entity_id: str
    entity_group: str
    
    # Pandera
    input_schema: DataFrameModel = None

    # tracking
    request_timestamps: List[datetime] = Field(default_factory=list)

    @property
    def _base_url(self) -> str:
        return "https://api.blinkfire.com/developer/api/v1"
    
    @property
    def _base_headers(self) -> Dict:
        return {"Authorization" : f"Bearer {self.api_token.get_secret_value()}"}
    
    @property
    def _rate_limits(self) -> Dict:
        return {
            "rate_limit": 15,   # 15 requests
            "time_window": 900  # 15 minutes in seconds
        }

    ####################################################
    ### AUDIENCES ######################################
    ####################################################

    def get_audiences(self, request_date: datetime) -> pd.DataFrame | None:

        # url = f"{self._base_url}/audiences/{self.entity_id}"
        # params_list = [{"day": self._convert_dt(date)} for date in dates]
        # results = asyncio.run(self._get_results(url, params_list))

        # return self._create_dataframe(
        #     pd.json_normalize(
        #         results, 
        #         record_path=['mediums', 'channels'], 
        #         meta= ['day', ['mediums', 'medium']], 
        #         sep= "_"
        #     )
        # )

        url = f"{self._base_url}/audiences/{self.entity_id}"

        with FLA_Requests().create_session() as session:
            response = session.get(
                url = url,
                headers = self._base_headers,
                params = {"day": self._convert_dt(request_date)}
            )

        if response.status_code == 200:
            return self._create_dataframe(
                pd.json_normalize(
                    response.json(), 
                    record_path=['mediums', 'channels'], 
                    meta= ['day', ['mediums', 'medium']], 
                    sep= "_"
                )
            )
        else:
            print("Failed response")
            print(response.status_code)
            print(response.text)
            return None

        
    ####################################################
    ### DEMOGRAPHICS ###################################
    ####################################################

    # def get_demographics_channel(self, dates: List[datetime]) -> List[Dict[str, Any]]:
    def get_demographics_channel(self, request_date: datetime) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/demographics/channel"
        mediums = ["facebook", "twitter", "instagram"]

        params_list = [
            {
                "entity_id": self.entity_id, 
                "medium_name": medium, 
                "search_date": self._convert_dt(request_date)
            } for medium in mediums
        ]

        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
    
    def get_demographics_entity(self, dates: List[datetime]) -> List[Dict]:
        url = f"{self._base_url}/demographics/entity"
        params_list = [{"entity_id": self.entity_id, "search_date": self._convert_dt(date)} for date in dates]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
    
    def get_demographics_viewers(self, dates: List[datetime]) -> List[Dict]:
        url = f"{self._base_url}/reports/viewership_demographics/{self.entity_id}"
        params_list = [{"start_date": date, "end_date": date, "breakdown": "channel"} for date in dates]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))

    ####################################################
    ### ENTITIES #######################################
    ####################################################
    
    def get_teams(self) -> pd.DataFrame:
        url = f"{self._base_url}/teams/{self.entity_id}"
        results = asyncio.run(self._get_results(url))
        return self._create_dataframe(pd.json_normalize(results, sep="_"))
    
    def get_venues(self) -> pd.DataFrame:

        url = f"{self._base_url}/venues"
        params_list = [{"team": self.entity_id}]
        results = asyncio.run(self._get_results(url, params_list))

        return self._create_dataframe(
            pd.json_normalize(
                results, 
                record_path=['venues'], 
                meta=['team'], 
                sep="_"
            )
        )
    
    def get_brands(self, limit: int = 100) -> pd.DataFrame:
        url = f"{self._base_url}/brands"

        with FLA_Requests().create_session() as session:
            response = session.get(
                url = url,
                headers = self._base_headers,
                params = {"sponsoring": self.entity_id, "limit": limit}
            )

        # params_list = [{"sponsoring": self.entity_id, "limit": limit}]
        # results = asyncio.run(self._get_results(url, params_list))

        final_results = self._get_cursor_results(
            url=url,
            results=[response.json()],
            key="sponsoring",
            limit=limit
        )

        return self._create_dataframe(
            pd.json_normalize(
                final_results, 
                record_path=['brands'], 
                sep="_"
            )
        )   
    
    def get_people(self, limit: int = 10) -> pd.DataFrame:
        url = f"{self._base_url}/people"
        params_list = [{"team": self.entity_id, "limit": limit}]
        results = asyncio.run(self._get_results(url, params_list))
        final_results = self._get_cursor_results(
            url=url,
            results=results,
            key="team",
            limit=limit
        )

        return self._create_dataframe(
            pd.json_normalize(
                final_results, 
                record_path=['people'], 
                sep="_"
            )
        ) 
    
    ####################################################
    ### USER ###########################################
    ####################################################

    def get_delivered_insights(self, limit: int = 10) -> pd.DataFrame:
        url = f"{self._base_url}/user/insights/delivered"
        params_list = [{"entity": self.entity_id, "limit": limit}]
        results = asyncio.run(self._get_results(url, params_list))
        final_results = self._get_cursor_results(
            url=url,
            results=results,
            key="entity",
            limit=limit
        )

        return self._create_dataframe(
            pd.json_normalize(
                final_results, 
                record_path=['delivered_insights'], 
                sep="_"
            )
        )   
    ####################################################
    ### REPORTS ########################################
    ####################################################

    # def get_global_ranking_report(self, dates: List[datetime]) -> List[Dict]:
    def get_global_ranking_report(self, request_date: datetime) -> List[Dict]:
    #     url = f"{self._base_url}/reports/global_ranking/{self.entity_id}"
    #     params_list = [
    #         {
    #             "entity_group": self.entity_group, 
    #             "start_date": self._convert_dt(date), 
    #             "end_date": self._convert_dt(date)
    #         } for date in dates
    #     ]
    #     ## return list of json objects - to parse in etl
    #     return asyncio.run(self._get_results(url, params_list))

        url = f"{self._base_url}/reports/global_ranking/{self.entity_id}"

        with FLA_Requests().create_session() as session:
            response = session.get(
                url = url,
                headers = self._base_headers,
                params = {
                    "entity_group": self.entity_group, 
                    "start_date": self._convert_dt(request_date), 
                    "end_date": self._convert_dt(request_date)
                }
            )
        
        if response.status_code == 200:
            return response.json()
        else:
            print("Failed response")
            print(response.status_code)
            print(response.text)
            return None
        
    # def get_asset_report(self, dates: List[datetime]) -> pd.DataFrame:
    def get_asset_report(self, request_date: datetime) -> pd.DataFrame:
        # url = f"{self._base_url}/reports/assets/{self.entity_id}"
        # params_list = [
        #     {
        #         "start_date": self._convert_dt(date), 
        #         "end_date": self._convert_dt(date)
        #     } for date in dates
        # ]
        # results = asyncio.run(self._get_results(url, params_list))
        # results = [response for response in results if "entity" in response]
        # return self._create_dataframe(
        #     pd.json_normalize(
        #         results,
        #         record_path=['entity', 'by_asset'],
        #         meta=["entity_id", ["entity", "entity_name"], "start_date", "end_date"],
        #         sep='_'
        #     )
        # )

        url = f"{self._base_url}/reports/assets/{self.entity_id}"

        with FLA_Requests().create_session() as session:
            response = session.get(
                url = url,
                headers = self._base_headers,
                params = {
                    "start_date": self._convert_dt(request_date),
                    "end_date": self._convert_dt(request_date)
                }
            )

        if response.status_code == 200 and "entity" in response.json():
            return self._create_dataframe(
                pd.json_normalize(
                    response.json(),
                    record_path=['entity', 'by_asset'],
                    meta=["entity_id", ["entity", "entity_name"], "start_date", "end_date"],
                    sep='_'
                )
            )
        if response.status_code == 200 and "entity" not in response.json():
            print(f"No data available for {request_date.strftime('%Y-%m-%d')}")
            print(response.status_code)
            print(response.text)
            return pd.DataFrame()
        else:
            print("Failed response")
            print(response.status_code)
            print(response.text)
            return None
    
    def get_sponsorship_report(self, dates: List[datetime]) -> List[Dict]:
        url = f"{self._base_url}/reports/sponsors/{self.entity_id}"
        params_list = [
            {
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date)
            } for date in dates
        ]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
        
    def get_daily_engagement_report(self, dates: List[datetime]) -> pd.DataFrame:
        url = f"{self._base_url}/reports/daily_engagement/{self.entity_id}"
        params_list = [
            {
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date)
            } for date in dates
        ]
        results = asyncio.run(self._get_results(url, params_list))
        return self._create_dataframe(
            pd.json_normalize(
                results, 
                record_path=['by_day', 'by_medium'], 
                meta= ['end_date', ['by_day', 'game_day']], 
                sep="_"
            )
        )
    
    def get_streaming_report(self, dates: List[datetime], medium: Literal['youtube', 'twitch', 'huya']) -> List[Dict]:
        url = f"{self._base_url}/reports/{medium}_report/{self.entity_id}"
        params_list = [
            {
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date)
            } for date in dates
        ]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
    
    def get_scene_value_report(self, dates: List[datetime]) -> pd.DataFrame:
        url = f"{self._base_url}/reports/scene_value/{self.entity_id}"
        params_list = [
            {
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date)
            } for date in dates
        ]
        results = asyncio.run(self._get_results(url, params_list))
        return self._create_dataframe(
            pd.json_normalize(
                results, 
                record_path= ['entity', 'scenes'], 
                meta = [['entity', 'id'], ['end_date'], ['post_branding']], 
                sep = "_"
            )
        )
    
    def get_custom_reports(self, dates: List[datetime], report_id: str) -> List[Dict]:
        url = f"{self._base_url}/reports/custom_reports/{report_id}"
        params_list = [
            {
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date)
            } for date in dates
        ]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))

    ####################################################
    ### POSTS ##########################################
    ####################################################  

    # def get_posts(self, dates: List[datetime], limit: int = 100) -> List[Dict]:
    def get_posts(self, request_date: datetime, limit: int = 100) -> pd.DataFrame | None:
        # url = f"{self._base_url}/posts"
        # params_list = [
        #     {
        #         "entity": self.entity_id,
        #         "start_date": self._convert_dt(date), 
        #         "end_date": self._convert_dt(date), 
        #         "limit": limit
        #     } for date in dates
        # ]
        # results = asyncio.run(self._get_results(url, params_list))
        # final_results = self._get_cursor_results(
        #     url=url,
        #     results=results,
        #     key=None,
        #     limit=limit
        # )

        # ## return list of json objects - to parse in etl
        # return final_results
    
        url = f"{self._base_url}/posts"

        with FLA_Requests().create_session() as session:
            response = session.get(
                url = url,
                headers = self._base_headers,
                params = {
                    "entity": self.entity_id,
                    "start_date": self._convert_dt(request_date), 
                    "end_date": self._convert_dt(request_date), 
                    "limit": limit
                }
            )

        if response.status_code == 200 and "posts" in response.json().keys():
            final_results = self._get_cursor_results(
                url=url,
                results=[response.json()],
                key=None,
                limit=limit
            )
            return pd.json_normalize(final_results, record_path=["posts"], sep="_")
        else:
            print("Failed response")
            print(response.status_code)
            print(response.text)
            return None
    
    def get_sponsorship_posts(self, dates: List[datetime], limit: int = 10) -> List[Dict]:
        url = f"{self._base_url}/reports/sponsors/{self.entity_id}/posts"
        params_list = [
            {
                "author": "totals", 
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date), 
                "limit": limit
            } for date in dates
        ]
        results = asyncio.run(self._get_results(url, params_list))
        final_results = self._get_cursor_results(
            url=url,
            results=results,
            key="author",
            limit=limit
        )
        
        ## return list of json objects - to parse in etl
        return final_results

    ##############################################
    ### ASYNC FUNCTIONS ##########################
    ##############################################

    async def _get_async_request(self, url: str, params: Dict) -> httpx.Response:

        # check rate limit
        async with asyncio.Semaphore(self._rate_limits['rate_limit']):

            # wait, if necessary
            await self._enforce_rate_limit()
            
            print(f"""
                Running {url}: 
                Params: {params}
            """)
            async with FLA_Requests().create_async_session() as session:
                response = await session.get(
                    url = url,
                    headers = self._base_headers,
                    params = params
                )

            if response.status_code != 200:
                print(f"""
                    Failed to get results for:
                    Url: {url}
                    Params: {params}

                    Status code: {response.status_code}
                    Response: {response.text}
                """)
                return None 
        
        return response.json()
    
    async def _gather_responses(self, url: str, params_list: List[Dict]) -> List[Dict]:
        tasks = [self._get_async_request(url, params) for params in params_list]
        tasks = [task for task in tasks if task is not None]
        return await asyncio.gather(*tasks)

    async def _get_results(self, url: str, params_list: List[Dict] = [{}]) -> List[Dict]:
        return await self._gather_responses(url, params_list)

    async def _enforce_rate_limit(self) -> None:
        """
        Enforces the rate limit by checking request timestamps.
        Removes old timestamps and sleeps if the limit is reached.
        """
        current_time = time.time()
        self.request_timestamps = [
            ts for ts in self.request_timestamps if ts > current_time - self._rate_limits['time_window']
        ]

        if len(self.request_timestamps) >= self._rate_limits['rate_limit']:
            sleep_time = self.request_timestamps[0] + self._rate_limits['time_window'] - current_time
            print(f"Rate limit reached. Sleeping for {sleep_time:.2f} seconds.")
            await asyncio.sleep(sleep_time)

        self.request_timestamps.append(current_time)
        print(f"Request Timestamps: {self.request_timestamps}")

        return None
    
    ##############################################
    ### HELPER FUNCTIONS #########################
    ##############################################

    def _convert_dt(self, dt: datetime) -> str:
            return dt.strftime("%Y-%m-%d")
    
    def _create_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:

        if self.input_schema:
            return DataFrame[self.input_schema](df)
        else:
            return pd.DataFrame(df)

    def _get_cursor_results(
        self, 
        url,
        results: List[Dict],
        key: str | None,
        limit: int
    ) -> List[Dict]:
        
        def _get_key(key: str | None) -> Dict:

            if key is None:
                return {}
            elif key == "author":
                return {"author": "totals"}
            else:
                return {key: self.entity_id}

        # prepare final results
        final_results = [result for result in results]

        # loop through and test for next page
        for result in results:

            result_keys = list(result.keys())                

            while 'next_page' in result_keys:
                
                cursor = result['next_page']
                params = {
                    **(_get_key(key)),
                    'cursor': cursor, 
                    'limit': limit
                }
                
                with FLA_Requests().create_session() as session:
                    new_result = session.get(
                        url=url,
                        headers=self._base_headers,
                        params=params
                    )
                
                final_results.append(new_result.json())
                result_keys = list(new_result.json().keys())

        return final_results