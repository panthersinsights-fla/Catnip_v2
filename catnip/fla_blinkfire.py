from pydantic import BaseModel, SecretStr

from pandera import DataFrameModel
from pandera.typing import DataFrame
import pandas as pd

from fla_requests import FLA_Requests
import asyncio
import httpx

from datetime import datetime
from typing import Dict, List, Literal

class FLA_Blinkfire(BaseModel):

    api_token: SecretStr
    entity_id: str
    entity_group: str
    
    # Pandera
    input_schema: DataFrameModel = None

    @property
    def _base_url(self) -> str:
        return "https://api.blinkfire.com/developer/api/v1"
    
    @property
    def _base_headers(self) -> Dict:
        return {"Authorization" : f"Bearer {self.api_token.get_secret_value()}"}

    ####################################################
    ### AUDIENCES ######################################
    ####################################################

    def get_audiences(self, dates: List[datetime]) -> pd.DataFrame:

        url = f"{self._base_url}/audiences/{self.entity_id}"
        params_list = [{"day": self._convert_dt(date)} for date in dates]
        results = asyncio.run(self._get_results(url, params_list))

        return self._create_dataframe(
            pd.json_normalize(
                results, 
                record_path=['mediums', 'channels'], 
                meta= ['day', ['mediums', 'medium']], 
                sep= "_"
            )
        )
    
    ####################################################
    ### DEMOGRAPHICS ###################################
    ####################################################

    def get_demographics_channel(self, dates: List[datetime]) -> List[dict]:
        url = f"{self._base_url}/demographics/channel"
        mediums = ["facebook", "twitter", "instagram"]

        params_list = [
            {
                "entity_id": self.entity_id, 
                "medium_name": medium, 
                "search_date": self._convert_dt(date)
            } for date in dates for medium in mediums
        ]

        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
    
    def get_demographics_entity(self, dates: List[datetime]) -> List[dict]:
        url = f"{self._base_url}/demographics/entity"
        params_list = [{"entity_id": self.entity_id, "search_date": self._convert_dt(date)} for date in dates]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
    
    def get_demographics_viewers(self, dates: List[datetime]) -> List[dict]:
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
    
    def get_brands(self, limit: int = 10) -> pd.DataFrame:
        url = f"{self._base_url}/brands"
        params_list = [{"sponsoring": self.entity_id, "limit": limit}]
        results = asyncio.run(self._get_results(url, params_list))
        final_results = self._get_cursor_results(
            url=url,
            results=results,
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

    def get_global_ranking_report(self, dates: List[datetime]) -> List[Dict]:
        url = f"{self._base_url}/reports/global_ranking/{self.entity_id}"
        params_list = [
            {
                "entity_group": self.entity_group, 
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date)
            } for date in dates
        ]
        ## return list of json objects - to parse in etl
        return asyncio.run(self._get_results(url, params_list))
        
    def get_asset_report(self, dates: List[datetime]) -> pd.DataFrame:
        url = f"{self._base_url}/reports/assets/{self.entity_id}"
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
                record_path=['entity','by_asset'], 
                meta=[['end_date'], ['entity_id'], ['entity','entity_name']], 
                sep="_"
            )
        )
    
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

    def get_posts(self, dates: List[datetime], limit: int = 10) -> List[Dict]:
        url = f"{self._base_url}/posts"
        params_list = [
            {
                "entity": self.entity_id, 
                "start_date": self._convert_dt(date), 
                "end_date": self._convert_dt(date), 
                "limit": limit
            } for date in dates
        ]
        results = asyncio.run(self._get_results(url, params_list))
        final_results = self._get_cursor_results(
            url=url,
            results=results,
            key=None,
            limit=limit
        )

        ## return list of json objects - to parse in etl
        return final_results
    
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