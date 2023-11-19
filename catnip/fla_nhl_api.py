from pydantic import BaseModel
from typing import List, Dict, Any

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame


from catnip.fla_requests import FLA_Requests
import httpx
import asyncio


class FLA_NHL_API(BaseModel):

    input_schema: DataFrameModel = None 

    @property
    def _base_url(self) -> str:
        return "https://statsapi.web.nhl.com/api/v1"
    

    ####################
    ### DAILY UPDATE ###
    ####################

    def get_teams(self) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/teams", key = "teams")

    def get_venues(self) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/venues", key = "venues")

    def get_seasons(self) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/seasons", key = "seasons")

    def get_game_types(self) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/gameTypes")

    def get_game_statuses(self) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/gameStatus")

    def get_postions(self) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/positions")

    ###########
    ### ETL ###
    ###########

    def get_schedule(self, yyyymmdd_date: str, game_type: str) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/schedule?date={yyyymmdd_date}&gameType={game_type}", key = "dates", second_key = "games")
    
    def get_standings(self, season: str) -> pd.DataFrame:

        return self._get_dataframe(url = f"{self._base_url}/standings?season={season}&standingsType=byLeague")

    def get_boxscore(self, game_id: str) -> dict:

        return (self._create_session().get(f"{self._base_url}/game/{game_id}/boxscore")).json()

    def get_linescore(self, game_id: str) -> dict:

        return (self._create_session().get(f"{self._base_url}/game/{game_id}/linescore")).json()

    def get_people(self, player_id: str) -> pd.DataFrame:

        '''
            - One-time full refresh
            - Then, query for all players that exist in boxscores but don't exist in people table
        '''

        return self._get_dataframe(url = f"{self._base_url}/people/{player_id}")
    
    ########################
    ### HELPER FUNCTIONS ###
    ########################
    
    def _get_dataframe(self, url: str, key: str = None, second_key: str = None) -> pd.DataFrame:

        with FLA_Requests().create_session() as session:
            
            try:

                if key is not None:

                    if second_key is not None:

                        if self.input_schema:
                            return DataFrame[self.input_schema](pd.json_normalize((session.get(url)).json()[key][0][second_key]))
                        else:
                            return pd.json_normalize((session.get(url)).json()[key][0][second_key])
                    else:

                        if self.input_schema:
                            return DataFrame[self.input_schema](pd.json_normalize((session.get(url)).json()[key]))
                        else:
                            return pd.json_normalize((session.get(url)).json()[key])
                else:

                    if self.input_schema:
                        return DataFrame[self.input_schema](pd.json_normalize((session.get(url)).json()))
                    else:
                        return pd.json_normalize((session.get(url)).json())
            except:

                print(f"No data at: {url}")
                return pd.DataFrame()
    
    #############
    ### ASYNC ###
    #############

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
    
    def _chunk_list(self, l: List[Any], chunk_size: int) -> List[List[Any]]:

        for i in range(0, len(l), chunk_size):
            yield l[i:i + chunk_size]

    '''
    ### Request Rest ##################################################
    batches = [min(start + batch_size, num_pages+1) for start in range(2, num_pages+1, batch_size)]
    batches = [2] + batches if num_pages > 1 else batches
    '''