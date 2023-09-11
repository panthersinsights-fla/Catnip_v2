from pydantic import BaseModel

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
from urllib3.util.retry import Retry


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

    def _create_session(self) -> httpx.Client:

        retry = Retry(total = 5, backoff_factor = 0.5)
        transport = httpx.HTTPTransport(retries = retry)
        client = httpx.Client(transport = transport)

        return client
    
    def _get_dataframe(self, url: str, key: str = None, second_key: str = None) -> pd.DataFrame:

        with self._create_session() as session:
            try:

                if key is not None:

                    if second_key is not None:

                        if self.input_schema:
                            return DataFrame[self.input_schema](pd.DataFrame.from_dict(pd.json_normalize((session.get(url)).json()[key][0][second_key])))
                        else:
                            return pd.DataFrame.from_dict(pd.json_normalize((session.get(url)).json()[key][0][second_key]))
                    else:

                        if self.input_schema:
                            return DataFrame[self.input_schema](pd.DataFrame.from_dict(pd.json_normalize((session.get(url)).json()[key])))
                        else:
                            return pd.DataFrame.from_dict(pd.json_normalize((session.get(url)).json()[key])) 
                else:

                    if self.input_schema:
                        return DataFrame[self.input_schema](pd.DataFrame.from_dict(pd.json_normalize((session.get(url)).json())))
                    else:
                        return pd.DataFrame.from_dict(pd.json_normalize((session.get(url)).json()))
            except:

                print(f"No data at: {url}")
                return pd.DataFrame()