from pydantic import BaseModel, SecretStr
from typing import List, Dict
import pandas as pd

from prefect.blocks.system import Secret

from datetime import datetime, timedelta
import pytz

class MyHockeyGame(BaseModel):

    game_descriptions: pd.Series

    class Config:
        arbitrary_types_allowed = True

    @property
    def event_opponent(self) -> str:
        return self.game_descriptions['full_opponent']

    @property
    def event_datetime(self) -> str:
        return self.game_descriptions['event_datetime']

    @property
    def event_time_formatted(self) -> str:
        return datetime.strftime(self.game_descriptions['event_datetime'], '%#I:%M %p')

    @property
    def event_day_of_week(self) -> str:
        return datetime.strftime(self.game_descriptions['event_datetime'], '%A')
    
    @property
    def doors_time(self) -> str:
        return datetime.strftime((self.game_descriptions['event_datetime'] - timedelta(minutes = 45)), '%#I:%M %p')

    @property
    def puck_drop(self) -> str:
        return datetime.strftime((self.game_descriptions['event_datetime'] + timedelta(minutes = 15)), '%#I:%M %p')

    @property
    def final_turnstile_timestamp(self) -> str:
        return datetime.strftime((self.game_descriptions['event_datetime'] + timedelta(hours = 1, minutes = 45)), '%#I:%M %p')
    
    @property
    def current_datetime(self) -> str:
        return datetime.now(pytz.timezone("America/New_York"))

    @property
    def fortress_event_code(self) -> str:
        return self.game_descriptions['productcode']