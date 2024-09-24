from pydantic import BaseModel, SecretStr
from typing import Dict, Literal

import pandas as pd
from pandera import DataFrameModel
from pandera.typing import DataFrame

import httpx
from catnip.fla_requests import FLA_Requests

from datetime import datetime
import base64
import time

from json import JSONDecodeError

class FLA_Fortress(BaseModel):

    api_key:        SecretStr
    username:       SecretStr
    password:       SecretStr
    
    input_schema:   DataFrameModel = None
    
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
    
    def _base_url_lookup(self) -> str:
        return {
            "2022-23": "https://panthers.fortressus.com/FGB_WebApplication/Panthers_22/Production/api/CRM",
            "2023-24": "https://panthers.fortressus.com/FGB_WebApplication/Panthers_23/Production/api/CRM",
            "2024-25": "https://panthers.fortressus.com/FGB_WebApplication/FGB/Production/api/CRM"
        } 

    def _endpoint_lookup(self) -> str:
        return {
            "attendance":   "TimeAttendanceInformation_Paging/",
            "events":       "EventInformation_PagingStatistics/",
            "members":      "MemberInformation_PagingStatistics/",
            "tickets":      "TicketInformation_PagingStatistics/"
        }

    #######################
    ### CLASS FUNCTIONS ###
    #######################

    def get_data(
        self, 
        endpoint: Literal["attendance", "events", "members", "tickets"],
        from_datetime: datetime, 
        to_datetime: datetime,
        season: Literal["2022-23", "2023-24", "2024-25"]
    ) -> pd.DataFrame:
        
        return self._request_loop(
            endpoint = self._endpoint_lookup()[endpoint],
            base_payload = {
                **self._get_base_payload(),
                "FromDateTime": from_datetime.strftime("%Y-%m-%dT%H:%M:%S"),
                "ToDateTime": to_datetime.strftime("%Y-%m-%dT%H:%M:%S")
            },
            base_url = self._base_url_lookup()[season]
        )

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    def _request_loop(
        self, 
        endpoint: str,
        base_payload: Dict,
        base_url: str,
        max_retries: int = 5
    ) -> pd.DataFrame:

        def _get_response(session: httpx.Client, page_num: int) -> httpx.Response:
                
            retries = 1
            while retries < max_retries+1:
                try:
                    response = session.post(
                        url = f"{base_url}/{endpoint}",
                        headers = self._headers,
                        json = {**base_payload, "PageNumber": page_num}
                    )
                    response.raise_for_status()
                    return response
                
                except httpx.HTTPError as e:
                    print(f"Request failed with status code {response.status_code}")
                    print(e)
                    time.sleep(2 ** retries)
                    retries += 1
                    continue 
            
            else:
                raise Exception("Max retries exceeded")


        ### Initial Request ##############################################
        print(f"{base_url}/{endpoint}")
        print(base_payload)
        with FLA_Requests().create_session() as session:
            response = session.post(
                url = f"{base_url}/{endpoint}",
                headers = self._headers,
                json = {**base_payload, "PageNumber": 1}
            )

        try:
            num_pages = response.json()['statistics']['numberOfPages']
        except JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            print(f"Response content: {response.text}...")
            print(f"Status Code: {response.status_code}")
            raise Exception("JSON decoding failed")
        except KeyError as ke:
            print(f"Key not found: {ke}")
            print(f"Available keys: {response.json().keys()}")
            print(f"Response content: {response.text}...") 
            print(f"Status Code: {response.status_code}")
            raise Exception("Required key missing in JSON")
        except TypeError as te:
            print(f"Key not found: {te}")
            print(f"Available keys: {response.json().keys()}")
            print(f"Response content: {response.text}...")
            print(f"Status Code: {response.status_code}")
            raise Exception("Required key missing in JSON")
        
        print(f"# Pages: {num_pages}")
        response_datetime = pd.Timestamp(response.headers['Date']).astimezone("America/New_York").tz_localize(None).to_datetime64()
        responses = [response]

        ### Request Rest ##################################################
        with FLA_Requests().create_session() as session:
            for i in range(2, num_pages+1):

                try: 
                    print(f"Requesting: Page #{i}")
                    responses = [*responses, _get_response(session, i)]

                    time.sleep(4.5)
                
                except Exception as e:
                    print(e)
                    break

        ### Create dataframe ###############################################
        print(f"# Responses: {len(responses)}")
        if len(responses) == 1:
            print("Only one response, here's the JSON value:")
            print(response.json())
        
        responses = [item for response in responses for item in response.json()['data']]

        if len(responses) == 0:
            return None
        
        print(f"First dictionary of data before update: {responses[0]}")
        responses = [{k: '999' if k in ['fbMemberID', 'accountID', 'seat'] and not str(v).isdigit() else v for k, v in d.items()} for d in responses]
        print(f"First dictionary of data after update: {responses[0]}")

        print(f"# Dictionaries: {len(responses)}")

        if len(responses) > 0:

            if self.input_schema:
                df = DataFrame[self.input_schema](responses)
            else:
                df = pd.DataFrame(responses)

            df['response_datetime'] = response_datetime

            return df
        
        else:
            return None