from pydantic import BaseModel
import httpx

from typing import List


class FLA_Requests(BaseModel):

    '''
        - create multiple requests in single session
        - create stats object and record request statistics
        - exception handling with logged prints of status code, text, json
        - rate limiting functionality
    '''
    ################
    ### SESSIONS ###
    ################

    def create_session(self) -> httpx.Client:

        transport = httpx.HTTPTransport(retries=5)
        timeout = httpx.Timeout(90, write=None)
        client = httpx.Client(
            transport = transport, 
            timeout = timeout
        )

        return client
    
    def create_async_session(self) -> httpx.AsyncClient:

        transport = httpx.AsyncHTTPTransport(retries=5)
        timeout = httpx.Timeout(45, write=None)
        client = httpx.AsyncClient(
            transport = transport, 
            timeout = timeout
        )

        return client
    
    #############
    ### STATS ###
    #############

    class stats():

        request_times: List[float]

        @property
        def min_time(self) -> float:
            return min(self.request_times)