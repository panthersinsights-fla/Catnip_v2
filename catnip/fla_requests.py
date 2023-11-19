from pydantic import BaseModel
import httpx

class FLA_Requests(BaseModel):

    def create_session(self) -> httpx.Client:

        transport = httpx.HTTPTransport(retries=5)
        timeout = httpx.Timeout(45, write=None)
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