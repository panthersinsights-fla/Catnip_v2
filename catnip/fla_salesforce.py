from pydantic import BaseModel, SecretStr
from typing import List, Dict, Literal

import pandas as pd
import json
from datetime import datetime
import httpx

import xml.etree.ElementTree as ET

from prefect.blocks.system import Secret

# from simple_salesforce import Salesforce



'''
    - bulk api v 2.0
    https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/walkthrough_upsert.htm




'''
class FLA_Salesforce(BaseModel):

    username: SecretStr
    password: SecretStr
    security_token: SecretStr

    @property
    def _soap_login_url(self) -> str:
        return f"https://login.salesforce.com/services/Soap/u/59.0"
    
    @property
    def _soap_login_headers(self) -> Dict:
        return {
            "content-type": "text/xml",
            "charset": "UTF-8",
            "SOAPAction": "login"
        }

    #######################
    ### CLASS FUNCTIONS ###
    #######################

    # def bulk_insert_records(
    #     self,
    #     df: pd.DataFrame,
    #     object_name: str,
    #     operation: str
    # ) -> None:

    #     self._create_connection().__dict__[f"{object_name}"].__dict__[f"{operation}"](df.to_dict(orient="records"))

    #     return None

    ########################
    ### HELPER FUNCTIONS ###
    ########################

    # def _create_connection(self) -> Salesforce:

    #     return Salesforce(
    #         username = self.username.get_secret_value(), 
    #         password = self.password.get_secret_value(), 
    #         security_token = self.security_token.get_secret_value()
    #     )
    
    def _create_connection(self):

        request_body = f"""
            <?xml version="1.0" encoding="utf-8" ?>
            <env:Envelope
                    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                    xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
                    xmlns:urn="urn:partner.soap.sforce.com">
                <env:Header>
                    <urn:CallOptions>
                        <urn:client>simple-salesforce</urn:client>
                        <urn:defaultNamespace>sf</urn:defaultNamespace>
                    </urn:CallOptions>
                </env:Header>
                <env:Body>
                    <n1:login xmlns:n1="urn:partner.soap.sforce.com">
                        <n1:username>{self.username.get_secret_value()}</n1:username>
                        <n1:password>{self.password.get_secret_value()}{self.security_token.get_secret_value()}</n1:password>
                    </n1:login>
                </env:Body>
            </env:Envelope>
        """.strip()

        response = httpx.post(
            url = self._soap_login_url,
            data = request_body,
            headers = self._soap_login_headers
        )

        print(response.status_code)
        print(response.content)
        print(type(response.content))

        # Parse the XML string
        root = ET.fromstring(response.content)

        # Define the namespaces
        namespaces = {
            'soapenv': 'http://schemas.xmlsoap.org/soap/envelope/',
            'partner': 'urn:partner.soap.sforce.com',
        }

        # Find the serverUrl and sessionId elements
        server_url_element = root.find(".//partner:serverUrl", namespaces)
        session_id_element = root.find(".//partner:sessionId", namespaces)

        # Extract the text content of the elements
        server_url = server_url_element.text if server_url_element is not None else None
        session_id = session_id_element.text if session_id_element is not None else None

        # Print the results
        print("Server URL:", server_url)
        print("Session ID:", session_id)

        return {"server_url": server_url, "session_id": session_id}