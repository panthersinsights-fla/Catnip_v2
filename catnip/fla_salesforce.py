from pydantic import BaseModel, SecretStr
from typing import List, Dict, Literal

import pandas as pd
import json
import httpx

import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from io import StringIO, BytesIO
import time 

'''
    - authorization token good for 120 minutes
'''
class FLA_Salesforce(BaseModel):

    username: SecretStr
    password: SecretStr
    security_token: SecretStr

    @property
    def _soap_login_url(self) -> str:
        return "https://login.salesforce.com/services/Soap/u/59.0"

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

    def bulk_two_ingest(
        self,
        df: pd.DataFrame,
        object_name: str,
        operation: Literal["insert", "upsert", "delete", "hardDelete", "update"],
        external_id_field_name: str = None,
        connection_dict: Dict = None 
    ) -> None:

        # get connection parameters
        if not connection_dict:
            connection_dict = self._create_connection()

        # prepare csv and create number of jobs
        df = self._convert_datetime_columns(df)
        df = self._convert_nulls(df)
        csv_data = self._convert_df_to_list_of_csvs(df)
        print(f"# CSV parts: {len(csv_data)}")

        for i, data_part in enumerate(csv_data):

            # create job
            create_job_response = self._create_job(
                connection_dict=connection_dict,
                object_name=object_name,
                operation=operation,
                external_id_field_name=external_id_field_name
            )

            # upload csv
            content_url = create_job_response['contentUrl']
            self._upload_csv(
                connection_dict=connection_dict,
                content_url=content_url,
                csv_data=data_part
            )

            # set job state to complete
            set_job_state_response = self._set_job_state(
                connection_dict=connection_dict,
                content_url=content_url
            )

            # check job status
            state = "UploadComplete"
            while state not in ["JobComplete", "Failed"]:

                job_status_response = self._check_job_status(
                    connection_dict=connection_dict,
                    content_url=content_url
                )

                state = job_status_response['state']
                time.sleep(1)

            # get failed results
            failed_results_response = self._get_failed_results(
                connection_dict=connection_dict,
                content_url=content_url
            )

            failed_df = pd.DataFrame()
            if failed_results_response:
                failed_df = pd.read_csv(BytesIO(failed_results_response))
                print("Failed Results:")
                print(failed_df.to_markdown())

            # get unprocessed results
            unprocessed_results_response = self._get_unprocessed_results(
                connection_dict=connection_dict,
                content_url=content_url
            )

            unprocessed_df = pd.DataFrame()
            if unprocessed_results_response:
                unprocessed_df = pd.read_csv(BytesIO(unprocessed_results_response))
                print("Unprocessed Results:")
                print(unprocessed_df.to_markdown())

        return failed_df


    #########################
    ### PROCESS FUNCTIONS ###
    #########################

    def _create_job(
        self,
        connection_dict: Dict,
        object_name: str,
        operation: str,
        external_id_field_name: str = None
    ) -> Dict:

        with self._create_session() as session:

            response = session.post(
                url = f"https://{urlparse(connection_dict['server_url']).hostname}/services/data/v59.0/jobs/ingest",
                headers = {
                    "Authorization": f"Bearer {connection_dict['session_id']}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "X-PrettyPrint": "1"
                },
                data = json.dumps({
                    "object": object_name,
                    "contentType": "CSV",
                    "operation": operation,
                    "lineEnding": "LF",
                    **({"externalIdFieldName": external_id_field_name} if external_id_field_name is not None else {}),
                })
            )

            print(f"Create Job Status: {response.status_code}")
            print(response.json())

        return response.json()

    def _upload_csv(
        self,
        connection_dict: Dict,
        content_url: str,
        csv_data: str
    ) -> None:

        # make request
        with self._create_session() as session:

            response = session.put(
                url = f"https://{urlparse(connection_dict['server_url']).hostname}/{content_url}",
                headers = {
                    "Authorization": f"Bearer {connection_dict['session_id']}",
                    "Content-Type": "text/csv",
                    "Accept": "application/json",
                    "X-PrettyPrint": "1"
                },
                data = csv_data
            )

            print(f"Upload CSV Status: {response.status_code}") 

        return None 
    
    def _set_job_state(
        self,
        connection_dict: Dict,
        content_url: str
    ) -> Dict:
        
        content_url = content_url.replace("/batches", "")

        with self._create_session() as session:

            response = session.patch(
                url = f"https://{urlparse(connection_dict['server_url']).hostname}/{content_url}",
                headers = {
                    "Authorization": f"Bearer {connection_dict['session_id']}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "charset": "UTF-8",
                    "X-PrettyPrint": "1"
                },
                data = json.dumps({
                    "state": "UploadComplete"
                })
            )

            print(f"Set Job Status Status: {response.status_code}") 

        return response.json()
    
    def _check_job_status(
        self,
        connection_dict: Dict,
        content_url: str
    ) -> Dict:
        
        content_url = content_url.replace("/batches", "")

        with self._create_session() as session:

            response = session.get(
                url = f"https://{urlparse(connection_dict['server_url']).hostname}/{content_url}",
                headers = {
                    "Authorization": f"Bearer {connection_dict['session_id']}",
                    "Accept": "application/json",
                    "X-PrettyPrint": "1"
                }
            )

            print(f"Check Job Status Status: {response.status_code}") 
            print(f"Check Job Status Response: {json.dumps(response.json())}") 

        return response.json()
    
    def _get_failed_results(
        self,
        connection_dict: Dict,
        content_url: str
    ) -> str:
        
        content_url = content_url.replace("/batches", "/failedResults/")

        with self._create_session() as session:

            response = session.get(
                url = f"https://{urlparse(connection_dict['server_url']).hostname}/{content_url}",
                headers = {
                    "Authorization": f"Bearer {connection_dict['session_id']}",
                    "Content-Type": "application/json",
                    "Accept": "text/csv",
                    "X-PrettyPrint": "1"
                }
            )

            print(f"Get Failed Results Status: {response.status_code}") 

        return response.content

    def _get_unprocessed_results(
        self,
        connection_dict: Dict,
        content_url: str
    ) -> str:
        
        content_url = content_url.replace("/batches", "/unprocessedRecords/")

        with self._create_session() as session:

            response = session.get(
                url = f"https://{urlparse(connection_dict['server_url']).hostname}/{content_url}",
                headers = {
                    "Authorization": f"Bearer {connection_dict['session_id']}",
                    "Content-Type": "application/json",
                    "Accept": "text/csv",
                    "X-PrettyPrint": "1"
                }
            )

            print(f"Get Unprocessed Results Status: {response.status_code}") 

        return response.content
    
    ########################
    ### HELPER FUNCTIONS ###
    ########################
    
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

    def _create_session(self) -> httpx.Client:

        transport = httpx.HTTPTransport(retries = 5)
        client = httpx.Client(transport = transport, timeout=45)

        return client

    def _convert_datetime_columns(
        self, 
        df: pd.DataFrame, 
        format_str: str = "%Y-%m-%dT%H:%M:%S.%f%z"
    ) -> pd.DataFrame:
        
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime(format_str)
        
        return df

    def _convert_nulls(
        self, 
        df: pd.DataFrame
    ) -> pd.DataFrame:
        
        df = df.fillna("#N/A")
        
        return df

    def _convert_df_to_list_of_csvs(
        self,
        df: pd.DataFrame,
        max_size_bytes: int = 1000 * 1024 * 1024
    ) -> List[StringIO]:
        
        # Create an in-memory file-like object to hold the CSV data
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False, lineterminator="\n", escapechar="\"")
        csv_data = csv_buffer.getvalue()
        
        # Check if the data size exceeds the specified limit
        if len(csv_data.encode("utf-8")) > max_size_bytes:
            num_parts = (len(csv_data) + max_size_bytes - 1) // max_size_bytes  # Calculate the number of parts
            
            # Split the data into parts
            data_parts = [csv_data[i * max_size_bytes:(i + 1) * max_size_bytes] for i in range(num_parts)]
        else:
            data_parts = [csv_data]
        
        return data_parts