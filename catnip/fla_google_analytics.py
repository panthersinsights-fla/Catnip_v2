from pydantic import BaseModel

from google.analytics.data_v1beta import BetaAnalyticsDataAsyncClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunReportResponse
)

from pandera import DataFrameModel
import pandas as pd

from datetime import datetime

class FLA_Google_Analytics(BaseModel):
    '''
        - requires credentials embedded into environment
    '''

    input_schema: DataFrameModel = None 

    async def run_report(
        self,
        property_id: int,
        request_date: datetime,
        dimension: str,
        property_name: str = None
    ) -> pd.DataFrame:
        
        ## get response
        response = await self._get_response(
            property_id=property_id,
            request_date=request_date,
            dimension=dimension, 
        )

        ## convert to dataframe
        df = self._convert_response(
            response=response,
            property_id=property_id,
            request_date=request_date,
            property_name=property_name
        )

        return df 
    

    ### class functions ###

    async def _get_response(
        self,
        property_id: int,
        request_date: datetime,
        dimension: str,  
    ) -> RunReportResponse:
        
        client = BetaAnalyticsDataAsyncClient()

        request = RunReportRequest(
            property=f"properties/{str(property_id)}",
            dimensions=[
                Dimension(name=dimension)
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="newUsers"),
                Metric(name="totalUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
                Metric(name="conversions"),
                Metric(name="engagedSessions"),
                Metric(name="eventCount"),
                Metric(name="eventValue"),
                Metric(name="userEngagementDuration")
            ],
            date_ranges=[
                DateRange(
                    start_date=request_date.strftime("%Y-%m-%d"),
                    end_date=request_date.strftime("%Y-%m-%d")
                )
            ],
            keep_empty_rows = True,
            limit = 250000
        )

        response = await client.run_report(request)

        return response 
    
    def _convert_response(
        self,
        response: RunReportResponse,
        property_id: int,
        request_date: datetime,
        property_name: str = None
    ) -> pd.DataFrame:
        

        df = pd.DataFrame(
            [{
                **{dim_header.name: dim_value.value for dim_value, dim_header in zip(row.dimension_values, response.dimension_headers)},
                **{met_header.name: met_value.value for met_value, met_header in zip(row.metric_values, response.metric_headers)},
                **{'property_id': property_id},
                **{'report_date': request_date},
                **{'property_name': property_name}
            } for row in response.rows]
        )

        return df 