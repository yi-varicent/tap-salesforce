# pylint: disable=protected-access
import singer
import json
import singer.utils as singer_utils
from requests.exceptions import HTTPError
from tap_salesforce.salesforce.exceptions import TapSalesforceException

LOGGER = singer.get_logger()


class ReportRest():

    def __init__(self, sf):
        self.sf = sf

    def query(self, catalog_entry, state):
        # SalesForce Report Rest API Documentation: https://developer.salesforce.com/docs/atlas.en-us.api_analytics.meta/api_analytics/sforce_analytics_rest_api_resource_reference.htm
        # Here's how we get report data:
        #  1- Get the reportId (tap_streap_id ) this is sent from symon
        #  2- Get the report details. This is required since we will use the reportMetadata in the next call
        #  3- Query the report, by passing in the reportMetadata.
        #     " Run a report without creating a report or changing an existing one by making a POST request to the query resource. "
        #  4- Report data that is returned is a bit different than normal object data (they have links, etc..)
        #     We'll do an intial transform so that we can pass the rows to the singer for transformation

        # Getting the report Id from the config (should be the same as the catalog entry)
        report = self.sf.describe()

        return self._get_report_data(report, catalog_entry)

    def _get_report_data(
            self,
            report_metadata,
            catalog_entry):
        body = {"reportMetadata": report_metadata['reportMetadata']}
        url = f'{self.sf.instance_url}/services/data/v48.0/analytics/reports/query'

        headers = self.sf._get_report_query_headers()

        sync_start = singer_utils.now()

        try:
            resp = self.sf._make_request(
                'POST', url, headers=headers, body=json.dumps(body))
            resp_json = resp.json()
            # T!T rows feature only exists when detail feature is selected in salesforce reports
            report_results = resp_json.get('factMap').get("T!T").get('rows')
            detail_column_info = resp_json.get(
                'reportExtendedMetadata').get('detailColumnInfo')
            detail_columns = resp_json.get(
                'reportMetadata').get('detailColumns')
            return self.__transform_report_api_result(report_results, detail_columns, detail_column_info)

        except HTTPError as ex:
            response = ex.response.json()
            if isinstance(response, list) and response[0].get("errorCode") == "QUERY_TIMEOUT":
                LOGGER.info(
                    "Salesforce returned QUERY_TIMEOUT querying %s",
                    catalog_entry['stream'])
            raise ex

    def __transform_report_api_result(self, report_results, detail_columns, detail_column_info):
        # Transform and cleanup results
        # if detail rows is not selected, report_results will be NoneType
        if report_results == None: return []

        results = []
        for row in report_results:
            data_cell = row['dataCells']
            tmp_row = {}
            for i in range(0, len(row['dataCells'])):
                # If value is none, then the label sometimes is `-` , that's why we have to check for nulls by checking the value
                # For some fileds, value can be a link to that object, so we can't actually use it, that's why we only use label.
                # There will be more corner cases with other types of reports, that all should be handled here
                if data_cell[i]['value'] != None:
                    if detail_column_info.get(detail_columns[i]).get('dataType') in set(['date', 'datetime']):
                        tmp_row[detail_columns[i]] = data_cell[i]['value']
                    else:
                        tmp_row[detail_columns[i]] = data_cell[i]['label']
                else:
                    tmp_row[detail_columns[i]] = ''

            results.append(tmp_row)

        return results
