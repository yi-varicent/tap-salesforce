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

        report = self.sf.describe_reports(catalog_entry['stream'])

        return self._get_report_data(report, catalog_entry)

    def _get_report_data(
            self,
            report_metadata,
            catalog_entry):
        body = {"reportMetadata": report_metadata['reportMetadata']}
        url = "{}/services/data/v48.0/analytics/reports/query".format(
            self.sf.instance_url)
        headers = self.sf._get_report_query_headers()

        sync_start = singer_utils.now()

        retryable = False
        try:
            resp = self.sf._make_request(
                'POST', url, headers=headers, body=json.dumps(body))
            resp_json = resp.json()
            report_results = resp_json.get('factMap').get("T!T").get('rows')
            return self.__transform_report_api_result(report_results, report_metadata['reportMetadata']['detailColumns'])

        except HTTPError as ex:
            response = ex.response.json()
            if isinstance(response, list) and response[0].get("errorCode") == "QUERY_TIMEOUT":
                LOGGER.info(
                    "Salesforce returned QUERY_TIMEOUT querying %s",
                    catalog_entry['stream'])
            raise ex

    def __transform_report_api_result(self, report_results, detail_columns):
        results = []
        for row in report_results:
            data_cell = row['dataCells']
            LOGGER.error(len(row['dataCells']))
            tmp_row = {}
            for i in range(0, len(row['dataCells'])):
                if data_cell[i]['value'] != None:
                    tmp_row[detail_columns[i]] = data_cell[i]['label']
                else:
                    tmp_row[detail_columns[i]] = ''

            results.append(tmp_row)

        return results
