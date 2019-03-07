import requests
import logging
from functools import lru_cache
import time

logger = logging.getLogger(__name__)

API_VERSION = 'v201806'
REPORTS_DEFINITIONS = {
    'BASE_PATH': 'https://developers.google.com/adwords/api/docs/appendix/reports/',
    'CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT': 'campaign-negative-keywords-performance-report.csv',
    'CRITERIA_PERFORMANCE_REPORT': 'criteria-performance-report.csv',
    'AD_PERFORMANCE_REPORT': 'ad-performance-report.csv',
    'KEYWORDS_PERFORMANCE_REPORT': 'keywords-performance-report.csv',
    'SEARCH_QUERY_PERFORMANCE_REPORT': 'search-query-performance-report.csv',
    'CAMPAIGN_PERFORMANCE_REPORT': 'campaign-performance-report.csv',
    'ADGROUP_PERFORMANCE_REPORT': 'adgroup-performance-report.csv',
    'CAMPAIGN_LOCATION_TARGET_REPORT': 'campaign-location-target-report.csv',
    'CLICK_PERFORMANCE_REPORT': 'click-performance-report.csv',
    'BUDGET_PERFORMANCE_REPORT': 'budget-performance-report.csv',
    'LABEL_REPORT': 'label-report.csv',
}
# https://developers.google.com/adwords/api/docs/appendix/geotargeting
LOCATIONS_URL = 'https://goo.gl/2BXevL'


@lru_cache()
def get_locations_csv():
    return requests.get(LOCATIONS_URL).content.decode('utf-8')


@lru_cache()
def get_report_csv(report_type):
    csv_url = '{}{}/{}'.format(REPORTS_DEFINITIONS['BASE_PATH'],
                               API_VERSION,
                               REPORTS_DEFINITIONS[report_type])
    result = requests.get(csv_url)
    if result.status_code == 200:
        return requests.get(csv_url).content.decode('utf-8')
    csv_url = '{}{}'.format(REPORTS_DEFINITIONS['BASE_PATH'],
                            REPORTS_DEFINITIONS[report_type])
    return requests.get(csv_url).content.decode('utf-8')


class BaseResult:
    def __init__(self, callback, parameters):
        self.callback = callback
        self.callback_parameters = parameters
        self.result = None

    def __repr__(self):
        return self.result.__repr__()

    def __iter__(self):
        if self.result and 'value' in self.result:
            for entry in self.result.value:
                yield entry


class SyncReturnValue(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        label_operations = [adw_op for adw_op in parameters if 'labelId' in adw_op['operand']]
        regular_operations = [adw_op for adw_op in parameters if 'labelId' not in adw_op['operand']]

        if label_operations:
            self.result = self._upload_sync_operations(callback.mutateLabel, label_operations)
            self.operations_sent = label_operations
        elif regular_operations:
            self.result = self._upload_sync_operations(callback.mutate, regular_operations)
            self.operations_sent = regular_operations

    def _upload_sync_operations(self, callback, operations):
        fail_counter = 0
        done = False
        while not done:
            try:
                results = callback(operations)
                done = True
                return results
            except Exception as e:
                fail_counter += 1
                time.sleep(fail_counter*60)
                if fail_counter > 3:
                    logger.error('Problem sync uploading the data, failure...')
                    raise e
                logger.error('Problem sync uploading the data, retrying for the %s time...', fail_counter)
                logger.error(str(e))

    def get_errors(self):
        if self.result and 'partialFailureErrors' in self.result:
            for entry in self.result.partialFailureErrors:
                entry['operation_failed'] = self.operations_sent[entry.fieldPathElements[0].index]
                yield entry


class SimpleReturnValue(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        self.result = callback(parameters)


class PagedResult(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        self.start_index = self.callback_parameters['paging']['startIndex']
        self.page_size = self.callback_parameters['paging']['numberResults']
        self.current_index = 0
        self._reset()

    def _reset(self):
        self.current_index = self.start_index
        self.result = None

    def get_next_page(self):
        self.callback_parameters['paging']['startIndex'] = self.current_index
        self.result = self.callback(self.callback_parameters)
        self.current_index += self.page_size
        return self.result

    def more_pages(self):
        if self.result is None:
            return True
        return self.current_index < self.result['totalNumEntries']

    def __iter__(self):
        entries_exist = True
        while entries_exist and self.more_pages():
            result = self.get_next_page()
            entries_exist = 'entries' in result
            if entries_exist:
                for entry in result['entries']:
                    yield entry
        self._reset()
        raise StopIteration


class BaseService:
    def __init__(self, client, service_name):
        self.client = client
        self.service_name = service_name
        self._service = None
        self.helper = None
        self.ResultProcessor = None

    @property
    def service(self):
        if not self._service:
            self._service = self.client.GetService(self.service_name, version=API_VERSION)
        return self._service

    def prepare_mutate(self, sync=None):
        self.helper = SyncServiceHelper(self)
        if sync:
            self.ResultProcessor = SyncReturnValue
        else:
            self.ResultProcessor = SimpleReturnValue

    def get(self, operation, client_id=None):
        self.ResultProcessor = PagedResult
        if client_id:
            self.client.SetClientCustomerId(client_id)
        operation = list(operation)
        if operation:
            operation = operation[0]
        else:
            raise RuntimeError('The operation object is empty')
        return self.ResultProcessor(self.service.get, operation)

    def mutate(self, client_customer_id=None, sync=None):
        if client_customer_id:
            self.client.SetClientCustomerId(client_customer_id)
        if sync:
            return self.ResultProcessor(self.service, self.helper.operations)
        return self.ResultProcessor(self.service.mutate, self.helper.operations)


class Selector:
    def __init__(self):
        self.selector = None
        self.selector = {
            'xsi_type': 'Selector',
            'paging': {
                'xsi_type': 'Paging',
                'startIndex': 0,
                # maximum number of results allowed by API
                # https://developers.google.com/adwords/api/docs/appendix/limits#general
                'numberResults': 10000,
            },
            'fields': [],
            'predicates': [],
            'ordering': [],
        }

    def __repr__(self):
        return self.selector.__repr__()

    def add_fields(self, *args):
        self.selector['fields'].extend(args)

    def add_predicate(self, field, operator, values):
        predicate = {
            'xsi_type': 'Predicate',
            'field': field,
            'operator': operator,
            'values': values,
        }
        self.selector['predicates'].append(predicate)


class SyncServiceHelper:
    def __init__(self, service):
        self.service = service
        self.operations = []

    def add_operation(self, operation):
        if self.operations:
            if operation.get('xsi_type', None) != self.operations[-1].get('xsi_type', None):
                raise RuntimeError('Only one operation type is supported per time')
        self.operations.append(operation)

    def clear_operations(self):
        self.operations = []
