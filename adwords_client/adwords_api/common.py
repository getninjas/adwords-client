import requests
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)

API_VERSION = 'v201802'
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
        if 'value' in self.result:
            for entry in self.value:
                yield entry


class SyncReturnValue(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        label_operations = [adw_op for adw_op in parameters if 'labelId' in adw_op['operand']]
        regular_operations = [adw_op for adw_op in parameters if 'labelId' not in adw_op['operand']]
        results = []
        if label_operations:
            partial_results = callback.mutateLabel(label_operations)
            results.append(partial_results)
        if regular_operations:
            partial_results = callback.mutate(regular_operations)
            results.append(partial_results)
        self.result = results

    def __iter__(self):
        for item in self.result:
            item_type = item['ListReturnValue.Type']
            for entry in item.value:
                entry['returnType'] = item_type
                yield entry


class SimpleReturnValue(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        self.result = callback(parameters)


class PagedResult(BaseResult):
    def __iter__(self):
        start_index = self.callback_parameters['paging']['startIndex']
        original_start_index = start_index
        page_size = self.callback_parameters['paging']['numberResults']
        more_pages = True
        while more_pages:
            self.result = self.callback(self.callback_parameters)
            if 'entries' in self.result:
                for entry in self.result['entries']:
                    yield entry
            else:
                self.callback_parameters['paging']['startIndex'] = original_start_index
                raise StopIteration
            start_index += page_size
            self.callback_parameters['paging']['startIndex'] = start_index
            more_pages = start_index < self.result['totalNumEntries']
        self.callback_parameters['paging']['startIndex'] = original_start_index
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

    # # Default selector for get, should be overwritten if necessary.
    def prepare_get(self, sync=None):
        if sync is None:
            self.helper = Selector()
        self.ResultProcessor = PagedResult

    def prepare_mutate(self, sync=None):
        self.helper = SyncServiceHelper(self)
        if sync:
            self.ResultProcessor = SyncReturnValue
        else:
            self.ResultProcessor = SimpleReturnValue

    def get(self, operation, client_id=None):
        if client_id:
            self.client.SetClientCustomerId(client_id)
        return self.ResultProcessor(self.service.get, next(operation))

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
        self.operations.append(operation)
