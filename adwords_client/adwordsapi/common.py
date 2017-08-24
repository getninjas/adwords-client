import requests
import logging
from suds import TypeNotFound

logger = logging.getLogger(__name__)

API_VERSION = 'v201708'
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


def get_report_csv(report_type):
    # TODO: download the csv version associated with the version of the api
    csv_url = '{}{}'.format(REPORTS_DEFINITIONS['BASE_PATH'],
                            REPORTS_DEFINITIONS[report_type])
    result = requests.get(csv_url)
    if result.status_code == 200:
        return result
    csv_url = '{}{}/{}'.format(REPORTS_DEFINITIONS['BASE_PATH'],
                               API_VERSION,
                               REPORTS_DEFINITIONS[report_type])
    return requests.get(csv_url)


class SudsFactory:
    def __init__(self, suds_client):
        self.suds_client = suds_client

    def get_object(self, name, namespace=None):
        if namespace:
            wsdl = '{{https://adwords.google.com/api/adwords/{}/{}}}{}'.format(namespace, API_VERSION, name)
            return self.suds_client.factory.create(wsdl)
        else:
            try:
                wsdl = '{{https://adwords.google.com/api/adwords/cm/{}}}{}'.format(API_VERSION, name)
                return self.suds_client.factory.create(wsdl)
            except TypeNotFound:
                pass
            try:
                wsdl = '{{https://adwords.google.com/api/adwords/o/{}}}{}'.format(API_VERSION, name)
                return self.suds_client.factory.create(wsdl)
            except TypeNotFound:
                pass
            raise NameError(name)


class BaseResult:
    def __init__(self, callback, parameters):
        self.callback = callback
        self.callback_parameters = parameters
        self.result = None

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return self.result.__getattribute__(item)

    def __contains__(self, item):
        return item in self.result

    def __repr__(self):
        return self.result.__repr__()


class SimpleReturnValue(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        self.result = callback(parameters)

    def __iter__(self):
        if 'value' in self:
            for entry in self.value:
                yield entry
        else:
            return iter(())

    def __getitem__(self, item):
        if 'value' in self:
            return self.value[item]
        else:
            raise IndexError('value not present')


class SimpleResult(BaseResult):
    def __init__(self, callback, parameters):
        super().__init__(callback, parameters)
        self.result = callback(parameters)


class PagedResult(BaseResult):
    def __iter__(self):
        start_index = self.callback_parameters.paging.startIndex
        original_start_index = start_index
        page_size = self.callback_parameters.paging.numberResults
        more_pages = True
        while more_pages:
            self.result = self.callback(self.callback_parameters)
            if 'entries' in self:
                for entry in self.entries:
                    yield entry
            else:
                self.callback_parameters.paging.startIndex = original_start_index
                raise StopIteration
            start_index += page_size
            self.callback_parameters.paging.startIndex = start_index
            more_pages = start_index < self.totalNumEntries
        self.callback_parameters.paging.startIndex = original_start_index
        raise StopIteration


class BaseService(SudsFactory):
    def __init__(self, client, service_name):
        self.client = client
        self.service_name = service_name
        self.service = client.GetService(service_name, version=API_VERSION)
        self.suds_client = self.service.suds_client
        self.helper = None
        self.ResultProcessor = None

    # Default selector for get, should be overwritten if necessary.
    def prepare_get(self):
        self.helper = Selector(self.service)
        self.ResultProcessor = PagedResult

    def get(self, client_customer_id=None):
        """

        :param client_customer_id:
        :param number_results:
        :param start_index:
        :param min_date: Default as in
        https://developers.google.com/adwords/api/docs/reference/v201601/DataService.Selector
        :param max_date: Default as in
        https://developers.google.com/adwords/api/docs/reference/v201601/DataService.Selector
        :return:
        """
        if client_customer_id:
            self.client.SetClientCustomerId(client_customer_id)

        # manually setting the headers and sending the objects
        soap_header = self.get_object('SoapHeader', 'cm')
        soap_header.clientCustomerId = self.client.client_customer_id
        soap_header.developerToken = self.client.developer_token
        soap_header.userAgent = self.client.user_agent
        self.suds_client.set_options(
            soapheaders=soap_header,
            headers=self.client.oauth2_client.CreateHttpHeader()
        )
        return self.ResultProcessor(self.suds_client.service.get, self.helper.selector)

    def mutate(self, client_customer_id=None):
        if client_customer_id:
            self.client.SetClientCustomerId(client_customer_id)

        # manually setting the headers and sending the objects
        soap_header = self.get_object('SoapHeader', 'cm')
        soap_header.clientCustomerId = self.client.client_customer_id
        soap_header.developerToken = self.client.developer_token
        soap_header.userAgent = self.client.user_agent
        self.suds_client.set_options(
            soapheaders=soap_header,
            headers=self.client.oauth2_client.CreateHttpHeader()
        )
        return self.ResultProcessor(self.service.mutate, self.helper.operations)


class BaseSelector(SudsFactory):
    def __init__(self, service):
        self.suds_client = service.suds_client
        self.selector = None

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            return self.selector.__getattribute__(item)

    def __repr__(self):
        return self.selector.__repr__()


class Selector(BaseSelector):
    def __init__(self, service):
        super().__init__(service)
        self.selector = self.get_object('Selector', 'cm')
        delattr(self.selector, 'dateRange')
        self.paging.startIndex = 0
        # maximum number of results allowed by API
        # https://developers.google.com/adwords/api/docs/appendix/limits#general
        self.paging.numberResults = 10000

    def add_fields(self, *args):
        self.fields.extend(args)

    def add_predicate(self, field, operator, values):
        predicate = self.get_object('Predicate', 'cm')
        predicate.field = field
        predicate.operator = operator
        predicate.values = values
        self.predicates.append(predicate)
