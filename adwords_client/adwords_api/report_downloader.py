import logging

from .common import API_VERSION

logger = logging.getLogger(__name__)


class ReportDownloader:
    def __init__(self, client):
        self.client = client
        self.downloader = None
        self.get_downloader()

    def get_downloader(self):
        self.downloader = self.client.GetReportDownloader(version=API_VERSION)

    def report(self, report_type, fields, client_customer_id=None, *args, **kwargs):
        if client_customer_id:
            self.client.SetClientCustomerId(client_customer_id)
        period = 'DURING {}'.format(kwargs['period']) if 'period' in kwargs else ''
        adgroup = 'WHERE {}'.format(' and '.join(args)) if len(args) > 0 else ''
        awql_query = 'SELECT {} FROM {} {} {}'.format(', '.join(fields), report_type, adgroup, period)
        report_params = {'query': awql_query,
                         'file_format': 'GZIPPED_CSV',
                         'include_zero_impressions':
                             kwargs['include_zero_impressions'] if 'include_zero_impressions' in kwargs else True,
                         'skip_report_header':
                             kwargs['skip_report_header'] if 'skip_report_header' in kwargs else True,
                         'skip_column_header':
                             kwargs['skip_column_header'] if 'skip_column_header' in kwargs else True,
                         'skip_report_summary':
                             kwargs['skip_report_summary'] if 'skip_report_summary' in kwargs else True}
        for key in kwargs:
            if key in report_params:
                report_params[key] = kwargs[key]
        return self.downloader.DownloadReportAsStreamWithAwql(**report_params)
