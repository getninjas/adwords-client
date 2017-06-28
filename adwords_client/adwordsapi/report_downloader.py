import csv
import logging
import tempfile
from gzip import GzipFile
from io import BytesIO

from .common import API_VERSION

logger = logging.getLogger(__name__)

def gunzip_csv(data, fields):
    logger.debug('Creating stream from data...')
    byte_stream = BytesIO(data)
    logger.debug('Decompressing data...')
    decompress = GzipFile(fileobj=byte_stream)
    logger.debug('Decoding...')
    decoded_data = decompress.read().decode('utf-8')
    logger.debug('Creating csv reader...')
    tmp_file = tempfile.TemporaryFile('w+', encoding='utf-8')
    tmp_file.write(decoded_data)
    tmp_file.seek(0, 0)
    result = csv.DictReader(tmp_file, fieldnames=fields)
    logger.debug('Returning csv...')
    return result


class ReportDownloader:
    def __init__(self, client):
        self.client = client
        self.downloader = None
        self.get_downloader()

    def get_downloader(self):
        self.downloader = self.client.GetReportDownloader(version=API_VERSION)

    def report(self, report_type, fields, client_customer_id=None, *args, **kwargs):
        """
        Report freshness as defined in https://support.google.com/adwords/answer/2544985
        Better run after 18 pm for intervals ending by the day before
        :param client_customer_id:
        :param args:
        :param kwargs:
        :return:
        """
        return_stream = kwargs.pop('return_stream', False)
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
        try:
            result = self.downloader.DownloadReportAsStreamWithAwql(**report_params)
        except Exception as e:
            raise e
        if not return_stream:
            report_reader = gunzip_csv(result.read(), fields)
            return report_reader
        else:
            return result.read()
