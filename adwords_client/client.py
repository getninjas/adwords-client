import csv
import datetime
import inspect
import io
import json
import logging
import time
import uuid
from collections import Mapping
from io import StringIO
from math import floor, isfinite
from tempfile import NamedTemporaryFile
from os import path
from multiprocessing import Pool

import googleads.adwords
import pandas as pd
import yaml
from sqlalchemy.exc import OperationalError

from . import adwords_api, config, sqlite, storages
from .internal_api.mappers import MAPPERS
from .internal_api.builder import OperationsBuilder
from .adwords_api import common, operations_
from .adwords_api.managed_customer_service import ManagedCustomerService
from .adwords_api.sync_job_service import SyncJobService

logger = logging.getLogger(__name__)


def _iter_floats(data):
    for item in data:
        try:
            if item and isfinite(item):
                yield float(item)
        except (TypeError, ValueError):
            pass


def multiprocessing_starmap(*args, **kwargs):
    return Pool().starmap(*args, **kwargs)


def adwords_client_factory(credentials):
    config = {'adwords': credentials}
    config_yaml = yaml.safe_dump(config)
    return googleads.adwords.AdWordsClient.LoadFromString(config_yaml)


class AdWords:
    @classmethod
    def from_credentials(cls, credentials, **kwargs):
        client = adwords_client_factory(credentials)
        return cls(client, **kwargs)

    @classmethod
    def from_file(cls, config_file):
        client = googleads.adwords.AdWordsClient.LoadFromStorage(config_file)
        return cls(client)

    def __init__(self, google_ads_client=None, workdir=None, storage=None, map_function=None):
        self._client = google_ads_client
        self.services = {}
        self._engine = None
        self.table_models = {}
        self.min_id = 0
        self.map_function = map_function or multiprocessing_starmap
        if storage:
            self.storage = storage
        else:
            self.storage = storages.FilesystemStorage(workdir) if workdir else storages.TemporaryFilesystemStorage()
        self._open_files = {}
        self._operations_buffer = None

    @property
    def client(self):
        if not self._client:
            config.configure(path)
            self._client = adwords_client_factory(config.FIELDS)
        return self._client

    @property
    def engine(self):
        if not self._engine:
            self._engine = sqlite.get_connection()
        return self._engine

    @property
    def operations(self):
        if not self._operations_buffer:
            self._operations_buffer = NamedTemporaryFile('w+')
            logger.debug('Created temporary buffer file %s', self._operations_buffer.name)
        return self._operations_buffer

    def service(self, service_name):
        if service_name not in self.services:
            self.services[service_name] = getattr(adwords_api, service_name)
        return self.services[service_name](self.client)

    def file(self, name, *args, **kwargs):
        return self._open_files.setdefault(name, self.storage.open(name, *args, **kwargs))

    def flush_files(self):
        for file in self._open_files.values():
            file.flush()

    def _write_entry(self, file_name, entry):
        self.file(file_name).write(json.dumps(entry) + '\n')

    def _read_entries(self, file_name):
        file = self.file(file_name, mode='r')
        file.flush()
        file.seek(0)
        for line in file:
            yield json.loads(line)

    def _read_from_folder(self, folder_name, name_filter=None):
        _, files = self.storage.listdir(folder_name)
        for file in files:
            if not name_filter or name_filter(file):
                yield from self._read_entries(path.join(folder_name, file))

    def _write_buffer(self, entry):
        self.operations.write(json.dumps(entry) + '\n')

    def _read_buffer(self):
        self.operations.flush()
        self.operations.seek(0)
        for line in self.operations:
            yield json.loads(line)

    @staticmethod
    def _get_dict_min_value(data):
        try:
            return min(int(floor(u)) for u in _iter_floats(data.values()))
        except ValueError:
            logger.debug('Problem getting min value for: %s', str(data))
            for k, v in data.items():
                logger.debug('Key: %s (%s) Value: %s (%s)', str(k), str(type(k)), str(v), str(type(v)))
            raise

    def _get_min_id(self, entry):
        if 'client_id' not in entry:
            raise ValueError('Every entry must have a "client_id" field.')
        self.min_id = min(self.min_id, self._get_dict_min_value(entry))
        return entry

    def insert(self, data):
        if isinstance(data, Mapping):
            data = [self._get_min_id(data)]
        else:
            data = iter(self._get_min_id(entry) for entry in data)
        for entry in data:
            self._write_buffer(entry)

    def get_report(self, report_type, customer_id, target_name,
                   create_table=False, exclude_fields=[],
                   exclude_terms=['Significance'], exclude_behavior=['Segment'],
                   include_fields=[], *args, **kwargs):
        logger.info('Getting %s...', report_type)
        simple_download = kwargs.pop('simple_download', False)
        only_fields = kwargs.pop('fields', None)
        report_csv = common.get_report_csv(report_type)
        report_csv = dict((item['Name'], item) for item in csv.DictReader(StringIO(report_csv)))
        to_remove = set([name for name, item in report_csv.items() if
                         (item['Behavior'] in exclude_behavior  # remove based on variable behavior
                          or any(term in name for term in exclude_terms)  # remove for some terms
                          or (only_fields is not None and name not in only_fields))  # only given fields
                         ])
        to_remove = to_remove.union(exclude_fields)
        fields = [field for field in report_csv if field not in to_remove]
        fields += include_fields
        fields.sort()

        rd = self.service('ReportDownloader')
        args = [report_type, fields, customer_id] + list(args)

        if not simple_download:
            report_id = kwargs.pop('report_id', None)
            reference_date = kwargs.pop('reference_date', None)
            kwargs['return_stream'] = True
            report_stream = rd.report(*args, **kwargs)
            converter = {
                field: MAPPERS.get(report_csv[field]['Type']).from_adwords_func
                for field in fields if report_csv[field]['Type'] in MAPPERS
            }
            data = pd.read_csv(io.BytesIO(report_stream),
                               compression='gzip',
                               header=None,
                               names=fields,
                               encoding='utf-8',
                               converters=converter,
                               engine='c')
            if report_id is not None:
                data['report_id'] = report_id
            if reference_date is not None:
                data['reference_date'] = reference_date
            data.to_sql(target_name,
                        self.engine,
                        index=False,
                        if_exists='replace' if create_table else 'append')
            return None
        else:
            kwargs['return_stream'] = True
            report_stream = rd.report(*args, **kwargs)
            with open(target_name, 'wb') as f:
                f.write(report_stream)
            return fields

    def get_clicks_report(self, customer_id, target_name, *args, **kwargs):
        include_fields = kwargs.pop('include_fields', [])
        exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
        exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
        exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
        create_table = kwargs.pop('create_table', False)
        kwargs['include_zero_impressions'] = False
        return self.get_report('CLICK_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               exclude_behavior,
                               include_fields,
                               *args, **kwargs)

    def get_negative_keywords_report(self, customer_id, target_name, *args, **kwargs):
        create_table = kwargs.pop('create_table', False)
        exclude_fields = ['ConversionTypeName']
        exclude_terms = []
        return self.get_report('CAMPAIGN_NEGATIVE_KEYWORDS_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               ['Segment'],
                               [],
                               *args, **kwargs)

    def get_criteria_report(self, customer_id, target_name, *args, **kwargs):
        create_table = kwargs.pop('create_table', False)
        exclude_fields = ['ConversionTypeName']
        exclude_terms = ['Significance', 'ActiveView', 'Average']
        return self.get_report('CRITERIA_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               ['Segment'],
                               [],
                               *args, **kwargs)

    def get_ad_performance_report(self, customer_id, target_name, *args, **kwargs):
        logger.debug('Running get_ad_performance_report...')
        include_fields = kwargs.pop('include_fields', [])
        exclude_fields = kwargs.pop('exclude_fields', ['BusinessName',
                                                       'ConversionTypeName',
                                                       'CriterionType',
                                                       'CriterionId',
                                                       'ClickType',
                                                       'ConversionCategoryName',
                                                       'ConversionTrackerId',
                                                       'IsNegative'])
        exclude_terms = kwargs.pop('exclude_terms', ['Significance', 'ActiveView', 'Average'])
        exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
        create_table = kwargs.pop('create_table', False)
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',
                    'ExternalCustomerId',
                    'CampaignId',
                    'CampaignName',
                    'CampaignStatus',
                    'AdGroupId',
                    'AdGroupName',
                    'AdGroupStatus',
                    'Id',
                    'Impressions',
                    'Clicks',
                    'Conversions',
                    'Cost',
                    'Status',
                    'CreativeUrlCustomParameters',
                    'CreativeTrackingUrlTemplate',
                    'CreativeDestinationUrl',
                    'CreativeFinalUrls',
                    'CreativeFinalMobileUrls',
                    'CreativeFinalAppUrls',
                    'Headline',
                    'HeadlinePart1',
                    'HeadlinePart2',
                    'Description',
                    'Description1',
                    'Description2',
                    'Path1',
                    'Path2',
                    'DisplayUrl',
                ]
        return self.get_report('AD_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               exclude_behavior,
                               include_fields,
                               *args, **kwargs)

    def get_keywords_report(self, customer_id, target_name, *args, **kwargs):
        create_table = kwargs.pop('create_table', False)
        exclude_fields = ['ConversionTypeName']
        exclude_terms = ['Significance']
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',
                    'ExternalCustomerId',
                    'CampaignId',
                    'CampaignName',
                    'CampaignStatus',
                    'AdGroupId',
                    'AdGroupName',
                    'AdGroupStatus',
                    'Id',
                    'Impressions',
                    'Clicks',
                    'Conversions',
                    'Cost',
                    'Status',
                    'KeywordMatchType',
                    'Criteria',
                    'BiddingStrategySource',
                    'BiddingStrategyType',
                    'SearchImpressionShare',
                    'CpcBid',
                    'CreativeQualityScore',
                    'PostClickQualityScore',
                    'SearchPredictedCtr',
                    'QualityScore',
                ]
        return self.get_report('KEYWORDS_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               ['Segment'],
                               [],
                               *args, **kwargs)

    def get_search_terms_report(self, customer_id, target_name, *args, **kwargs):
        create_table = kwargs.pop('create_table', False)
        exclude_fields = ['ConversionTypeName']
        exclude_terms = ['Significance']
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',
                    'ExternalCustomerId',
                    'CampaignId',
                    'CampaignName',
                    'CampaignStatus',
                    'AdGroupId',
                    'AdGroupName',
                    'AdGroupStatus',
                    'Impressions',
                    'Clicks',
                    'Conversions',
                    'Cost',
                    'Status',
                    'KeywordId',
                    'KeywordTextMatchingQuery',
                    'Query',
                ]
        return self.get_report('SEARCH_QUERY_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               ['Segment'],
                               [],
                               *args, **kwargs)

    def get_campaigns_report(self, customer_id, target_name, *args, **kwargs):
        include_fields = kwargs.pop('include_fields', [])
        exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
        exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
        exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
        create_table = kwargs.pop('create_table', False)
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',
                    'ExternalCustomerId',
                    'CampaignId',
                    'CampaignName',
                    'CampaignStatus',
                    'Impressions',
                    'Clicks',
                    'Conversions',
                    'Cost',
                    'Status',
                    'BiddingStrategyType',
                    'SearchImpressionShare',
                ]
        return self.get_report('CAMPAIGN_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               exclude_behavior,
                               include_fields,
                               *args, **kwargs)

    def get_labels_report(self, customer_id, target_name, *args, **kwargs):
        """
        Get report from AdWords account 'customer_id' and save to Redshift 'target_name' table
        """
        include_fields = kwargs.pop('include_fields', [])
        exclude_fields = kwargs.pop('exclude_fields', [])
        exclude_terms = kwargs.pop('exclude_terms', [])
        exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
        create_table = kwargs.pop('create_table', False)
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',  # The descriptive name of the Customer account.
                    'ExternalCustomerId',  # The Customer ID.
                    'LabelId',
                    'LabelName',
                ]
        return self.get_report('LABEL_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               exclude_behavior,
                               include_fields,
                               *args, **kwargs)

    def get_budget_report(self, customer_id, target_name, *args, **kwargs):
        """
        Get report from AdWords account 'customer_id' and save to Redshift 'target_name' table
        """
        include_fields = kwargs.pop('include_fields', [])
        exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
        exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
        exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
        create_table = kwargs.pop('create_table', False)
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',  # The descriptive name of the Customer account.
                    'ExternalCustomerId',  # The Customer ID.
                    'BudgetId',
                    'BudgetName',
                    'BudgetReferenceCount',  # The number of campaigns actively using the budget.
                    'Amount',  # The daily budget
                    'IsBudgetExplicitlyShared',
                    # Shared budget (true) or specific to the campaign (false)
                ]
        return self.get_report('BUDGET_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               exclude_behavior,
                               include_fields,
                               *args, **kwargs)

    def get_adgroups_report(self, customer_id, target_name, *args, **kwargs):
        include_fields = kwargs.pop('include_fields', [])
        exclude_fields = kwargs.pop('exclude_fields', ['ConversionTypeName'])
        exclude_terms = kwargs.pop('exclude_terms', ['Significance'])
        exclude_behavior = kwargs.pop('exclude_behavior', ['Segment'])
        create_table = kwargs.pop('create_table', False)
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',
                    'ExternalCustomerId',
                    'CampaignId',
                    'CampaignName',
                    'CampaignStatus',
                    'AdGroupId',
                    'AdGroupName',
                    'AdGroupStatus',
                    'Impressions',
                    'Clicks',
                    'Conversions',
                    'Cost',
                    'Status',
                    'BiddingStrategySource',
                    'BiddingStrategyType',
                    'SearchImpressionShare',
                    'CpcBid',
                    'Labels',
                ]
        return self.get_report('ADGROUP_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               exclude_behavior,
                               include_fields,
                               *args, **kwargs)

    def get_campaigns_location_report(self, customer_id, target_name, *args, **kwargs):
        create_table = kwargs.pop('create_table', False)
        exclude_fields = []
        exclude_terms = ['Significance']
        use_fields = kwargs.pop('fields', False)
        if use_fields:
            try:
                kwargs['fields'] = list(use_fields)
            except TypeError:
                kwargs['fields'] = [
                    'AccountDescriptiveName',
                    'ExternalCustomerId',
                    'CampaignId',
                    'CampaignName',
                    'CampaignStatus',
                    'Id',
                    'IsNegative',
                    'BidModifier',
                    'Impressions',
                    'Clicks',
                    'Conversions',
                    'Cost',
                ]
        return self.get_report('CAMPAIGN_LOCATION_TARGET_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               ['Segment'],
                               [],
                               *args, **kwargs)

    def create_batch_operation_log(self, table_name, drop=False):
        logger.info('Running %s...', inspect.stack()[0][3])
        self.create_operations_table(table_name, 'replace' if drop else 'append')

    def log_batchjob(self, batchjob_service, file_name, comment=''):
        logger.info('Running %s...', inspect.stack()[0][3])
        client_id = batchjob_service.client.client_customer_id
        batchjob_id = batchjob_service.batch_job.result['value'][0].id
        batchjob_upload_url = batchjob_service.batch_job.result['value'][0].uploadUrl.url
        batchjob_status = batchjob_service.batch_job.result['value'][0].status
        data = {'creation_time': datetime.datetime.now().isoformat(),
                'client_id': client_id,
                'batchjob_id': batchjob_id,
                'upload_url': batchjob_upload_url,
                'result_url': '',
                'metadata': comment,
                'status': batchjob_status}
        self._write_entry(file_name, data)

    def _update_jobs_status(self, jobs):
        logger.info('Running %s...', inspect.stack()[0][3])
        while jobs['dirty']:
            client_id, job_list = jobs['dirty'].popitem()
            for dirty_job in job_list:
                pending_job = jobs['pending'][client_id][dirty_job['id']]
                if dirty_job['status'] != pending_job['status']:
                    formatted_dirty_job = {
                        'status': dirty_job['status'],
                        'result_url': dirty_job['downloadUrl'].url if 'downloadUrl' in dirty_job else '',
                        'client_id': client_id,
                        'batchjob_id': dirty_job['id'],
                    }
                    progress = {}
                    if 'progressStats' in dirty_job:
                        progress = {
                            'estimated_percent_executed': dirty_job['progressStats']['estimatedPercentExecuted'],
                            'num_operations_executed': dirty_job['progressStats']['numOperationsExecuted'],
                            'num_operations_succeeded': dirty_job['progressStats']['numOperationsSucceeded'],
                            'num_results_written': dirty_job['progressStats']['numResultsWritten'],
                        }
                    formatted_dirty_job.update(progress)

                    # remove job from pending dict if it is done or cancelled and add it to done dict
                    if dirty_job['status'] == 'DONE' or dirty_job['status'] == 'CANCELED':
                        del jobs['pending'][client_id][dirty_job['id']]
                        if not jobs['pending'][client_id]:
                            del jobs['pending'][client_id]
                        jobs['done'].setdefault(client_id, {})[dirty_job['id']] = formatted_dirty_job

    def _update_jobs(self, bjs, jobs):
        logger.info('Running %s...', inspect.stack()[0][3])
        if len(jobs['pending']) > 0:
            jobs['dirty'] = bjs.get_multiple_status(jobs['pending'])
            self._update_jobs_status(jobs)

    def _collect_jobs(self, operations_folder):
        batchjobs = {}
        for operation in self._read_from_folder(operations_folder, name_filter=lambda x: x.endswith('.result')):
            client_id = operation['client_id']
            if operation['status'] != 'DONE' and operation['status'] != 'CANCELED':
                batchjobs.setdefault(client_id, {})[operation['batchjob_id']] = operation
        return {'pending': batchjobs, 'dirty': {}, 'done': {}}

    def wait_jobs(self, operations_folder=''):
        logger.info('Running %s...', inspect.stack()[0][3])
        jobs = self._collect_jobs(operations_folder)
        sleep_time = 15
        bjs = None
        while len(jobs['pending']) > 0:
            if not bjs:
                bjs = self.service('BatchJobService')
            logger.info('Waiting for batch jobs to finish...')
            time.sleep(sleep_time)
            sleep_time *= 2
            self._update_jobs(bjs, jobs)
        self._write_entry(path.join(operations_folder, 'jobs.result'), jobs)
        self.flush_files()
        return jobs

    def get_min_value(self, table_name, *args):
        min_value = 0
        with self.engine.begin() as conn:
            for column in args:
                try:
                    query = 'select min({column}) from {table_name};'.format(column=column, table_name=table_name)
                    row = conn.execute(query).fetchone()
                    if row[0] is not None and min_value > row[0]:
                        min_value = row[0]
                except OperationalError:
                    pass
        return min_value

    def load_table(self, table_name):
        query = 'select * from {}'.format(table_name)
        data = pd.read_sql_query(query, self.engine)
        return data

    def flatten_table(self, from_table, to_table):
        data = list(d for _, d in self.iter_operations_table(from_table))
        df = pd.DataFrame(data) if data else pd.DataFrame(columns=['id'])
        df.to_sql(to_table, self.engine, index=False, if_exists='replace')

    def iter_operations_table(self, table_name):
        for operation in sqlite.itertable(self.engine, table_name):
            yield operation.client_id, json.loads(operation.operation)

    def create_operations_table(self, table_name, if_exists='replace'):
        logger.info('Running %s...', inspect.stack()[0][3])
        if if_exists == 'replace':
            with self.engine.begin() as conn:
                conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        query = 'create table if not exists {} (' \
                'id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                'client_id INTEGER, ' \
                'operation text)'.format(table_name)
        with self.engine.begin() as conn:
            conn.execute(query)
        sqlite.create_index(self.engine, table_name, 'id', 'client_id')
        self.table_models[table_name] = sqlite.get_model_from_table(table_name, self.engine)

    def clear(self, table_name):
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        self.table_models.pop(table_name, None)
        self.min_id = 0

    def split(self):
        operations_folder = str(uuid.uuid1())
        for entry in self._read_buffer():
            self._write_entry(path.join(operations_folder, '{}.data'.format(entry['campaign_id'])), entry)
        for file in self._open_files.values():
            file.close()
        self._open_files = {}
        return operations_folder

    def dump_table(self, df, table_name, table_mappings=None, if_exists='replace', **kwargs):
        logger.info('Dumping dataframe data to table...')
        renamed_df = df
        if table_mappings:
            renamed_df = df.rename(columns={value: key for key, value in table_mappings.items()}, copy=False)
        self.create_operations_table(table_name, if_exists=if_exists)
        data = iter(self._get_min_id(table_name, entry)
                    for entry in renamed_df.to_dict(orient='records'))
        sqlite.bulk_insert(self.engine, table_name, data)

    def count_table(self, table_name):
        with self.engine.begin() as conn:
            try:
                query = 'select count(*) from {table_name};'.format(table_name=table_name)
                for row in conn.execute(query):
                    count = row[0]
            except OperationalError:
                count = 0
        return count

    def _execute_operations(self, file_name):
        bjs = self.service('BatchJobService')
        operation_builder = OperationsBuilder(self.min_id)
        previous_client_id = None
        batch_size = 5000
        run_final_upload = False
        for internal_operation in self._read_entries(file_name):
            run_final_upload = True
            client_id = internal_operation['client_id']
            if client_id != previous_client_id:
                if previous_client_id is not None:
                    bjs.helper.upload_operations(is_last=True)
                previous_client_id = client_id
                bjs.prepare_job(int(client_id))
                self.log_batchjob(bjs, file_name + '.result')
                in_batch = 0
            for operation in operation_builder(internal_operation):
                if operation:
                    if in_batch > 0 and in_batch % batch_size == 0:
                        bjs.helper.upload_operations()
                    bjs.helper.add_operation(operation)
                    in_batch += 1
        if run_final_upload:
            bjs.helper.upload_operations(is_last=True)
        self.flush_files()


    # TODO: this method should instantiate a new class (maybe SyncOperation) and transform the internal functions
    # into instance methods. Also, separate the treatment for each "object_type" into a new method as well.
    def execute_operations(self, operations_folder):
        """
        Possible columns in the table:

        :param table_name:
        :param batchlog_table:
        :return:
        """
        logger.info('Running %s...', inspect.stack()[0][3])
        _, files = self.storage.listdir(operations_folder)
        files = [[path.join(operations_folder, file)] for file in files]
        self._client = None
        self._engine = None
        self._operations_buffer = None
        self.map_function(self._execute_operations, files)

    def create_labels(self, table_name):
        logger.info('Running %s...', inspect.stack()[0][3])
        query = 'select * from {}'.format(table_name)

        n_entries = self.count_table(table_name)
        if n_entries == 0:
            return

        df = pd.read_sql_query(query, self.engine)
        # Specific stuff ahead

        sjs = SyncJobService(self)
        # Build operations
        for client_id, lines in df.groupby('client_id'):
            # Apply operations
            operations_list = []
            for label_op in lines.itertuples():
                operations_list.extend(
                    operations_.add_label_operation(label_op.label)
                )

            sjs.mutate(client_id, operations_list, 'LabelService')

    def get_accounts(self, client_id=None):
        mcs = ManagedCustomerService(self.client)
        return {k[1]: v[1] for k, v in mcs.get_customers(client_id)}
