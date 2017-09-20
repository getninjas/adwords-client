import csv
import datetime
import inspect
import io
import json
import logging
import time
from collections import Mapping
from io import StringIO
from pandas import isnull
from math import floor

import googleads.adwords
import pandas as pd
import yaml
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text

from . import adwordsapi
from . import config
from . import sqlite as sqlutils
from . import utils
from .adwordsapi import common
from .adwordsapi import operations
from .adwordsapi.managed_customer_service import ManagedCustomerService
from .adwordsapi.sync_job_service import SyncJobService

logger = logging.getLogger(__name__)


class AdWords:
    @classmethod
    def autoload(cls, path=None):
        config.configure(path)
        return AdWords.from_args(**config.FIELDS)

    @classmethod
    def from_args(cls, **kwargs):
        config = {'adwords': kwargs}
        config_yaml = yaml.safe_dump(config)
        client = googleads.adwords.AdWordsClient.LoadFromString(config_yaml)
        return cls(client)

    @classmethod
    def from_file(cls, config_file):
        client = googleads.adwords.AdWordsClient.LoadFromStorage(config_file)
        return cls(client)

    def __init__(self, google_ads_client):
        self.client = google_ads_client
        self.services = {}
        self.engine = sqlutils.get_connection()
        self.table_models = {}
        self.table_min_id = {}

    def service(self, service_name):
        if service_name not in self.services:
            self.services[service_name] = getattr(adwordsapi, service_name)
        return self.services[service_name](self.client)

    def get_report(self, report_type, customer_id, target_name,
                   create_table=False, exclude_fields=[],
                   exclude_terms=['Significance'], exclude_behavior=['Segment'],
                   include_fields=[], *args, **kwargs):
        logger.info('Getting {}...'.format(report_type))
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
                field: utils.MAPPERS.get(report_csv[field]['Type']).from_adwords_func
                for field in fields if report_csv[field]['Type'] in utils.MAPPERS
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
        create_table = kwargs.pop('create_table', False)
        exclude_fields = ['BusinessName',
                          'ConversionTypeName',
                          'CriterionType',
                          'CriterionId',
                          'ClickType',
                          'ConversionCategoryName',
                          'ConversionTrackerId',
                          'IsNegative']
        exclude_terms = ['Significance', 'ActiveView', 'Average']
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
                    'Headline',
                    'HeadlinePart1',
                    'HeadlinePart2',
                    'Description',
                    'Description1',
                    'Description2',
                    'DisplayUrl',
                ]
        return self.get_report('AD_PERFORMANCE_REPORT',
                               customer_id,
                               target_name,
                               create_table,
                               exclude_fields,
                               exclude_terms,
                               ['Segment'],
                               [],
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
        sqlutils.create_index(self.conn,
                              target_name,
                              'CampaignId')
        sqlutils.create_index(self.conn,
                              target_name,
                              'CampaignName')
        sqlutils.create_index(self.conn,
                              target_name,
                              'CustomerDescriptiveName',
                              'CampaignName')

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
        sqlutils.create_index(self.conn,
                              target_name,
                              'AdGroupId')
        sqlutils.create_index(self.conn,
                              target_name,
                              'AdGroupName')
        sqlutils.create_index(self.conn,
                              target_name,
                              'CustomerDescriptiveName',
                              'AdGroupName')

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
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        self.create_operations_table(table_name, 'replace' if drop else 'append')

    def log_batchjob(self, table_name, batchjob_service, comment=''):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        client_id = batchjob_service.client.client_customer_id
        batchjob_id = batchjob_service.batch_job[0].id
        batchjob_upload_url = batchjob_service.batch_job[0].uploadUrl.url
        batchjob_status = batchjob_service.batch_job[0].status
        data = {'creation_time': datetime.datetime.now().isoformat(),
                'client_id': client_id,
                'batchjob_id': batchjob_id,
                'upload_url': batchjob_upload_url,
                'result_url': '',
                'metadata': comment,
                'status': batchjob_status}
        self.insert(table_name, data)

    def set_batchjob_status(self, jobs_table, old_jobs, jobs):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        query = text("""
        UPDATE {}
        SET
          operation = :operation
        WHERE
          id = :row_id
        """.format(jobs_table))
        with self.engine.begin() as conn:
            for client_id in jobs:
                updates = []
                for job in jobs[client_id]:
                    old_job = old_jobs[client_id][job['id']]
                    if job['status'] != old_job['data']['status']:
                        old_job['data'].update({
                            'status': job['status'],
                            'result_url': job['downloadUrl'].url if 'downloadUrl' in job else '',
                            'client_id': client_id,
                            'batchjob_id': job['id']
                        })
                        updates.append({'row_id': old_job['id'], 'operation': json.dumps(old_job['data'])})
                if updates:
                    conn.execute(query, *updates)

    def get_batchjobs(self, batch_table):
        batchjobs = {}
        for operation in sqlutils.itertable(self.engine, batch_table):
            row_id = operation.id
            client_id = operation.client_id
            data = json.loads(operation.operation)
            if data['status'] != 'DONE' and data['status'] != 'CANCELED':
                batchjobs.setdefault(client_id, {})[data['batchjob_id']] = {'id': row_id, 'data': data}
        return batchjobs

    def update_log_tables(self, bjs, batch_table, accounts):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        if len(accounts) > 0:
            result = bjs.get_multiple_status(accounts)
            self.set_batchjob_status(batch_table, accounts, result)
            accounts = self.get_batchjobs(batch_table)
        return accounts

    def exponential_backoff(self, *args, **kwargs):
        logger.warning('DEPRECATED: use wait_jobs instead...')
        return self.wait_jobs(*args, **kwargs)

    def wait_jobs(self, jobs_table='batchlog_table', **kwargs):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        self.create_batch_operation_log(jobs_table)
        accounts = self.get_batchjobs(jobs_table)
        sleep_time = 15
        bjs = None
        while len(accounts) > 0:
            if not bjs:
                bjs = self.service('BatchJobService')
            logger.info('Waiting for batch jobs to finish...')
            time.sleep(sleep_time)
            sleep_time *= 2
            accounts = self.update_log_tables(bjs, jobs_table, accounts)

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
        for operation in sqlutils.itertable(self.engine, table_name):
            yield operation.client_id, json.loads(operation.operation)

    def create_operations_table(self, table_name, if_exists='replace'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        if if_exists == 'replace':
            with self.engine.begin() as conn:
                conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        query = 'create table if not exists {} (' \
                'id INTEGER PRIMARY KEY AUTOINCREMENT, ' \
                'client_id INTEGER, ' \
                'operation text)'.format(table_name)
        with self.engine.begin() as conn:
            conn.execute(query)
        self.table_models[table_name] = sqlutils.get_model_from_table(table_name, self.engine)

    @staticmethod
    def _get_dict_min_value(data):
        try:
            return min(
                int(floor(u)) for u in data.values()
                if isinstance(u, int)
                or (isinstance(u, float) and not isnull(u))
                or (isinstance(u, str) and u.lstrip('-').isdigit())
            )
        except ValueError:
            logger.debug('Problem getting min value for: {}'.format(str(data)))
            for k, v in data.items():
                logger.debug('Key: {} ({}) Value: {} ({})'.format(str(k), str(type(k)), str(v), str(type(v))))
            raise

    def _make_entry(self, table_name, entry):
        self.table_min_id[table_name] = min(self.table_min_id.get(table_name, 0), self._get_dict_min_value(entry))
        return {'client_id': entry['client_id'], 'operation': json.dumps(entry)}

    def insert(self, table_name, data, if_exists='append'):
        model = self.table_models.get(table_name)
        if model is None or if_exists == 'replace':
            self.create_operations_table(table_name, if_exists=if_exists)
            model = self.table_models.get(table_name)
        if isinstance(data, Mapping):
            data = [self._make_entry(table_name, data)]
        else:
            data = iter(self._make_entry(table_name, entry) for entry in data)
        sqlutils.bulk_insert(self.engine, table_name, data, model)

    def clear(self, table_name):
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        self.table_models.pop(table_name, None)
        self.table_min_id.pop(table_name, None)

    def dump_table(self, df, table_name, table_mappings=None, if_exists='replace', **kwargs):
        logger.info('Dumping dataframe data to table...')
        renamed_df = df
        if table_mappings:
            renamed_df = df.rename(columns={value: key for key, value in table_mappings.items()}, copy=False)
        self.create_operations_table(table_name, if_exists=if_exists)
        data = iter(self._make_entry(table_name, entry)
                    for entry in renamed_df.to_dict(orient='records'))
        sqlutils.bulk_insert(self.engine, table_name, data)

    def count_table(self, table_name):
        with self.engine.begin() as conn:
            try:
                query = 'select count(*) from {table_name};'.format(table_name=table_name)
                for row in conn.execute(query):
                    count = row[0]
            except OperationalError:
                count = 0
        return count

    def _setup_operations(self, table_name, batchlog_table):
        n_entries = self.count_table(table_name)
        self.create_batch_operation_log(batchlog_table)
        if n_entries == 0:
            return None, []
        bjs = self.service('BatchJobService')
        operations = self.iter_operations_table(table_name)
        return bjs, operations

    def _execute_operations(self, bjs, operations, batchlog_table, operation_builder):
        previous_client_id = None
        batch_size = 5000
        for client_id, internal_operation in operations:
            if client_id != previous_client_id:
                if previous_client_id is not None:
                    bjs.helper.upload_operations(is_last=True)
                previous_client_id = client_id
                bjs.prepare_job(int(client_id))
                self.log_batchjob(batchlog_table, bjs)
                in_batch = 0
            for operation in operation_builder(internal_operation):
                if operation:
                    if in_batch > 0 and in_batch % batch_size == 0:
                        bjs.helper.upload_operations()
                    bjs.helper.add_operation(operation)
                    in_batch += 1
        if bjs is not None:
            bjs.helper.upload_operations(is_last=True)

    def modify_bids(self, table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(table_name, batchlog_table)

        def build_bid_change_operation(internal_operation):
            old_bid = utils.MAPPERS['Bid'].to_adwords(internal_operation['old_bid'])
            new_bid = utils.MAPPERS['Bid'].to_adwords(internal_operation['new_bid'])
            if new_bid != old_bid:
                # TODO: check if this operation is associated with an adgroup and not a keyword, should not exist
                if internal_operation['keyword_id'] > -1:
                    yield operations.add_keyword_cpc_bid_adjustment_operation(
                        utils.MAPPERS['Long'].to_adwords(internal_operation['adgroup_id']),
                        utils.MAPPERS['Long'].to_adwords(internal_operation['keyword_id']),
                        new_bid,
                    )
                else:
                    yield operations.add_adgroup_cpc_bid_adjustment_operation(
                        utils.MAPPERS['Long'].to_adwords(internal_operation['campaign_id']),
                        utils.MAPPERS['Long'].to_adwords(internal_operation['adgroup_id']),
                        new_bid,
                    )
            else:
                yield None

        self._execute_operations(bjs, accounts, batchlog_table, build_bid_change_operation)

    def sync_objects(self, table_name, batchlog_table='batchlog_table'):
        """
        Possible columns in the table:

        :param table_name:
        :param batchlog_table:
        :return:
        """
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(table_name, batchlog_table)

        def get_next_id():
            nonlocal self, table_name
            self.table_min_id[table_name] -= 1
            return self.table_min_id[table_name]

        mappers = {
            'client_id': 'Long',
            'campaign_id': 'Long',
        }
        mappers.update(operations.add_new_keyword_operation.__annotations__)
        mappers.update(operations.add_adgroup.__annotations__)
        mappers.update(operations.add_ad.__annotations__)
        mappers.update(operations.add_budget.__annotations__)
        mappers.update(operations.add_campaign.__annotations__)

        def build_new_keyword_operation(internal_operation):
            object_type = internal_operation.pop('object_type')
            if object_type == 'keyword':
                internal_operation = {k: utils.MAPPERS[mappers.get(k, 'Identity')].to_adwords(v)
                                      for k, v in internal_operation.items()}
                internal_operation = {k: v for k, v in internal_operation.items() if v is not None}
                yield operations.add_new_keyword_operation(**internal_operation)
            elif object_type == 'adgroup':
                internal_operation = {k: utils.MAPPERS[mappers.get(k, 'Identity')].to_adwords(v)
                                      for k, v in internal_operation.items()}
                internal_operation = {k: v for k, v in internal_operation.items() if v is not None}
                yield operations.add_adgroup(**internal_operation)
            elif object_type == 'ad':
                internal_operation = {k: utils.MAPPERS[mappers.get(k, 'Identity')].to_adwords(v)
                                      for k, v in internal_operation.items()}
                internal_operation = {k: v for k, v in internal_operation.items() if v is not None}
                yield operations.add_ad(**internal_operation)
            elif object_type == 'campaign':
                internal_operation = {k: utils.MAPPERS[mappers.get(k, 'Identity')].to_adwords(v)
                                      for k, v in internal_operation.items()}
                internal_operation = {k: v for k, v in internal_operation.items() if v is not None}
                if internal_operation.get('operator', 'ADD').upper() == 'ADD' and 'budget_id' not in internal_operation:
                    internal_operation['budget_id'] = utils.MAPPERS['Long'].to_adwords(get_next_id())
                    yield operations.add_budget(**internal_operation)
                yield operations.add_campaign(**internal_operation)
                for language_id in internal_operation.get('languages', []):
                    language_id = utils.MAPPERS['Long'].to_adwords(language_id)
                    yield operations.add_campaign_language(language_id=language_id, **internal_operation)
                for location_id in internal_operation.get('locations', []):
                    location_id = utils.MAPPERS['Long'].to_adwords(location_id)
                    yield operations.add_campaign_location(location_id=location_id, **internal_operation)
            else:
                yield None

        self._execute_operations(bjs, accounts, batchlog_table, build_new_keyword_operation)

    def modify_keywords_text(self, table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(table_name, batchlog_table)

        def build_new_keyword_operation(internal_operation):
            # check if this operation is associated with an adgroup and not a keyword
            if internal_operation['keyword_id'] > -1:
                yield operations.add_biddable_adgroup_criterion_operation(
                    int(internal_operation['adgroup_id']),
                    'SET',
                    'Keyword',
                    int(internal_operation['keyword_id']),
                    userStatus='PAUSED'
                )
                yield operations.add_new_keyword_operation(
                    int(internal_operation['adgroup_id']),
                    internal_operation['new_text'],
                    internal_operation['keyword_match_type'].upper(),
                    internal_operation['status'].upper(),
                    int(internal_operation['cpc_bid'])
                )
            else:
                yield None

        self._execute_operations(bjs, accounts, batchlog_table, build_new_keyword_operation)

    def modify_budgets(self, operations_table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(operations_table_name, batchlog_table)

        def build_budget_operation(internal_operation):
            yield from operations.apply_new_budget(
                campaign_id='%.0f' % internal_operation['campaign_id'],  # 3.14 -> '3'
                amount=internal_operation['amount'],
                id_builder=bjs.get_temporary_id
            )

        self._execute_operations(bjs, accounts, batchlog_table, build_budget_operation)

    def add_adgroups_labels(self, operations_table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(operations_table_name, batchlog_table)

        def build_adgroup_label_operation(internal_operation):
            yield from operations.add_adgroup_label_operation(
                adgroup_id='%.0f' % internal_operation['adgroup_id'],
                label_id='%.0f' % internal_operation['label_id']
            )

        self._execute_operations(bjs, accounts, batchlog_table, build_adgroup_label_operation)

    def modify_adgroups_names(self, operations_table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(operations_table_name, batchlog_table)

        def build_adgroup_name_operation(internal_operation):
            yield from operations.set_adgroup_name_operation(
                adgroup_id='%.0f' % internal_operation['adgroup_id'],
                name=internal_operation['name']
            )

        self._execute_operations(bjs, accounts, batchlog_table, build_adgroup_name_operation)

    def modify_keywords_status(self, table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))

        def build_status_operation(internal_operation):
            if internal_operation['old_status'] != internal_operation['new_status']:
                yield operations.add_biddable_adgroup_criterion_operation(
                    int(internal_operation['adgroup_id']),
                    'SET',
                    'Keyword',
                    int(internal_operation['keyword_id']),
                    userStatus=internal_operation['new_status']
                )
            else:
                yield None

        bjs, accounts = self._setup_operations(table_name, batchlog_table)
        self._execute_operations(bjs, accounts, batchlog_table, build_status_operation)

    def create_labels(self, table_name):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
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
                    operations.add_label_operation(label_op.label)
                )

            sjs.mutate(client_id, operations_list, 'LabelService')

    def get_accounts(self, client_id=None):
        mcs = ManagedCustomerService(self.client)
        return {k[1]: v[1] for k, v in mcs.get_customers(client_id)}
