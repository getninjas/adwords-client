import csv
import datetime
import inspect
import io
import logging
import time
from io import StringIO

import googleads.adwords
import pandas as pd
from sqlalchemy.sql import text

from . import utils
from . import adwordsapi
from . import sqlite as sqlutils
from .adwordsapi import common
from .adwordsapi import operations
from .adwordsapi.sync_job_service import SyncJobService

logger = logging.getLogger(__name__)


class AdWords:
    def __init__(self, config_file):
        self.client = googleads.adwords.AdWordsClient.LoadFromStorage(config_file)
        self.services = {}
        self.engine = sqlutils.get_connection()

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
        report_csv = dict((item['Name'], item) for item in csv.DictReader(StringIO(report_csv.content.decode('utf-8'))))
        to_remove = set([name for name, item in report_csv.items() if
                         (item['Behavior'] in exclude_behavior  # remove based on variable behavior
                          or any(term in name for term in exclude_terms)  # remove for some terms
                          or (only_fields is not None and name not in only_fields))  # only given fields
                         ])
        to_remove = to_remove.union(exclude_fields)
        fields = [field for field in report_csv if field not in to_remove]
        fields += include_fields
        fields.sort()
        casts = {'Money': 'REAL',
                 'Long': 'BIGINT',
                 'Double': 'REAL',
                 'Integer': 'BIGINT',
                 'Bid': 'REAL'}

        rd = self.service('ReportDownloader')
        args = [report_type, fields, customer_id] + list(args)

        def process_entry(field):
            try:
                if report_csv[field]['Type'] in casts:
                    if casts[report_csv[field]['Type']] == 'INTEGER':
                        return utils.process_integer
                    else:
                        return utils.process_double
            except:
                pass
            return None

        if not simple_download:
            report_id = kwargs.pop('report_id', None)
            reference_date = kwargs.pop('reference_date', None)
            kwargs['return_stream'] = True
            report_stream = rd.report(*args, **kwargs)
            converter = dict((field, process_entry(field)) for field in fields if process_entry(field))
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',
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
                                'DisplayUrl']
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',  # The descriptive name of the Customer account.
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',  # The descriptive name of the Customer account.
                                'ExternalCustomerId',  # The Customer ID.
                                'BudgetId',
                                'BudgetName',
                                'BudgetReferenceCount',  # The number of campaigns actively using the budget.
                                'Amount',  # The daily budget
                                'IsBudgetExplicitlyShared',  # Shared budget (true) or specific to the campaign (false)
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = ['AccountDescriptiveName',
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
        if kwargs.pop('fields', False):
            kwargs['fields'] = [''] + kwargs['fields']
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
        if drop:
            with self.engine:
                self.engine.execute('DROP TABLE IF EXISTS {}'.format(table_name))
        query = """
        create table if not exists {} (
        creation_time text,
        client_id int,
        batchjob_id int,
        upload_url text,
        result_url text,
        metadata text,
        status text,
        PRIMARY KEY (creation_time, client_id, batchjob_id))
        """.format(table_name)
        with self.engine.begin() as conn:
            conn.execute(query)
        sqlutils.create_index(self.engine, table_name, 'creation_time', 'client_id')
        sqlutils.create_index(self.engine, table_name, 'client_id')
        sqlutils.create_index(self.engine, table_name, 'batchjob_id')
        sqlutils.create_index(self.engine, table_name, 'creation_time')

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
        sqlutils.execute(self.engine, table_name, 'insert', data)

    def set_batchjob_status(self, jobs_table, old_jobs, jobs):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        query = text("""
        UPDATE {}
        SET
          status = :status,
          result_url = :result_url
        WHERE
          client_id = :client_id
          AND batchjob_id = :batchjob_id
        """.format(jobs_table))
        with self.engine.begin() as conn:
            for client_id in jobs:
                data = [{'status': job['status'],
                         'result_url': job['downloadUrl'].url if 'downloadUrl' in job else '',
                         'client_id': client_id,
                         'batchjob_id': job['id']}
                        for job in jobs[client_id]
                        if job['status'] != old_jobs[client_id][job['id']][0]['status']]
                if data:
                    conn.execute(query, *data)

    def update_log_tables(self, bjs, batch_table):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        query = """
            select
                client_id,
                batchjob_id as id,
                status
            from {}
            where status <> 'DONE' and status <> 'CANCELED'
            """.format(batch_table)
        accounts = sqlutils.dict_query(self.engine, query, 1, 1)
        if len(accounts) > 0:
            result = bjs.get_multiple_status(accounts)
            self.set_batchjob_status(batch_table, accounts, result)
            accounts = sqlutils.dict_query(self.engine, query, 1, 1)
        return accounts

    def exponential_backoff(self, batchlog_table):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        sleep_time = 15
        logger.info('Getting operations data...')
        bjs = self.service('BatchJobService')

        accounts = [True]  # forcing first loop, wait 15s by default
        while len(accounts) > 0:
            logger.info('Waiting for batch jobs to finish...')
            time.sleep(sleep_time)
            sleep_time *= 2
            accounts = self.update_log_tables(bjs, batchlog_table)

    def load_table(self, table_name):
        logger.info('Getting operations data...')
        query = 'select * from {}'.format(table_name)
        data = pd.read_sql_query(query, self.engine)
        return data

    def _setup_operations(self, table_name, batchlog_table):
        self.create_batch_operation_log(batchlog_table)
        bjs = self.service('BatchJobService')
        accounts = self.load_table(table_name)
        return bjs, accounts

    def _execute_operations(self, bjs, operations, batchlog_table, operation_builder):
        for client_id, account_operations in operations.groupby('client_id'):
            bjs.prepare_job(int(client_id))
            self.log_batchjob(batchlog_table, bjs)
            batch_size = 5000
            in_batch = 0
            for internal_operation in account_operations.itertuples():
                for operation in operation_builder(internal_operation):
                    if operation:
                        if in_batch > 0 and in_batch % batch_size == 0:
                            bjs.helper.upload_operations()
                        bjs.helper.add_operation(operation)
                        in_batch += 1

            bjs.helper.upload_operations(is_last=True)

    def modify_bids(self, table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(table_name, batchlog_table)

        def build_bid_change_operation(internal_operation):
            if utils.cents_as_money(internal_operation.new_bid) != utils.cents_as_money(internal_operation.old_bid):
                # TODO: check if this operation is associated with an adgroup and not a keyword, should not exist
                if internal_operation.keyword_id > -1:
                    yield operations.add_keyword_cpc_bid_adjustment_operation(
                        int(internal_operation.adgroup_id),
                        int(internal_operation.keyword_id),
                        utils.cents_as_money(internal_operation.new_bid)
                    )
                else:
                    yield operations.add_adgroup_cpc_bid_adjustment_operation(
                        int(internal_operation.campaign_id),
                        int(internal_operation.adgroup_id),
                        utils.cents_as_money(
                            internal_operation.new_bid)
                    )
            else:
                yield None

        self._execute_operations(bjs, accounts, batchlog_table, build_bid_change_operation)

    def modify_keywords_text(self, table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(table_name, batchlog_table)

        def build_new_keyword_operation(internal_operation):
            # check if this operation is associated with an adgroup and not a keyword
            if internal_operation.keyword_id > -1:
                yield operations.add_biddable_adgroup_criterion_operation(
                    int(internal_operation.adgroup_id),
                    'SET',
                    'Keyword',
                    int(internal_operation.keyword_id),
                    userStatus='PAUSED'
                )
                yield operations.add_new_keyword_operation(
                    int(internal_operation.adgroup_id),
                    internal_operation.new_text,
                    internal_operation.keyword_match_type.upper(),
                    internal_operation.status.upper(),
                    int(internal_operation.cpc_bid)
                )
            else:
                yield None

        self._execute_operations(bjs, accounts, batchlog_table, build_new_keyword_operation)

    def modify_budgets(self, operations_table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(operations_table_name, batchlog_table)

        def build_budget_operation(internal_operation):
            yield from operations.add_adgroup_label_operation(
                adgroup_id='%.0f' % internal_operation.adgroup_id,
                label_id='%.0f' % internal_operation.label_id
            )

        self._execute_operations(bjs, accounts, batchlog_table, build_budget_operation)

    def add_adgroups_labels(self, operations_table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(operations_table_name, batchlog_table)

        def build_adgroup_label_operation(internal_operation):
            yield from operations.apply_new_budget(
                campaign_id='%.0f' % internal_operation.campaign_id,  # 3.14 -> '3'
                amount=internal_operation.amount,
                id_builder=bjs.get_temporary_id
            )

        self._execute_operations(bjs, accounts, batchlog_table, build_adgroup_label_operation)

    def modify_adgroups_names(self, operations_table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        bjs, accounts = self._setup_operations(operations_table_name, batchlog_table)

        def build_adgroup_name_operation(internal_operation):
            yield from operations.set_adgroup_name_operation(
                adgroup_id='%.0f' % internal_operation.adgroup_id,
                name=internal_operation.name
            )

        self._execute_operations(bjs, accounts, batchlog_table, build_adgroup_name_operation)

    def modify_keywords_status(self, table_name, batchlog_table='batchlog_table'):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))

        def build_status_operation(internal_operation):
            if internal_operation.old_status != internal_operation.new_status:
                yield operations.add_biddable_adgroup_criterion_operation(
                    int(internal_operation.adgroup_id),
                    'SET',
                    'Keyword',
                    int(internal_operation.keyword_id),
                    userStatus=internal_operation.new_status
                )
            else:
                yield None

        bjs, accounts = self._setup_operations(table_name, batchlog_table)
        self._execute_operations(bjs, accounts, batchlog_table, build_status_operation)

    def create_labels(self, table_name):
        logger.info('Running {}...'.format(inspect.stack()[0][3]))
        query = 'select * from {}'.format(table_name)
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
