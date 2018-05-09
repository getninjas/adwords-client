import csv
import datetime
import inspect
import json
import logging
import time
import uuid
import yaml
from collections import Mapping
from io import StringIO
from math import floor, isfinite
from multiprocessing import Pool
from os import path
from tempfile import NamedTemporaryFile

import googleads.adwords

from . import adwords_api, config, storages, utils
from .adwords_api import common
from .internal_api.builder import OperationsBuilder
from .internal_api.mappers import MAPPERS

logger = logging.getLogger(__name__)


def _iter_floats(data):
    for item in data:
        try:
            if item and isfinite(item):
                yield float(item)
        except (TypeError, ValueError):
            pass


def _get_dict_min_value(data):
    try:
        return min(int(floor(u)) for u in _iter_floats(data.values()))
    except ValueError:
        logger.debug('Problem getting min value for: %s', str(data))
        for k, v in data.items():
            logger.debug('Key: %s (%s) Value: %s (%s)', str(k), str(type(k)), str(v), str(type(v)))
        raise


def multiprocessing_starmap(*args, **kwargs):
    return Pool().starmap(*args, **kwargs)


def adwords_client_factory(credentials):
    config = {'adwords': credentials}
    config_yaml = yaml.safe_dump(config)
    return googleads.adwords.AdWordsClient.LoadFromString(config_yaml)


class AdWords:
    def __init__(self, workdir=None, storage=None, map_function=None):
        self._client = None
        self.services = {}
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
            config.configure()
            self._client = adwords_client_factory(config.FIELDS)
        return self._client

    @property
    def operations(self):
        if not self._operations_buffer:
            self._operations_buffer = NamedTemporaryFile('w+')
            logger.debug('Created temporary buffer file %s', self._operations_buffer.name)
        return self._operations_buffer

    def service(self, service_name):
        if service_name not in self.services:
            self.services[service_name] = getattr(adwords_api, service_name)(self.client)
        return self.services[service_name]

    def get_file(self, name, *args, **kwargs):
        if name not in self._open_files:
            self._open_files[name] = self.storage.open(name, *args, **kwargs)
        return self._open_files[name]

    def flush_files(self):
        for file in self._open_files.values():
            file.flush()

    def _write_entry(self, file_name, entry):
        self.get_file(file_name).write(json.dumps(entry) + '\n')

    def _read_entries(self, file_name):
        file = self.get_file(file_name, mode='r')
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

    def _get_min_id(self, entry):
        self.min_id = min(self.min_id, _get_dict_min_value(entry))
        return entry

    def insert(self, data):
        if isinstance(data, Mapping):
            data = [self._get_min_id(data)]
        else:
            data = (self._get_min_id(entry) for entry in data)
        for entry in data:
            if 'client_id' not in entry:
                raise ValueError('Every entry must have a "client_id" field.')
            self._write_buffer(entry)

    def get_report(self, report_type, customer_id, exclude_fields=[],
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

        report_stream = rd.report(*args, **kwargs)

        if simple_download:
            return report_stream
        else:
            raw_report = utils.gunzip(report_stream)
            converter = {
                field: MAPPERS.get(report_csv[field]['Type']).from_adwords_func
                for field in fields if report_csv[field]['Type'] in MAPPERS
            }
            report_iterator = utils.csv_reader(raw_report, fields, converter=converter)
            report = list(report_iterator())
            return report

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
                        'result_url': dirty_job['downloadUrl']['url'] if dirty_job['downloadUrl'] else '',
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
            jobs['dirty'] = bjs.get_multiple_status(jobs['pending'])
            self._update_jobs_status(jobs)
            # only sleep if we still have pending jobs
            if len(jobs['pending']) > 0:
                logger.info('Waiting for batch jobs to finish...')
                time.sleep(sleep_time)
                sleep_time *= 2
        self._write_entry(path.join(operations_folder, 'jobs.result'), jobs)
        self.flush_files()
        return jobs

    def split(self):
        operations_folder = str(uuid.uuid1())
        for entry in self._read_buffer():
            self._write_entry(path.join(operations_folder, '{}.data'.format(entry['campaign_id'])), entry)
        while self._open_files:
            _, file = self._open_files.popitem()
            file.close()
        return operations_folder

    def _batch_operations(self, file_name):
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

    def _get_service_from_object_type(self, internal_operation):
        object_type_service_mapper = {
            'managed_customer': 'ManagedCustomerService',
            'customer': 'CustomerService',
            'shared_criterion': 'SharedCriterionService',
            'campaign_shared_set': 'CampaignSharedSetService',
            'shared_set': 'SharedSetService',
            'budget_order': 'BudgetOrderService',
            'campaign': 'CampaignService',
            'billing_account': 'BudgetOrderService',
            'label': 'LabelService',
            'campaign_ad_schedule': 'CampaignCriterionService',
            'campaign_targeted_location': 'CampaignCriterionService',
            'campaign_sitelink': 'CampaignExtensionSettingService',
            'campaign_callout': 'CampaignExtensionSettingService',
            'campaign_structured_snippet': 'CampaignExtensionSettingService',
            'account_label': 'AccountLabelService',
            'campaign_language': 'CampaignCriterionService'
        }

        if internal_operation['object_type'] == 'attach_label':
            if 'ad_id' in internal_operation:
                raise NotImplementedError()
            elif 'adgroup_id' in internal_operation:
                raise NotImplementedError()
            elif 'campaign_id' in internal_operation:
                return object_type_service_mapper.get('campaign')
            elif 'customer_id' in internal_operation:
                return object_type_service_mapper.get('managed_customer')

        try:
            return object_type_service_mapper.get(internal_operation['object_type'])
        except KeyError:
            logger.debug('There is no custom service class for this object_type: %s', str(internal_operation['object_type']))
            raise

    def _sync_operations(self):
        previous_client_id = None
        previous_service_name = None
        operation_builder = OperationsBuilder()
        results = []
        for internal_operation in self._read_buffer():
            client_id = internal_operation['client_id']
            service_name = self._get_service_from_object_type(internal_operation)
            for adwords_operation in operation_builder(internal_operation):
                service = self.service(service_name)
                if previous_client_id is not None:
                    previous_service = self.service(previous_service_name)
                    if client_id != previous_client_id or service_name != previous_service_name:
                        # time to upload
                        results.extend(previous_service.mutate(previous_client_id, sync=True))
                        # prepare for next upload
                        service.prepare_mutate(sync=True)
                else:
                    # this runs on the first loop
                    service.prepare_mutate(sync=True)

                service.helper.add_operation(adwords_operation)
                previous_client_id = client_id
                previous_service_name = service_name
        if previous_service_name and previous_client_id:
            previous_service = self.service(previous_service_name)
            results.extend(previous_service.mutate(previous_client_id, sync=True))

        self._operations_buffer = None
        return results

    # TODO: this method should instantiate a new class (maybe SyncOperation) and transform the internal functions
    # into instance methods. Also, separate the treatment for each "object_type" into a new method as well.
    def execute_operations(self, operations_folder=None, sync=False):
        """
        Possible columns in the table:

        :param table_name:
        :param batchlog_table:
        :return:
        """
        if sync:
            return self._sync_operations()
        else:
            if not operations_folder:
                raise ValueError('Async operations must have an operation folder defined.')
            logger.info('Running %s...', inspect.stack()[0][3])
            _, files = self.storage.listdir(operations_folder)
            files = [[path.join(operations_folder, file)] for file in files]
            self._client = None
            self._operations_buffer = None
            self.services = {}
            self.map_function(self._batch_operations, files)

    def get_accounts(self, client_id=None):
        mcs = self.service('ManagedCustomerService')
        return {account['name']: account for account in mcs.get_customers(client_id)}

    def get_entities(self, get_internal_operation):
        operation_builder = OperationsBuilder()
        service_name = self._get_service_from_object_type(get_internal_operation)
        service = self.service(service_name)
        client_id = get_internal_operation.get('client_id', None)
        service.prepare_get()
        results = service.sync_get(operation_builder(get_internal_operation, is_get_operation=True), client_id)
        if type(results) is list:
            return results
        return list(results)
