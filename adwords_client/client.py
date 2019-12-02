import csv
import datetime
import inspect
import json
import logging
import time
import uuid
import yaml
from collections import Mapping
from threading import local
from io import StringIO
from math import floor, isfinite
from multiprocessing import Pool
from os import path
from tempfile import NamedTemporaryFile
from concurrent.futures import ThreadPoolExecutor

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


def adwords_client_factory(credentials):
    config = {'adwords': credentials}
    config_yaml = yaml.safe_dump(config)
    return googleads.adwords.AdWordsClient.LoadFromString(config_yaml)


def multiprocessing_map(*args, **kwargs):
    return Pool().map(*args, **kwargs)


class AdWords:
    def __init__(self, workdir=None, storage=None, map_function=None, **kwargs):
        self.map_function = map_function or multiprocessing_map
        if storage:
            self.storage = storage
        else:
            self.storage = storages.FilesystemStorage(workdir) if workdir else storages.TemporaryFilesystemStorage()
        self.extra_options = kwargs
        self.min_id = 0
        self._reset()

    def _reset(self):
        self._local = None
        self._operations_buffer = None
        # If this is a django storage, we want to reset the storage and lazy object before fork
        if hasattr(self.storage, '_wrapped'):
            # resets the wrapped object in the LazyObject
            # For some strange reason, there is an empty object() that is used to mark the emptyness of the storage
            # https://github.com/django/django/blob/4c599ece57fa009cf3615f09497f81bfa6a585a7/django/utils/functional.py#L231
            self.storage.__init__()

    @property
    def local(self):
        if not self._local:
            self._local = local()
        return self._local

    @property
    def client(self):
        if not getattr(self.local, 'client', None):
            config.configure()
            client_settings = config.FIELDS.copy()
            client_settings.update(self.extra_options)
            self.local.client = adwords_client_factory(client_settings)
        return self.local.client

    @property
    def operations(self):
        if not self._operations_buffer:
            self._operations_buffer = NamedTemporaryFile('w+')
            logger.debug('Created temporary buffer file %s', self._operations_buffer.name)
        return self._operations_buffer

    @property
    def services(self):
        if not getattr(self.local, 'services', None):
            self.local.services = {}
        return self.local.services

    @property
    def open_files(self):
        if not getattr(self.local, 'open_files', None):
            self.local.open_files = {}
        return self.local.open_files

    def service(self, service_name):
        if service_name not in self.services:
            self.services[service_name] = getattr(adwords_api, service_name)(self.client)
        return self.services[service_name]

    def get_file(self, name, *args, **kwargs):
        if name not in self.open_files:
            self.open_files[name] = self.storage.open(name, *args, **kwargs)
        return self.open_files[name]

    def flush_files(self):
        while self.open_files:
            _, file = self.open_files.popitem()
            file.flush()
            file.close()

    def _write_entry(self, file_name, entry):
        self.get_file(file_name, mode='w+').write(json.dumps(entry) + '\n')

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
        save_in_disk = kwargs.pop('save_in_disk', False)
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
            return report_stream, fields
        else:
            raw_report = utils.gunzip(report_stream)
            converter = {
                field: MAPPERS.get(report_csv[field]['Type']).from_adwords_func
                for field in fields if report_csv[field]['Type'] in MAPPERS
            }
            if save_in_disk:
                report = utils.save_report_in_disk(raw_report, fields, converter=converter)
            else:
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
        self._write_entry(path.join(operations_folder, 'jobs.status'), jobs)
        self.flush_files()
        return jobs

    def split(self, operations_folder=''):
        operations_folder = operations_folder or str(uuid.uuid1())
        for entry in self._read_buffer():
            self._write_entry(path.join(operations_folder, '{}.data'.format(entry['campaign_id'])), entry)

        def _close_file(file_handler):
            file_handler.flush()
            file_handler.close()

        with ThreadPoolExecutor() as executor:
            executor.map(_close_file, self.open_files.values())

        self.open_files.clear()
        return operations_folder

    def _batch_operations(self, file_name):
        logger.info('Processing operation file %s', file_name)
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
            'campaign_language': 'CampaignCriterionService',
            'user_list': 'AdwordsUserListService',
            'user_list_member': 'AdwordsUserListService',
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

        if internal_operation['object_type'] == 'offline_conversion':
            if internal_operation.get('operator', None) in ['SET', 'REMOVE']:
                return 'OfflineConversionAdjustmentFeedService'
            else:
                return 'OfflineConversionFeedService'

        try:
            return object_type_service_mapper.get(internal_operation['object_type'])
        except KeyError:
            logger.debug('There is no custom service class for this object_type: %s', str(internal_operation['object_type']))
            raise

    def _sync_operations(self):
        operation_builder = OperationsBuilder()
        results = []
        errors = []
        number_of_operations = 0
        max_operations = 1000
        service = None
        client_id = None
        for internal_operation in self._read_buffer():
            client_id = internal_operation['client_id']
            service_name = self._get_service_from_object_type(internal_operation)
            service = self.service(service_name)
            if not number_of_operations:
                service.prepare_mutate(sync=True)
            for adwords_operation in operation_builder(internal_operation, sync=True):
                if adwords_operation:
                    number_of_operations += 1
                    service.helper.add_operation(adwords_operation)
                    if number_of_operations > 0 and number_of_operations % max_operations == 0:
                        partial_results = service.mutate(client_customer_id=client_id, sync=True)
                        get_errors = getattr(partial_results, "get_errors", None)
                        partial_errors = get_errors() if callable(get_errors) else []
                        results.extend(partial_results)
                        errors.extend(partial_errors)
                        service.prepare_mutate(sync=True)
        if service:
            if service.helper.operations:
                partial_results = service.mutate(client_customer_id=client_id, sync=True)
                get_errors = getattr(partial_results, "get_errors", None)
                partial_errors = get_errors() if callable(get_errors) else []
                results.extend(partial_results)
                errors.extend(partial_errors)
        self._operations_buffer = None
        return results, errors

    # TODO: this method should instantiate a new class (maybe SyncOperation) and transform the internal functions
    # into instance methods. Also, separate the treatment for each "object_type" into a new method as well.
    def execute_operations(self, operations_folder=None, sync=False, force_all=False):
        if sync:
            return self._sync_operations()
        else:
            if not operations_folder:
                raise ValueError('Async operations must have an operation folder defined.')
            logger.info('Running %s...', inspect.stack()[0][3])
            _, folder_files = self.storage.listdir(operations_folder)
            files = {}
            for file_path in folder_files:
                data_file = None
                result_file = None
                if file_path.endswith('.data'):
                    data_file = file_path
                elif file_path.endswith('.result'):
                    data_file, _, _ = file_path.rpartition('.')
                    result_file = file_path
                if data_file:
                    # if entry has been set before (if we saw .result first)
                    # avoid overwriting the result file value
                    files[data_file] = files.get(data_file) or result_file
            selected_files = [path.join(operations_folder, f) for f, data in files.items() if not data or force_all]
            self._reset()
            logger.info('Applyting map function to operation files...')
            return list(self.map_function(self._batch_operations, selected_files))

    def get_accounts(self, client_id=None):
        logger.info('Getting accounts for client_id %s...', client_id or self.client.client_customer_id)
        operation_builder = OperationsBuilder()
        internal_operation = {
            'object_type': 'managed_customer',
            'fields': ['Name', 'CustomerId']
        }
        mcs = self.service('ManagedCustomerService')
        result_paginator = mcs.get(operation_builder(internal_operation, sync=True), client_id)
        result = {}
        while result_paginator.more_pages():
            page = result_paginator.get_next_page()
            if 'entries' in page:
                for account in page['entries']:
                    result.setdefault(account['customerId'], {})['entry'] = account
            if 'links' in page:
                for account in page['links']:
                    result.setdefault(account['clientCustomerId'], {}).setdefault('link', []).append(account)
        return result

    def get_batchjobs(self, client_id=None):
        logger.info('Getting batchjobs for account %s...', client_id or self.client.client_customer_id)
        operation_builder = OperationsBuilder()
        internal_operation = {
            'object_type': 'batch_job',
            'fields': ['Id', 'Status', 'ProgressStats']
        }
        bjs = self.service('BatchJobService')
        yield from bjs.get(operation_builder(internal_operation, sync=True), client_id)

    def cancel_batchjobs(self, jobs, client_id=None):
        logger.info('Canceling batchjobs for account %s...', client_id or self.client.client_customer_id)
        bjs = self.service('BatchJobService')
        return bjs.cancel_jobs(jobs, client_id)

    def get_entities(self, get_internal_operation):
        operation_builder = OperationsBuilder()
        service_name = self._get_service_from_object_type(get_internal_operation)
        service = self.service(service_name)
        client_id = get_internal_operation.get('client_id', None)
        results = service.get(operation_builder(get_internal_operation, sync=True), client_id)
        return list(results)
