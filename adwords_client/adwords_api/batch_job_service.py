import logging
from collections import OrderedDict

import googleads

from . import common as cm
from adwords_client.adwords_api.operations.utils import batch_job_operation

logger = logging.getLogger(__name__)


class BatchJobHelper(googleads.adwords.BatchJobHelper):

    def __init__(self, service):
        request_builder = self.GetRequestBuilder(client=service.client)
        response_parser = self.GetResponseParser()
        super().__init__(request_builder=request_builder, response_parser=response_parser)
        self.operations = OrderedDict()     # Should honor the operation type insertion order
        self.last_operation = None
        self.upload_helper = self.GetIncrementalUploadHelper(service.batch_job.result['value'][0].uploadUrl.url)
        self._last_temporary_id = 0

    def __getitem__(self, op_type, item):
        return self.operations[op_type][item]

    def add_operation(self, operation):
        if operation['xsi_type'] in self.operations:
            self.operations[operation['xsi_type']].append(operation)
        else:
            self.operations[operation['xsi_type']] = [operation]
        self.last_operation = operation

    def get_temporary_id(self):
        """
        Get next Temporary ID for adwords objects on this BatchJobService operations

        Dependent operations are applied in the order they appear in your upload.
        Therefore, when using temporary IDs, make sure the operation that creates a parent object
        comes before the operations that create its child objects.
        See: https://developers.google.com/adwords/api/docs/guides/batch-jobs?#using_temporary_ids
        """
        # TODO: Protect this with locks?
        self._last_temporary_id -= 1
        return self._last_temporary_id

    def upload_operations(self, is_last=False):
        fail_counter = 0
        done = False
        while not done:
            try:
                if is_last:
                    logger.info('Uploading final data...')
                else:
                    logger.info('Uploading intermediate data...')
                self.upload_helper.UploadOperations(list(self.operations.values()), is_last=is_last)
                self.operations = {}
                self.last_operation = None
                done = True
            except Exception as e:
                fail_counter += 1
                if fail_counter > 2:
                    logger.error('Problem uploading the data, failure...')
                    raise e
                logger.error('Problem uploading the data, retrying...')


class BatchJobOperations:
    def __init__(self, service):
        self.operations = []

    def __getitem__(self, item):
        return self.operations[item]

    def add_batch_job_operation(self, *args, **kwargs):
        self.operations.append(batch_job_operation(*args, **kwargs))


class BatchJobService(cm.BaseService):
    def __init__(self, client):
        super().__init__(client, 'BatchJobService')
        self.batch_job = None
        self.helper = None

    def get_wholeoperation_id(self):
        try:
            return self.batch_job.result['value'][0].id
        except IndexError:
            raise RuntimeError('No operations queued. No "whole operation" id created')

    def prepare_mutate(self, sync=None):
        self.helper = BatchJobOperations(self.service)
        self.ResultProcessor = cm.SimpleReturnValue

    def prepare_job(self, client_customer_id=None):
        self.prepare_mutate()
        self.helper.add_batch_job_operation('ADD')
        self.batch_job = self.mutate(client_customer_id)
        logger.info('Created new batchjob:\n%s', self.batch_job)
        self.helper = BatchJobHelper(self)

    def cancel_jobs(self, jobs):
        result = {}
        for client_id in jobs:
            self.prepare_mutate()
            for job in jobs[client_id]:
                if job['status'] not in ['DONE', 'CANCELING', 'CANCELED']:
                    self.helper.add_batch_job_operation('SET', job['id'], 'CANCELING')
            result[client_id] = self.mutate(client_id) if len(self.helper.operations) > 0 else None
        return result

        self.helper = BatchJobOperations(self.service)
        self.helper.add_batch_job_operation('SET')
        self.ResultProcessor = cm.SimpleReturnValue
        return

    def get_status(self, batch_job_id, client_customer_id=None):
        self.prepare_get()
        self.helper.add_fields('DownloadUrl', 'Id', 'ProcessingErrors', 'ProgressStats', 'Status')
        self.helper.add_predicate('Id', 'EQUALS', [batch_job_id])
        return next(iter(self.get(client_customer_id)))

    def get_multiple_status(self, jobs):
        result = {}
        for client_id in jobs:
            self.prepare_get()
            self.helper.add_fields('DownloadUrl', 'Id', 'ProcessingErrors', 'ProgressStats', 'Status')
            self.helper.add_predicate('Id', 'IN', [job for job in jobs[client_id]])
            result[client_id] = list(self.get(client_id))
        logger.info('BatchJob Statuses:\n%s', result)
        return result
