import logging
import pandas as pd
from datetime import datetime
from multiprocessing import Pool, cpu_count, RLock

from .client import AdWords
from .sqlite import remove_temporary_files

logger = logging.getLogger(__name__)


def timestamp_client_table(client, table_name, timestamp):
    df = pd.read_sql_table(table_name, client.engine)
    df['tmstmp'] = timestamp
    df.to_sql(table_name, client.engine, if_exists='replace', index=False)


def adwords_worker(timestamp,
                   operation_function,
                   mapper,
                   internal_table,
                   proc_id,
                   total_procs,
                   *args, **kwargs):
    config_file_path = kwargs.pop('config_file', None)
    adwords = AdWords.autoload(config_file_path)
    try:
        mapper.set_lock(LOCK)
        if kwargs.pop('map_data', True):
            mapper.map_data(adwords, internal_table, proc_id, total_procs)
        # remove operations log table argument before function call
        operations_log_table = kwargs.pop('operations_log_table', None)
        drop_batchlog_table = kwargs.pop('drop_batchlog_table', False)
        drop_operations_log_table = kwargs.pop('drop_operations_log_table', False)
        # keep this argument since batch operations write to this table locally
        batchlog_table = kwargs.get('batchlog_table', None)
        operation_function(adwords, internal_table, *args, **kwargs)
        if batchlog_table:
            timestamp_client_table(adwords, batchlog_table, timestamp)
            mapper.upsync(adwords, batchlog_table, batchlog_table, drop_table=drop_batchlog_table)
        if operations_log_table:
            timestamp_client_table(adwords, internal_table, timestamp)
            mapper.upsync(adwords, internal_table, operations_log_table, drop_table=drop_operations_log_table)
        mapper.set_lock(None)
    except Exception as e:
        logger.exception(e)
        raise e
    finally:
        remove_temporary_files()


def kwargs_worker(args, kwargs):
    adwords_worker(*args, **kwargs)


def init_lock(parent_lock):
    global LOCK
    LOCK = parent_lock


class ClientOperation:
    def __init__(self, mapper, config_file=None):
        self.mapper = mapper
        self.config_file = config_file

    def run(self, operation_function, internal_table, *args, **kwargs):
        child_args = []
        # only use multiprocessing if working with batch jobs
        batchlog_table = kwargs.get('batchlog_table', False)
        n_procs = kwargs.pop('n_procs', cpu_count() + 1) if batchlog_table else 1
        timestamp = datetime.now().isoformat()
        if self.config_file:
            kwargs['config_file'] = self.config_file
        for i in range(0, n_procs):
            worker_args = [
                              timestamp,
                              operation_function,
                              self.mapper,
                              internal_table,
                              i,
                              n_procs
                          ] + list(args)
            child_args.append([worker_args, kwargs])

        lock = RLock()
        with Pool(n_procs, initializer=init_lock, initargs=(lock, )) as p:
            p.starmap(kwargs_worker, child_args, 1)
