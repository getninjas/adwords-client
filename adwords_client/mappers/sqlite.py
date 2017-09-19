import logging
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


class SqliteMapper:
    def __init__(self, table_mappings, connection_factory):
        self.table_mappings = table_mappings
        self.engine = None
        self.connection_factory = connection_factory
        self.lock = None

    def set_lock(self, lock):
        self.lock = lock

    def get_engine(self):
        if not self.engine:
            self.engine = self.connection_factory()
        return self.engine

    def map_data(self, client, table_name, group_id, n_groups):
        full_table_name = table_name

        # build query, only split data if campaign_id exists
        if self.table_mappings and 'campaign_id' in self.table_mappings:
            query_fields = ','.join(field for field in self.table_mappings.values())
            query = text('select {} from {} where abs({}) % {} = {}'.format(
                query_fields, full_table_name, self.table_mappings['campaign_id'], n_groups, group_id)
            )
        else:
            if n_groups > 1:
                logger.warning('"campaing_id" field not in table, can not automatically split data, '
                               'reading full data in all workers')
            query = 'select * from {}'.format(full_table_name)

        df = pd.read_sql(query, self.get_engine())
        client.dump_table(df, table_name, table_mappings=self.table_mappings)

    def upsync(self, client, source_table, target_table, drop_table=False):
        try:
            df = client.load_table(source_table)
        except OperationalError:
            return
        if not df.empty:
            with self.lock:
                df.to_sql(target_table, self.engine, index=False, if_exists='replace' if drop_table else 'append')

    def drop_table(self, table_name):
        logger.info('Dropping table...')
        with self.lock, self.get_engine().begin() as conn:
            conn.execute('DROP TABLE IF EXISTS {}'.format(table_name))
