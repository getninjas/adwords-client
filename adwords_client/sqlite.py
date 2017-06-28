import inspect
import logging
import os
import sqlite3
import tempfile

import sqlalchemy

logger = logging.getLogger(__name__)
TEMPORARY_FILES = []


def sqlite_add_function(conn, f, function_name=None):
    def wrapped_fuction(*args):
        try:
            return f(*args)
        except:
            return None
    if function_name is None:
        function_name = f.__name__
    step_args = inspect.signature(f)
    n_args = len(step_args.parameters) if 'args' not in step_args.parameters else -1
    conn.create_function(function_name, n_args, wrapped_fuction)
    return wrapped_fuction


def sqlite_factory(file_name=None, **kwargs):
    db_source = file_name if file_name else ':memory:'
    conn = sqlite3.connect(db_source, timeout=300.0)
    conn.row_factory = sqlite3.Row
    for func_name, func in kwargs.items():
        sqlite_add_function(conn, func, function_name=func_name)
    return conn


def get_connection(file_name=None, sqlalquemy_engine=True, connection_factory=None):
    if file_name:
        db_source = file_name
    elif not connection_factory:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            db_source = temp_file.name
            TEMPORARY_FILES.append(db_source)
    else:
        db_source = ''

    connect_string = 'sqlite:///{}'.format(db_source) if db_source != ':memory:' else 'sqlite://'

    def new_connection():
        return connection_factory() if connection_factory else sqlite_factory(db_source)

    if sqlalquemy_engine:
        return sqlalchemy.create_engine(connect_string, creator=new_connection)
    else:
        return new_connection()


def remove_temporary_files():
    for file_name in TEMPORARY_FILES:
        try:
            os.remove(file_name)
        except:
            pass


def dict_query(conn, query, n_key=1, *args):
    data = conn.execute(query).fetchall()
    result = {}
    args = [n_key] + list(args)
    for row in data:
        key_pos = 0
        curr_dict = result
        for n_key in args[:-1]:
            key = tuple(row[key_pos:key_pos + n_key]) if n_key > 1 else row[key_pos]
            if key not in curr_dict:
                curr_dict[key] = {}
            key_pos += n_key
            curr_dict = curr_dict[key]
        n_key = args[-1]
        key = tuple(row[key_pos:key_pos + n_key]) if n_key > 1 else row[key_pos]
        if key not in curr_dict:
            curr_dict[key] = []
        curr_dict[key].append(row)
    return result


def create_index(engine, table_name, *args):
    idx_name = '_'.join(args)
    idx_fields = ', '.join(args)
    query = 'create index if not exists {0}_{1}_idx on {0} ({2})'.format(table_name, idx_name, idx_fields)
    if type(engine) == sqlalchemy.engine.Engine:
        with engine.begin() as conn:
            conn.execute(query)
    else:
        with engine:
            engine.execute(query)


def execute(engine, target_name, operation, *multiparams, schema=None, **params):
    target = sqlalchemy.Table(target_name,
                              sqlalchemy.MetaData(engine),
                              autoload=True,
                              autoload_with=engine,
                              schema=schema)
    with engine.begin() as conn:
        conn.execute(getattr(target, operation)(), *multiparams, **params)
