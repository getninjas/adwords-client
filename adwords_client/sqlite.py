import inspect
import logging
import sqlite3
import tempfile

import sqlalchemy
import sqlalchemy.orm

logger = logging.getLogger(__name__)


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
    file_obj = None
    db_source = ''
    if file_name:
        db_source = file_name
    elif not connection_factory:
        file_obj = tempfile.NamedTemporaryFile()
        db_source = file_obj.name

    connect_string = 'sqlite:///{}'.format(db_source) if db_source != ':memory:' else 'sqlite://'

    def new_connection():
        return connection_factory() if connection_factory else sqlite_factory(db_source)

    if sqlalquemy_engine:
        conn = sqlalchemy.create_engine(connect_string, creator=new_connection)
    else:
        conn = new_connection()

    if file_obj:
        conn._TEMP_FILE_OBJ = file_obj
    return conn


def execute(engine, target_name, operation, *multiparams, schema=None, **params):
    target = sqlalchemy.Table(target_name,
                              sqlalchemy.MetaData(engine),
                              autoload=True,
                              autoload_with=engine,
                              schema=schema)
    with engine.begin() as conn:
        conn.execute(getattr(target, operation)(), *multiparams, **params)
