import inspect
import logging
import os
import sqlite3
import tempfile

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy.ext.automap import automap_base

logger = logging.getLogger(__name__)
TEMPORARY_FILES = []


def itertable(engine, table_name):
    model = get_model_from_table(table_name, engine)
    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    s = Session()
    for instance in s.query(model).order_by(model.id):
        yield instance


def bulk_insert(engine, table_name, data, model=None):
    model = get_model_from_table(table_name, engine) if model is None else model
    Session = sqlalchemy.orm.sessionmaker(bind=engine)
    s = Session()
    s.bulk_insert_mappings(model, data)
    s.commit()


def get_model_from_table(table_name, engine):
    metadata = sqlalchemy.MetaData()
    metadata.reflect(engine, only=[table_name])
    Base = automap_base(metadata=metadata)
    Base.prepare()
    return getattr(Base.classes, table_name)


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
