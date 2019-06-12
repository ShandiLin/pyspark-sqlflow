from __future__ import print_function

from pyspark_sqlflow import setting

from pyspark_sqlflow.engine.connector.postgresql_connector import PostgresqlConnector
from pyspark_sqlflow.engine.connector.teradata_connector import TeradataConnector
from pyspark_sqlflow.engine.connector.hive_connector import HiveConnector

from pyspark_sqlflow.engine.processor.postgresql_processor import PostgresqlProcessor
from pyspark_sqlflow.engine.processor.teradata_processor import TeradataProcessor
from pyspark_sqlflow.engine.processor.hive_processor import HiveProcessor


class DB:
    TERADATA = 'teradata'
    POSTGRESQL = 'postgresql'
    HIVE = 'hive'

    USE_JDBC = [TERADATA, POSTGRESQL]


def connector(name):
    if name == DB.TERADATA:
        return TeradataConnector
    elif name == DB.POSTGRESQL:
        return PostgresqlConnector
    elif name == DB.HIVE:
        return HiveConnector
    else:
        raise ValueError('unknown connector: {name}'.format(name=name))


def processor(name):
    if name == DB.TERADATA:
        return TeradataProcessor
    elif name == DB.POSTGRESQL:
        return PostgresqlProcessor
    elif name == DB.HIVE:
        return HiveProcessor
    else:
        raise ValueError('unknown processor: {name}'.format(name=name))


class Handler(object):

    valid_engine_types = ['read', 'write']

    def __init__(self, job_conf, engine_type, jars=None):
        if engine_type not in self.valid_engine_types:
            raise ValueError("""unknown engine_type: {v}, available options: {l}""".format(
                    v=engine_type, l=self.valid_engine_types))
        self.engine_type = engine_type
        self.db = job_conf['table'].split('.')[0]

        engine = job_conf['engine']
        self.engine_name = engine['name']
        self.job_jdbc_conf = engine.get('jdbc')
        self.conn_conf = setting.pyspark_sqlflow_conf.get('database', {}).get(self.engine_name)
        self.jars=jars

    def get_processor(self, df=None):
        conn_cls = connector(self.engine_name)

        if self.engine_name in DB.USE_JDBC:
            # import here to escape import ordering error
            from pyspark_sqlflow.config.job_config_parser import merge_jdbc_conf
            if self.engine_type == 'read':
                jdbc_conf = merge_jdbc_conf(
                    self.job_jdbc_conf, self.conn_conf.get('jdbc.reader', {}))
                conn = conn_cls(conn_conf=self.conn_conf, jdbc_conf=jdbc_conf, jars=self.jars) \
                        .get_reader(db=self.db)
            elif self.engine_type == 'write':
                jdbc_conf = merge_jdbc_conf(
                    self.job_jdbc_conf, self.conn_conf.get('jdbc.writer', {}))
                conn = conn_cls(conn_conf=self.conn_conf, jdbc_conf=jdbc_conf, jars=self.jars) \
                        .get_writer(db=self.db, df=df)
        elif self.engine_name == DB.HIVE:
            conn = conn_cls().get_conn()
        else:
            raise ValueError('unknown engine name {v}'.format(v=engine_name))

        Processor = processor(self.engine_name)
        return Processor(conn=conn)
