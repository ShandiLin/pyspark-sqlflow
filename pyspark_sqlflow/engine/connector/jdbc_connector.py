from __future__ import print_function

from pyspark_sqlflow.engine.spark import get_spark
from pyspark_sqlflow.engine.connector.base_connector import BaseConnector


class JDBCConnector(BaseConnector):
    '''
    Read and write to through jdbc
    '''
    name = None

    def __init__(self, conn_conf, jdbc_conf=None, jars=None, *args, **kwargs):
        self.conn_conf = conn_conf
        self.jdbc_conf = jdbc_conf
        self.jars = jars
        self._set_conf()

    def _set_conf(self):
        self.url = self.conn_conf.get('url')
        self.user = self.conn_conf.get('user')
        self.password = self.conn_conf.get('password')
        self.driver = self.conn_conf.get('jdbc', {}).get('driver')

    def get_jdbc_url(self, db):
        raise NotImplementedError('implement how to compose url for connection')

    def get_reader(self, db):
        '''
            return: pyspark.sql.readwriter.DataFrameReader
        '''
        jdbc_url = self.get_jdbc_url(db)

        spark = get_spark(jars=self.jars)
        conn = spark.read \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", jdbc_url)

        if self.jdbc_conf:
            for key, val in self.jdbc_conf.items():
                print('key:', key, 'val:', val)
                conn.option(key, val)

        if self.user and self.password:
            conn = conn \
                .option("user", self.user) \
                .option("password", self.password)

        return conn

    def get_writer(self, db, df):
        '''
            return: pyspark.sql.readwriter.DataFrameWriter
        '''
        jdbc_url = self.get_jdbc_url(db)

        conn = df.write \
            .format("jdbc") \
            .option("driver", self.driver) \
            .option("url", jdbc_url)

        if self.jdbc_conf:
            for key, val in self.jdbc_conf.items():
                print('key:', key, 'val:', val)
                conn.option(key, val)

        if self.user and self.password:
            conn = conn \
                .option("user", self.user) \
                .option("password", self.password)

        return conn
