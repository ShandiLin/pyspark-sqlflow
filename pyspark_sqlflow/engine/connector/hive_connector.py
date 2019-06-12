from __future__ import print_function

from pyspark_sqlflow.engine.spark import get_spark
from pyspark_sqlflow.engine.connector.base_connector import BaseConnector


class HiveConnector(BaseConnector):

    def get_conn(self):
        return get_spark()

    def get_reader(self):
        return self.get_conn()

    def get_writer(self):
        return self.get_conn()
