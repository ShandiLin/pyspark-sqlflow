from __future__ import print_function

from pyspark_sqlflow.engine.connector.jdbc_connector import JDBCConnector


class PostgresqlConnector(JDBCConnector):

    name = 'postgresql'

    def __init__(self, conn_conf, jdbc_conf, jars):
        super(PostgresqlConnector, self).__init__(conn_conf, jdbc_conf, jars)

    def get_jdbc_url(self, db):
        return """jdbc:{url}/{db}""".format(url=self.url, db=db)
