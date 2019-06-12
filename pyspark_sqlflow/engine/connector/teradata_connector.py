from __future__ import print_function

from pyspark_sqlflow.engine.connector.jdbc_connector import JDBCConnector


class TeradataConnector(JDBCConnector):

    name = 'teradata'

    tmode = 'ANSI'
    client_charset = 'UTF8'

    def __init__(self, conn_conf, jdbc_conf, jars):
        super(TeradataConnector, self).__init__(conn_conf, jdbc_conf, jars)

    def _set_conf(self):
        super(TeradataConnector, self)._set_conf()
        self.tmode = self.conn_conf.get('tmode') or self.tmode
        self.client_charset = self.conn_conf.get('client_charset') or self.client_charset

    def get_jdbc_url(self, db):
        return """jdbc:{url}/DATABASE={db}, TMODE={tmode},
                CLIENT_CHARSET={client_charset} COLUMN_NAME=ON, MAYBENULL=ON""".format(
                url=self.url, db=db,
                tmode=self.tmode, client_charset=self.client_charset)
