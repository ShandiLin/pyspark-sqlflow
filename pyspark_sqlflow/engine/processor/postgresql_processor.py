from __future__ import print_function

from pyspark_sqlflow.engine.processor.base_processor import BaseProcessor


class PostgresqlProcessor(BaseProcessor):

    def __init__(self, conn):
        super(PostgresqlProcessor, self).__init__(conn)

    def query(self, sql, *args, **kwargs):
        sql_query = "({sql}) as t1".format(sql=sql)
        return self.conn.option("dbtable", sql_query).load()

    def write(self, table, mode):
        table = table.split('.')[-1]
        return self.conn.option("dbtable", table) \
                .mode(mode).save()
