from __future__ import print_function

from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark_sqlflow.utils.log import Log
from pyspark_sqlflow.engine.processor.base_processor import BaseProcessor


class TeradataProcessor(BaseProcessor, Log):

    def __init__(self, conn):
        super(TeradataProcessor, self).__init__(conn)

    def check_table_exist(self, table):
        dbc_tables = "dbc.tables"
        sql_query = "(select * from {dbc} where DatabaseName='{db}' and TableName='{table}') as t1".format(
                        dbc=dbc_tables, db=table.split('.')[0].upper(), table=table.split('.')[1].upper())
        return self.conn.option("dbtable", sql_query).load().count() == 1

    def get_all_yyyymm_tables(self, table):
        dbc_tables = "dbc.tables"
        sql_query = """(select TableName from {dbc}
                        where DatabaseName='{db}'
                          and TableName like '{table}%') as t1""".format(
                        dbc=dbc_tables, db=table.split('.')[0].upper(), table=table.split('.')[1].upper())
        return self.conn.option("dbtable", sql_query).load() \
                .filter("trim(TableName) rlike '^%s[0-9]{6}$'" % (table.split('.')[1].upper()))

    def get_latest_yyyymm_table_name(self, table):
        # TODO fix import here
        import pyspark.sql.functions as F
        df = self.get_all_yyyymm_tables(table)
        return df.agg(F.max(F.rtrim(F.col('TableName'))).alias('max_partition')).collect()[0].max_partition.lower()

    def get_monthly_table_name(self, table, require_yyyymm):
        if self.check_table_exist(table + require_yyyymm):
            return table + require_yyyymm
        else:
            latest_yyyymm = self.get_latest_yyyymm_table_name(table)[-6:]
            if (datetime.strptime(require_yyyymm, '%Y%m') - relativedelta(months=1)).strftime('%Y%m') == latest_yyyymm:
                print('{table}{rym} not exist, select current table instead'.format(table=table, rym=require_yyyymm))
                return table
            else:
                raise ValueError('{table}{rym} not exist'.format(table=table, rym=require_yyyymm))

    def is_monthly_table(self, table):
        def get_table():
            dbc_tables = "dbc.tables"
            sql_query = """(select TableName from {dbc} where DatabaseName='{db}' and TableName = '{table}') as t1""".format(
                            dbc=dbc_tables, db=table.split('.')[0].upper(), table=table.split('.')[1].upper())
            return self.conn.option("dbtable", sql_query).load()
        base_table = get_table()
        yyyymm_tables = self.get_all_yyyymm_tables(table)
        return base_table.count() == 1 and yyyymm_tables.count() > 1

    # TODO check if backfill has been done
    def query_monthly_table_data(self, table, yyyymm, sql, backfill=False):
        # TODO fix import here
        import pyspark.sql.functions as F
        table_m = self.get_monthly_table_name(table, yyyymm)
        sql = sql.replace(table, table_m)
        self.log.info('sql:', sql)
        sql_query = "({sql}) as t1".format(sql=sql)
        df = self.conn.option("dbtable", sql_query).load().withColumn("yyyymm", F.lit(yyyymm))
        if backfill:
            prev_ym = (datetime.strptime(yyyymm, '%Y%m') - relativedelta(months=1)).strftime('%Y%m')
            if self.check_table_exist(table + prev_ym):
                self.log.info('backfill {table} since it pop up!!!'.format(table=table + prev_ym))
                backfill_sql = sql.replace(table, table + prev_ym)
                backfill_sql_query = "({sql}) as t1".format(sql=backfill_sql)
                df = df.unionAll(self.conn.option("dbtable", backfill_sql_query).load().withColumn("yyyymm", F.lit(prev_ym)))
                return df
            else:
                self.log.info("{table} haven't exist, no need to backfill".format(table=table + prev_ym))
        return df

    def query_one_time_table_data(self, sql):
        self.log.info('sql:', sql)
        sql_query = "({sql}) as t1".format(sql=sql)
        return self.conn.option("dbtable", sql_query).load()

    def query(self, sql, *args, **kwargs):
        table_type = kwargs.get('table_type')
        if table_type == 'X':
            return self.query_one_time_table_data(sql)
        elif table_type == 'M':
            return self.query_monthly_table_data(
                    table=kwargs['table'],
                    yyyymm=kwargs['yyyymm'],
                    sql=sql,
                    backfill=kwargs['backfill'])
        elif not table_type:
            self.log.info('no table type specified')
        else:
            self.log.error('unknown table type: {}'.format(table_type))

    def write(self, table, mode):
        table = table.split('.')[-1]
        return self.conn.option("dbtable", table) \
                .option("mode", mode).save()
