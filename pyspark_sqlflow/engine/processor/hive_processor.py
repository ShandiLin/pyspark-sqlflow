from __future__ import print_function

import re
import subprocess

from pyspark_sqlflow.utils.log import Log
from pyspark_sqlflow.engine.processor.base_processor import BaseProcessor

from cathay_hive.hive_inserter import HiveInserter


def get_hive_exec_path():
    try:
        cmd = ['which', 'hive']
        hive_exec_path = subprocess.check_output(cmd).strip()
        return hive_exec_path
    except:
        print('{cmd} is not working in console, hive path might not defined?'.format(" ".join(cmd)))
        raise

def get_hive_version():
    try:
        hive_exec_path = get_hive_exec_path()
        cmd = [hive_exec_path, '--version']
        hive = subprocess.check_output(cmd).split('\n')[0]
        hive_version = re.match("Hive (.*)-.*", hive).group(1)
        return hive_version
    except:
        print('{cmd} is not working in console, check hive path?'.format(" ".join(cmd)))
        raise

def cast_date_columns_to_string(df):
    import pyspark.sql.functions as F
    from pyspark.sql.types import StringType

    date_columns = [schema.name for schema in df.schema if str(schema.dataType) == 'DateType']
    for dc in date_columns:
        df = df.withColumn(dc, F.col(dc).cast(StringType()))
    return df

def cast_decimal_columns_to_double(df):
    import pyspark.sql.functions as F
    from pyspark.sql.types import DoubleType

    decimal_columns = [schema.name for schema in df.schema if str(schema.dataType).startswith('DecimalType')]
    for dec in decimal_columns:
        df = df.withColumn(dec, F.col(dec).cast(DoubleType()))
    return df


class HiveProcessor(BaseProcessor, Log):

    def __init__(self, conn):
        super(HiveProcessor, self).__init__(conn)

    def create_view(self, table, view):
        return self.conn.sql("""create or replace view {view} as select * from {table}""".format(view=view, table=table))

    def write(self, df, conf, fmt='table'):
        if fmt == 'table':
            tbl_format = conf.get('table_format') or 'parquet'
            drop_tbl = conf.get('drop_table') or False
            self.write_table(df, conf.get('table'), write_mode=conf.get('mode'),
                                drop_table=drop_tbl, table_format=tbl_format,
                                partition=conf.get('num_partitions'),
                                partition_by=conf.get('partition_by'))

    def write_table(self, df, table, write_mode='overwrite', partition=1,
                    drop_table=False, table_format="parquet", partition_by=None):

        if table_format == 'parquet':
            self.log.warning(
                'due to decimal conversion difference between hive and spark in parquet format, '
                'cast all DecimalType to DoubleType if using parquet format. '
                'Or adding .config("spark.sql.parquet.writeLegacyFormat", True) when creating spark'
            )
            df = cast_decimal_columns_to_double(df)
            hive_version = get_hive_version()
            self.log.info('Hive version:', hive_version)
            if hive_version < '1.2.0':
                self.log.warning(
                    'due to date format unsupported in parquet in hive 1.1 (solved since hive 1.2), '
                    'cast all DateType column to StringType if hive < 1.2 and using parquet format to write table.'
                )
                df = cast_date_columns_to_string(df)

        HiveInserter.write_table(spark=self.conn, df=df, table_name=table,
                        write_mode=write_mode, partition=partition,
                        drop_table=drop_table, table_format=table_format,
                        partition_by=partition_by)


    def query(self, sql):
        return self.conn.sql(sql)
