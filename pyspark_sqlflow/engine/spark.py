from __future__ import print_function

from pyspark_sqlflow import setting

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark_sqlflow.engine.spark_udf import *

DEFAULT_SPARK_APP_NAME = 'pyspark_sqlflow'
DEFAULT_SPARK_MASTER = 'local'


def get_spark(jars=None):

    spark_cfg = setting.pyspark_sqlflow_conf.get('spark', {})

    # init spark session (set default name and master)
    spark = SparkSession.builder \
        .appName(DEFAULT_SPARK_APP_NAME) \
        .master(DEFAULT_SPARK_MASTER)

    # add jars for jdbc (using default if not set)
    if jars:
        spark.config('spark.jars', jars)

    # add hive setting if master is yarn
    if spark_cfg['spark.master'] == 'yarn':
        spark.enableHiveSupport() \
            .config('hive.exec.dynamic.partition.mode', 'nonstrict') \
            .config('spark.sql.parquet.compression.codec', 'uncompressed')

    # set config from pyspark_sqlflow.cfg (spark.jars can be overwrite)
    for key in spark_cfg:
        if spark_cfg[key]:
            spark.config(key, spark_cfg[key])

    # create spark session
    spark = spark.getOrCreate()

    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger('org').setLevel(log4jLogger.Level.ERROR)
    log4jLogger.LogManager.getLogger('akka').setLevel(log4jLogger.Level.ERROR)
    log4jLogger.LogManager.getRootLogger().setLevel(log4jLogger.Level.ERROR)
    return spark


def get_logger(spark):
    name = spark.conf.get('spark.app.name', DEFAULT_SPARK_APP_NAME)
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log4jLogger.LogManager.getLogger(name).setLevel(log4jLogger.Level.ERROR)
    return log4jLogger.LogManager.getLogger(name)
