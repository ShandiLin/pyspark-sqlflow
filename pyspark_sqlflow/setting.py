import os
import sys
import toml
import glob

from pyspark_sqlflow.config.global_config_parser import GlobalConfigParser


def get_home():
    home = os.environ.get('PYSPARK_SQLFLOW_HOME', None)
    if not home:
        raise ValueError('set PYSPARK_SQLFLOW_HOME variable in environment')
    return home

def get_config(home):
    if 'PYSPARK_SQLFLOW_CONFIG' not in os.environ:
        return os.path.join(home, 'pyspark-sqlflow.toml')
    return os.environ['PYSPARK_SQLFLOW_CONFIG']


PYSPARK_SQLFLOW_HOME = get_home()
PYSPARK_SQLFLOW_CONFIG = get_config(PYSPARK_SQLFLOW_HOME)

pyspark_sqlflow_conf = GlobalConfigParser(conf=PYSPARK_SQLFLOW_CONFIG).read()


def set_spark_env():
    def add_pythonpath(fpath):
        if fpath not in sys.path:
            sys.path.append(fpath)

    def add_path(path):
        if path not in os.environ["PATH"]:
            os.environ["PATH"] = path + os.pathsep + os.environ["PATH"]

    # set environment variables
    for env_key, env_value in pyspark_sqlflow_conf.get('env', {}).items():
        if env_value:
            os.environ[env_key] = env_value

    # append path and pythonpath for spark
    spark_home = os.environ.get('SPARK_HOME', None)
    if spark_home:
        spark_bin = spark_home + '/bin'
        add_path(spark_bin)

        spark_python = spark_home + '/python'
        add_pythonpath(spark_python)

        spark_py4j = glob.glob(spark_python + '/lib/py4j-*-src.zip')[0]
        add_pythonpath(spark_py4j)
