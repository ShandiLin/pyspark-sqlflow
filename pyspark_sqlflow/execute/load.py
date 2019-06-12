from __future__ import print_function

from pyspark_sqlflow.utils.log import Log
from pyspark_sqlflow.engine.handler import DB
from pyspark_sqlflow.engine.handler import Handler


class Loader(Log):
    """
    Execute the destination section in every sql steps
    """
    def __init__(self, conf, *args, **kwargs):
        self.conf = conf
        self.jars = kwargs.get('jars')

    def set_engine(self, df):
        self.engine = Handler(self.conf, 'write', jars=self.jars).get_processor(df)

    def execute(self, df):
        self.set_engine(df)
        engine_name = self.conf.get('engine', {}).get('name')
        table = self.conf.get('table')
        mode = self.conf.get('mode')
        self.log.info('writing {tbl} in {engine}, with mode: {mode}'.format(
            tbl=table, engine=engine_name, mode=mode))

        if engine_name == DB.HIVE:
            self.engine.write(df, self.conf)
        elif engine_name in DB.USE_JDBC:
            self.engine.write(table, mode)
