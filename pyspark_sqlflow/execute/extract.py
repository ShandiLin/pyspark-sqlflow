from __future__ import print_function

from pyspark_sqlflow.engine.handler import Handler


class Extract(object):
    """
    Execute the source section in every sql steps
    """
    def __init__(self, conf, *args, **kwargs):
        self.conf = conf
        self.jars = kwargs.get('jars')

    def set_engine(self):
        self.engine = Handler(self.conf, 'read', jars=self.jars).get_processor()

    def execute(self, yyyymm=None, backfill=False):
        self.set_engine()
        if self.conf.get('type') == 'M' and not yyyymm:
            raise ValueError('need yyyymm for monthly table')

        return self.engine.query(self.conf['sql'],
                          table_type=self.conf.get('type'),
                          table=self.conf.get('table'),
                          yyyymm=yyyymm,
                          backfill=backfill)
