from pyspark_sqlflow.config.base_config_parser import BaseConfigParser


class GlobalConfigParser(BaseConfigParser):

    def __init__(self, conf, exec_date=None):
        super(GlobalConfigParser, self).__init__(conf, exec_date)

    def _preprocess(self, conf_obj):
        # TODO validate setting
        pass
