import toml


class BaseConfigParser(object):

    def __init__(self, conf, exec_date):
        self.conf = conf
        self.exec_date = exec_date

    def _preprocess(self, conf_obj):
        # TODO validate setting
        pass

    def read(self):
        with open(self.conf, 'rb') as f:
            conf_obj = toml.load(self.conf)
        self._preprocess(conf_obj)
        return conf_obj
