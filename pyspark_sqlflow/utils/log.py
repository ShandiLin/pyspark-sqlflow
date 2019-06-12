import os
import pkg_resources
import logging
import logging.config

from pyspark_sqlflow import setting


def get_log_config():
    return pkg_resources.resource_filename('pyspark_sqlflow', 'utils/log.ini')

def get_logfilename():
    log_base_folder = setting.pyspark_sqlflow_conf.get('log').get('logs_folder')
    job_name = os.environ.get('JOB', 'default')
    job_log_folder = os.path.join(log_base_folder, job_name)
    if not os.path.isdir(job_log_folder):
        os.makedirs(job_log_folder)
    return os.path.join(job_log_folder, job_name + '.log')


class Log(object):

    def __init__(self, config_path=None):
        if not config_path:
            config_path = get_log_config()
        self.config_path = config_path
        logging.logfilename = get_logfilename()
        logging.config.fileConfig(fname=self.config_path)

    @property
    def log(self):
        try:
            return self._log
        except AttributeError:
            self._log = logging.getLogger(
                self.__class__.__module__ + '.' + self.__class__.__name__)
            return self._log
