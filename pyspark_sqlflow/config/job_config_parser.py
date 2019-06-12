import stringcase
import pkg_resources
from datetime import datetime

from pyspark_sqlflow import setting
from pyspark_sqlflow.engine.handler import DB
from pyspark_sqlflow.config.base_config_parser import BaseConfigParser
from pyspark_sqlflow.config.jinja import Jinja
from pyspark_sqlflow.utils.log import Log


def get_default_jar_path(engine_name):
    jars = pkg_resources.resource_listdir('pyspark_sqlflow', 'jars')
    need_jars = list()
    for jar in jars:
        if engine_name in jar:
            # Do not use os.path.join()
            need_jars.append(pkg_resources.resource_filename('pyspark_sqlflow', '/'.join(('jars', jar))))
    return ','.join(need_jars) if len(need_jars) > 0 else None


def merge_jdbc_conf(job_jdbc_conf, global_jdbc_conf):
    for key, val in global_jdbc_conf.items():
        job_val = job_jdbc_conf.get(key)
        if val and job_val:
            print('{k}: job: {v}, global: {gv}'.format(k=key, v=job_val, gv=val))
            if stringcase.camelcase(key) == 'numPartitions' and job_val > val:
                raise ValueError("""{key} set to {j_val} in job conf,
                    which can not be larger than global setting: {g_val}""".format(
                    key=key, j_val=job_val, g_val=val
                ))
        elif val and not job_val:
            job_jdbc_conf[stringcase.camelcase(key)] = val
    return job_jdbc_conf


class JobConfigParser(BaseConfigParser, Log):

    def __init__(self, conf, exec_date):
        super(JobConfigParser, self).__init__(conf, exec_date)

    def render_variables(self, var_dict, **kwargs):
        if var_dict:
            for key in var_dict:
                var_dict[key] = Jinja().render(var_dict[key], **kwargs)
            return var_dict
        return {}

    def replace_sql(self, sql, var_dict):
        if var_dict:
            for var in var_dict:
                sql = sql.replace('$'+var, var_dict[var])
        return sql

    def handle_variables(self, step, section, **kwargs):
        sec = step.get(section)
        step[section]['variables'] = self.render_variables(sec.get('variables'), **kwargs)
        if sec.get('sql'):
            step[section]['sql'] = self.replace_sql(sec['sql'], step[section]['variables'])
        if sec.get('table'):
            step[section]['table'] = self.replace_sql(sec['table'], step[section]['variables'])
        return step

    def _preprocess(self, conf_obj):
        # TODO validate setting
        dt = datetime.strptime(self.exec_date, '%Y-%m-%d')
        for i, step in enumerate(conf_obj['steps']):
            conf_obj['steps'][i] = self.handle_variables(step, "source", dt=dt)
            conf_obj['steps'][i] = self.handle_variables(step, "destination", dt=dt)

    def get_all_jars(self, conf_obj):
        jars = list()
        for step in conf_obj['steps']:
            engine_name = step.get('source', {}).get('engine', {}).get('name')
            conn_conf = setting.pyspark_sqlflow_conf.get('database', {}).get(engine_name)
            if engine_name in DB.USE_JDBC:
                jar_path = conn_conf.get('jar', {}).get('jar_path') \
                            or get_default_jar_path(engine_name)
                jars.append(jar_path)
        self.log.info('need jars: {}'.format(','.join(jars)))
        return ','.join(jars) if len(jars) > 0 else None
