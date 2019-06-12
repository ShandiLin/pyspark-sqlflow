from __future__ import print_function

import os
import time
from datetime import datetime, timedelta

from pyspark_sqlflow.utils.log import Log
from pyspark_sqlflow import setting
from pyspark_sqlflow.config.job_config_parser import JobConfigParser
from pyspark_sqlflow.execute.extract import Extract
from pyspark_sqlflow.execute.load import Loader
from pyspark_sqlflow.setting import set_spark_env


JOBS_DIR = setting.pyspark_sqlflow_conf.get('job').get('jobs_folder')


class CLI(Log):

    def run(self, job_conf, backfill, exec_date):
        start_time = time.time()
        self.log.info("===== run sql in config file =====")
        self.log.info("""run {job_conf}, with args:\nbackfill={backfill}\nexec_date={exec_date}""".format(
        job_conf=job_conf, backfill=backfill, exec_date=exec_date))

        trace_back_days = 2
        exec_dt = datetime.strptime(exec_date, "%Y-%m-%d")
        yyyymm = (exec_dt - timedelta(days=trace_back_days)).strftime('%Y%m')
        self.log.info('yyyymm (now - {back_days} days): {ym}'.format(back_days=trace_back_days, ym=yyyymm))

        jcp = JobConfigParser(conf=os.path.join(JOBS_DIR, job_conf),
                              exec_date=exec_date)
        jobs = jcp.read()
        all_jars = jcp.get_all_jars(jobs)

         # TODO might be set elsewhere
        set_spark_env()

        # TODO parse file in config, call function
        for step in jobs['steps']:
            step_start_time = time.time()
            self.log.info('===== {} ====='.format(step['name']))
            if step.get('source'):
                self.log.info('===== read from source =====')
                ext = Extract(conf=step['source'], jars=all_jars)
                df = ext.execute(yyyymm=yyyymm, backfill=backfill)
                df.show(3)
                reading_time = time.time() - step_start_time
                self.log.info("===== reading time: {}  =====".format(timedelta(seconds=reading_time)))

            if step.get('destination'):
                self.log.info('===== write to destination =====')
                load = Loader(conf=step['destination'], jars=all_jars)
                load.execute(df)
                writing_time = time.time() - step_start_time - reading_time
                self.log.info("===== writing time: {}  =====".format(timedelta(seconds=writing_time)))

        execution_time = time.time() - start_time
        self.log.info("===== Total execution time: {}  =====".format(timedelta(seconds=execution_time)))
