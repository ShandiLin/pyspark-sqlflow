# Pyspark sql flow

writing `.toml config` to transfer data between different databases using pyspark
> connect to db through spark jdbc

* connectors is defined in `pyspark_sqlflow/engine/connector/*`
* corresponding processors is defined in `pyspark_sqlflow/engin/processor/*`
> available connectors and processor: hive, postgresql, teradata
>> it's ok to include your own connectors and processors

## TODO
* automatically generate folders and global config when initial
* support spark custom udf from plugins
* support storing result dataframe to variables or temp view as source of next step instead of writing to table in every steps

## how to use
* create folder and export PATH
```
mkdir <project_folder>
export PYSPARK_SQLFLOW_HOME=<your path to project_folder>
```

* set global config<br/>
<project_folder>/pyspark-sqlflow.toml
> set all your database connect information<br/>
> set environment and spark parameters
```
title = "global config file"

[job]
jobs_folder = "<project_folder>/jobs"

[log]
logs_folder = "<project_folder>/logs"

[database]
    [database.teradata]
    url = "teradata://<ip>"
    tmode = "ANSI"
    client_charset = "UTF8"
    user = ""
    password = ""

        [database.teradata.jar]
        jar_path = ""

        [database.teradata.jdbc]
        driver = "com.teradata.jdbc.TeraDriver"

            [database.teradata.jdbc.reader]
            num_partitions = 2
            partition_column = ""
            lower_bound = 1
            upper_bound = 100
            fetchsize = 100

            [database.teradata.jdbc.writer]
            batchsize = 100

    [database.postgresql]
    url = "postgresql://<ip>:5432"
    user = ""
    password = ""

        [database.postgresql.jar]
        jar_path = ""

        [database.postgresql.jdbc]
        driver = "org.postgresql.Driver"

        [database.postgresql.jdbc.reader]
        num_partitions = 3
        partition_column = ""
        lower_bound = 1
        upper_bound = 100
        fetchsize = 100

        [database.postgresql.jdbc.writer]
        batchsize = 100

[env]
JAVA_HOME = ""
HADOOP_CONF_DIR = ""
SPARK_HOME = ""

[spark]
"spark.app.name" = "pyspark-sqlflow-etl"
"spark.master" = "local"
"spark.executor.memory" = "1g"
"spark.executor.cores" = 1
"spark.executor.instances" = 1
"spark.driver.memory" = "1g"
"spark.driver.cores" = 1
"spark.jars" = ""

```

* create jobs and logs folder
```
cd <project_folder>
mkdir jobs
mkdir logs
```

* put your job config in jobs folder
> * define metadata<br/>
> * define sql steps - support variables render by jinja<br/>
> * define engine(db) and jdbc parameters
```
[metadata]
name="test"
job_type="D"
description="testing"

[[steps]]
name="step - 1"
description="read from postgresql, and write to postgresql"

[steps.source]
table = "test.test$YYYYMM"
sql = "select id, name from test$YYYYMM limit 100"

[steps.source.variables]
YYYYMM = "{{ dt | dt.add_month(trace_num=-1, format='%Y%m') }}"

[steps.source.engine]
name = "postgresql"
    [steps.source.engine.jdbc]
    num_partitions = 3
    partition_column = "id"
    lower_bound = 1
    upper_bound = 500

[steps.destination]
table = "test.test1"
mode = "overwrite"

[steps.destination.engine]
name = "postgresql"
    [steps.destination.engine.jdbc]
    num_partitions = 1

[[steps]]
name="step - 2"
...
```

* run your job by command
```
sqlflow run <job file name>
```

* log will be record in logs folder
