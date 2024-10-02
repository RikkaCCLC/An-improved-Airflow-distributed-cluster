# hiveOperator 执行hive sql
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator


default_args = {
    'owner' : 'maliu',
    'start_date':datetime(2021,10,1),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    dag_id= 'execute_hive_sql',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
)

first = HiveOperator(
    task_id='person_info',
    hive_cli_conn_id='node1-hive-metastore',#连接Hive metastore
    hql='select id,name,age from person_info',
    dag=dag
)
second = HiveOperator(
    task_id='score_info',
    hive_cli_conn_id='node1-hive-metastore',#连接Hive metastore
    hql='select id,name,score from score_info',
    dag=dag
)

third = HiveOperator(
    task_id='join',
    hive_cli_conn_id='node1-hive-metastore',#连接Hive metastore
    hql='select a.id,a.name,a.age,b.score from person_info a,score_info b where a.id = b.id',
    dag=dag
)

first >> second >> third


