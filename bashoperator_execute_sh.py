# bashoperator执行shell 脚本
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'lisi',
    'start_date':datetime(2021,10,1),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    dag_id= 'execute_shell_sh',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
)

first = BashOperator(
    task_id='first',
    bash_command='sh /root/airflow/dags/first_shell.sh %s'%datetime.now().strftime("%Y-%m-%d"),
    dag=dag
)
second = BashOperator(
    task_id='scond',
    bash_command='sh /root/airflow/dags/second_shell.sh %s'%datetime.now().strftime("%Y-%m-%d"),
    dag=dag
)

first >> second