# sshOperator 远程节点执行脚本
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

default_args = {
    'owner' : 'wangwu',
    'start_date':datetime(2021,10,1),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    dag_id= 'execute_remote_shell',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
)

first = SSHOperator(
    task_id='first',
    ssh_conn_id='ssh-node5',#连接node5
    command='sh /root/second_shell.sh ',
    dag=dag
)
second = SSHOperator(
    task_id='second',
    ssh_conn_id='ssh-node5',#连接node5
    remote_host='192.168.179.6',
    command='sh /root/first_shell.sh ',
    dag=dag
)

first >> second


