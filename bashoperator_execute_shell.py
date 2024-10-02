# bashoperator执行shell命令
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'zhangsan',
    'start_date':datetime(2021,10,1),
    'email':'kettle_test1@163.com',
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    dag_id= 'execute_shell',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t2 = BashOperator(
    task_id='print_hello',
    bash_command='echo "hello airflow"',
    dag=dag
)

t3 = BashOperator(
    task_id='template',
    bash_command="""
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ params.name}}"
        echo "{{ params.age}}"
    {% endfor %}
    """,
    params={'name':'wangwu','age':10},
    dag=dag
)

t1>>t2>>t3


