# python operator执行python 方法逻辑
import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner' : 'tianqi',
    'start_date':datetime(2021,10,1),
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag = DAG(
    dag_id= 'execute_pythoncode',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1)
)

def print_hello1(*a ,**b):
    print(a)
    print(b)
    print("print-hello1:hello airflow")
    return "xxx-sss"

def print_hello2(rd):
    print(rd)
    print("print-hello2:hello airflow")
    return "111-222"

first = PythonOperator(
    task_id='first',
    python_callable=print_hello1,
    op_args=[1,2,3,"aaa","bbb"],
    op_kwargs={"id":1,"name":"zs","age":18},
    dag=dag
)

second = PythonOperator(
    task_id='second',
    python_callable=print_hello2,
    op_kwargs={"rd":random.randint(0,9)},
    dag=dag
)

first >> second


