from datetime import datetime, timedelta
from airflow import  DAG
from airflow.operators.bash import BashOperator


#实例化DAG
default_args = {
    'owner':'zhangsan',# 当前DAG 拥有者
    'start_date':datetime(2021,10,1), #第一次执行DAG 时间
    'retries':1,#任务执行失败，重试次数
    'retry_delay':timedelta(minutes=5) # 执行失败重试间隔
}

dag = DAG(
    dag_id='airflow_depends2',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
)

#定义task
A = BashOperator(
    task_id='A',
    bash_command='echo "run A task"',
    dag=dag
)
B = BashOperator(
    task_id='B',
    bash_command='echo "run B task"',
    dag=dag
)
C = BashOperator(
    task_id='C',
    bash_command='echo "run C task"',
    dag=dag
)
D = BashOperator(
    task_id='D',
    bash_command='echo "run D task"',
    dag=dag
)

#定义task 之间的依赖关系
A >> C >>D
B >> C >>D