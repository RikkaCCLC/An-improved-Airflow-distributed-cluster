# airflow 调度执行 shell 命令

#导入DAG 对象，需要实例化DAG对象
from datetime import datetime, timedelta

from airflow import  DAG

# 导入 BashOperator ，后期执行shell命令
from airflow.operators.bash import BashOperator


#实例化DAG
default_args = {
    'owner':'airflow',# 当前DAG 拥有者
    'start_date':datetime(2021,10,1), #第一次执行DAG 时间
    'retries':1,#任务执行失败，重试次数
    'retry_delay':timedelta(minutes=5) # 执行失败重试间隔
}

dag = DAG(
    # 指定DAG id ,显示在Airflow WebUI上
    dag_id='myairflow_execute_shell',
    #外部定义的变量，DAG 可以使用，dic格式
    default_args=default_args,
    #DAG调度周期，可以配置天，周，小时、分钟、秒
    schedule_interval=timedelta(days=1)
)

#定义task
first = BashOperator(
    #指定 task id
    task_id='first',
    #指定当前task 执行命令
    bash_command='echo "run first task"',
    #指定 opartor所属DAG
    dag=dag
)

second = BashOperator(
    #指定 task id
    task_id='second',
    #指定当前task 执行命令
    bash_command='echo "run second task"',
    #指定 opartor所属DAG
    dag=dag
)

third = BashOperator(
    #指定 task id
    task_id='third',
    #指定当前task 执行命令
    bash_command='echo "run third task"',
    #指定 opartor所属DAG
    dag=dag
)

#定义task 之间的依赖关系
first >> second >> third

# first.set_downstream(second)
# second.set_downstream(third)


