from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import run_twitter_etl,_run_twitter_etl

defaul_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2024,28,2),
    'email':['email@gmail.com'],
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}


dag = DAG(
    'twitter_dag',
    defaul_args = defaul_args,
    description='MY first etl code'
)

#run_etl =PythonOperator(
#    task_id = 'complete_twitter_etl',
#    python_callable = _run_twitter_etl,
#    dag=dag,
#)

run_etl =PythonOperator(
    task_id = 'complete_twitter_etl',
    python_callable = run_twitter_etl,
    dag=dag,
)

run_etl