# airflowRedditPysparkDag.py
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
import os

'''
input arguments for downloading S3 data 
and Spark jobs
REMARK: 
Replace `srcDir` and `redditFile` as the full paths containing your PySpark scripts
and location of the Reddit file will be stored respectively 
'''

redditFile = os.getcwd() + '/data/RC-s3-2007-10'
srcDir = os.getcwd() + ''
packages = "--packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0 "
sparkSubmit = 'spark-submit'

## Define the DAG object
default_args = {
    'owner': 'neel',
    'depends_on_past': False,
    'start_date': datetime(2020, 5, 7),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}
dag = DAG('mongoTwitterPyspark', default_args=default_args, schedule_interval='*/5 * * * *')



#task to compute mobility trends
t1 = BashOperator(
    task_id='mobility',
    bash_command=sparkSubmit + ' ' + packages + srcDir + '/mobilityTrendSparkJob.py',
    dag=dag)

#task to analyse tweets
t1 = BashOperator(
    task_id='tweets',
    bash_command=sparkSubmit + ' ' + packages + srcDir + '/twitterTrendsSparkJob.py',
    dag=dag)