from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator

import json
import os


def create_dag(dag_id, schedule, default_args, conf):
    dag = DAG(dag_id, default_args=default_args, schedule_interval=schedule)

    with dag:
        run_python_task = PythonOperator(
            task_id='python_op_tester',
            dag=dag
        )

        run_docker_task = DockerOperator(
            api_version='1.19',
            docker_url='tcp://localhost:2375',
            command='/bin/sleep 30',
            image='centos:latest',
            network_mode='bridge',
            task_id='docker_op_tester',
            dag=dag
        )

        # init >> run_python_task >> run_docker_task
        run_python_task >> run_docker_task

        return dag


def initialize():
    """
    Get .json files from S3
    :return:
    """
    pass


path = '/Users/siromanto/free/0.projects/test-dynamic-airflow/data'   #TODO: change it to relative
dag_configs = []

for file in os.listdir(path):
    if file.endswith('.json'):
        print(file)
        path_file = path + '/' + file
        with open(path_file, 'r', encoding='utf-8') as json_data:
            conf = json.load(json_data)
            dag_configs.append(conf)


for conf in dag_configs:
    dag_id = 'dag_from_json_{}'.format(conf['States']['ChoiceState']['Type'])
    schedule = '@daily'
    default_args = {'owner': 'airflow',
                    'depends_on_past': False,
                    'start_date': datetime(2019, 9, 22),
                    'email': ["airflow@airflow.com"],
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 1,
                    'retry_delay': timedelta(minutes=5),
                    # 'concurrency': 1,
                    'max_active_runs': 1
                    }

    globals()[dag_id] = create_dag(dag_id,
                                   schedule,
                                   default_args,
                                   conf)
