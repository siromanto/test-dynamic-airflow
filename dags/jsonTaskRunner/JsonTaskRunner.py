from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.docker_operator import DockerOperator

import json
import os

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'start_date': datetime(2019, 9, 25),
                'email': ["airflow@airflow.com"],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                # 'concurrency': 1,
                'max_active_runs': 1
                }

dag_name = 'jsonTaskRunner'
PROJECT_DIR = os.getcwd()
dag_configs = []
files_path = os.path.join(PROJECT_DIR, 'dags/data')
print(f'SOURCE_DIR ===> {files_path}')

try:
    for file in os.listdir(files_path):
        if file.endswith('.json'):
            print('*' * 200)
            print(file)
            path_file = files_path + '/' + file
            with open(path_file, 'r', encoding='utf-8') as json_data:
                conf = json.load(json_data)
                dag_configs.append(conf)

except Exception as e:
    raise ValueError('There is no correct json-files...')


def create_dag(name, args, configs):
    dag = DAG(
        name,
        default_args=args,
        schedule_interval='@daily'
    )

    def init():
        print('=' * 200)
        print('INIT FUNCTION ---->')
        print('=' * 200)

    def run_python_task(conf, **kwargs):
        print('*' * 200)
        print('PYTHON TASK ----->')
        # print(kwargs)
        print('*' * 200)

    def run_docker_task(conf, **kwargs):
        print('*' * 200)
        print('DOCKER TASK ----->')
        print('*' * 200)

    with dag:
        init = PythonOperator(
            task_id='check_config_files',
            python_callable=init,
            dag=dag
        )

        clear = DummyOperator(
            task_id='clear',
            dag=dag
        )

        for conf in configs:
            execute_python = PythonOperator(
                task_id='execute_python_{}'.format(conf['States']['ChoiceState']['Type']),
                python_callable=run_python_task,
                op_args=[conf],
                dag=dag
            )

            execute_docker = PythonOperator(
                task_id='execute_docker_{}'.format(conf['States']['ChoiceState']['Type']),
                python_callable=run_docker_task,
                op_args=[conf],
                dag=dag
            )

            init >> execute_python >> execute_docker >> clear

        return dag


globals()[dag_name] = create_dag(dag_name, default_args, dag_configs)
