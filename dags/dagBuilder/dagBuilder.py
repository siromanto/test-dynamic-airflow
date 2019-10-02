import os
import json
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator


PROJECT_DIR = os.getcwd()
CONFIG_DB_KEY = 'extract_json_config'

# "*/5 * * * *"


def say_hello(task_name, task_args):

    if task_args.get('params').get('method') == 'method_one':
        run_method_one(task_name)
    else:
        run_method_two(task_name)


def run_method_one(name):
    print('*'*200)
    print('Hello, METHOD_ONE - {}'.format(name))
    print('*'*200)


def run_method_two(name):
    print('=' * 200)
    print('METHOD_TWO - {}'.format(name))
    print('=' * 200)


# file_path = os.path.join(PROJECT_DIR, 'dags/data')
file_name = os.path.join(PROJECT_DIR, 'dags/config_files/conf_1.yml')
# print(f'SOURCE_DIR ===> {file_name}')
path = Path(file_name)
print(f'path ===> {path}')


# Load the DAG configuration, setting a default if none is present
with path.open() as f:
    default_config = yaml.safe_load(f)
    # print(f'default_config = {default_config}')

default_config = json.loads(json.dumps(default_config, default=str))
# print(f'default_config 2 = {default_config}')

# config = Variable.setdefault(CONFIG_DB_KEY, default_config,
#                              deserialize_json=True)

config = default_config

# Create the DAG
with DAG(**config['dag']) as dag:
    tasks_dict = {}

    # Start the graph with a dummy task
    last_task = DummyOperator(task_id='start')

    tasks_dict['start'] = last_task

    # Extend the graph with a task for each new name
    print(f"TASKS --->{config['tasks']}")
    for task_name, task_args in config['tasks'].items():

        if task_args['task_operator'] == 'PythonOperator':
            task = PythonOperator(
                task_id='run_{}'.format(task_args['task_id']),
                python_callable=eval(task_args['callable_method']),
                op_args=(task_name, task_args)
            )
        elif task_args['task_operator'] == 'DockerOperator':
            task = DockerOperator(
                task_id=task_args['task_id'],
                image=task_args['image'],
                api_version=task_args['api_version'],
                auto_remove=task_args['auto_remove'],
                environment={
                        'AF_EXECUTION_DATE': task_args['environment']['af_execution_date'],
                        'AF_OWNER': task_args['environment']['af_owner']
                },
                command=task_args['command'],
                docker_url=task_args['docker_url'],
                network_mode=task_args['network_mode']
            )

        tasks_dict[task_name] = task

        if task_args.get('dependencies'):
            task.set_upstream(tasks_dict.get(task_args.get('dependencies')))
        else:
            # in case if we will have init task
            # last_task >> task
            last_task.set_downstream(task)


# Variable.delete(CONFIG_DB_KEY)