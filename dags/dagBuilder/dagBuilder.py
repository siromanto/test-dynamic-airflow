import os
import json
from pathlib import Path

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


PROJECT_DIR = os.getcwd()
CONFIG_DB_KEY = 'extract_json_config'

# "*/5 * * * *"


def say_hello(task_name, task_args):
    print('*'*200)
    print('Hello, {}'.format(task_name))
    print('*'*200)




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

# print(f'CONFIG NEW DAG ====> {config}')
# print(f"**config['dag'] ===> {config['dag']}")




# Create the DAG
with DAG(**config['dag']) as dag:
    tasks_dict = {}

    # Start the graph with a dummy task
    last_task = DummyOperator(task_id='start')

    tasks_dict['start'] = last_task

    # Extend the graph with a task for each new name
    print(f"TASKS --->{config['tasks']}")
    for task_name, task_args in config['tasks'].items():

        task = PythonOperator(
            task_id='run_{}'.format(task_args['task_id']),
            python_callable=eval(task_args['callable_method']),
            op_args=(task_name, task_args)
        )

        tasks_dict[task_name] = task

        if task_args.get('dependencies'):
            task.set_upstream(tasks_dict.get(task_args.get('dependencies')))
        else:
            # in case if we will have init task
            # last_task >> task
            last_task.set_downstream(task)


# Variable.delete(CONFIG_DB_KEY)