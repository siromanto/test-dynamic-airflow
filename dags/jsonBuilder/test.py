import os
import json


path = '/Users/siromanto/free/0.projects/test-dynamic-airflow/data'   #TODO: change it to relative


def test():
    configs = []
    for file in os.listdir(path):
        if file.endswith('.json'):
            print(file)
            path_file = path + '/' + file
            with open(path_file, 'r', encoding='utf-8') as json_data:
                conf = json.load(json_data)
                print(json_data)
                print(conf)
                configs.append(conf)


    return configs


def test_json():
    confs = test()

    for f in confs:
        print(f['States']['ChoiceState']['Type'])


if __name__ == '__main__':
    test_json()