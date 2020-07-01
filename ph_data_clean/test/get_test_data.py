import boto3
import yaml
import copy
import pandas as pd

TEST_FILE = r'test_data.yaml'

with open(TEST_FILE, encoding='UTF-8') as file:
    return yaml.load(file, Loader=yaml.FullLoader)