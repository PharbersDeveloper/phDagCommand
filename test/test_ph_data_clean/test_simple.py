import os
import pytest
from ph_data_clean.__main__ import clean
from ph_data_clean.util.yaml_utils import load_by_file
from ph_data_clean.model.clean_result import Tag

PROJECT_NAME = 'phdagcommand'


@pytest.mark.skip("util")
def chdir():
    if not os.getcwd().endswith(PROJECT_NAME):
        os.chdir('..')
        chdir()


chdir()


def test_all():
    test_file = r'file/ph_data_clean/s3_test_data/CPA-Pfizer-test.yaml'
    test_datas = load_by_file(test_file)
    for test_data in test_datas[0:1]:
        result = clean(test_data)
        if result.tag != Tag.SUCCESS:
            print()
            print(str(result))
        else:
            print()
            print('success  ', result)


test_all()
