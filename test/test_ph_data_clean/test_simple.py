import os
import pytest
from ph_data_clean.__main__ import clean
from ph_data_clean.util.yaml_utils import load_by_file
from ph_data_clean.model.clean_result import Tag

PROJEDT_NAME = 'phDagCommand'


@pytest.mark.skip("util")
def chdir():
    if not os.getcwd().endswith(PROJEDT_NAME):
        os.chdir('..')
        chdir()


chdir()


def test_all():
    test_file = r'file/ph_data_clean/s3_test_data/GYC&CPA-Lilly-test.yaml'
    test_datas = load_by_file(test_file)
    for test_data in test_datas:
        result = clean(test_data)
        if result.tag != Tag.SUCCESS:
            print(str(result))
        else:
            print('success')


test_all()
