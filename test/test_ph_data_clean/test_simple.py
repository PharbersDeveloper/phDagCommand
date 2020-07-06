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
    mapping_path = r'file/ph_data_clean/mapping_table/'
    test_file = r'file/ph_data_clean/s3_test_data/CPA&GYC-Pfizer-test.yaml'
    test_datas = load_by_file(test_file)
    for test_data in test_datas:
        result = clean(mapping_path, test_data)
        assert result.tag == Tag.SUCCESS
test_all()
