import os
from ph_data_clean.__main__ import main as clean_main
from ph_data_clean.util.yaml_utils import load_by_dir, load_by_file
from ph_data_clean.model.clean_result import Tag

def get_test_data(path):
    return [k for i in load_by_dir(path) for k in i]


def test_all():
    mapping_path = '../../file/ph_data_clean/mapping_table/'
    test_file = '../../file/ph_data_clean/s3_test_data/'
    test_datas = get_test_data(test_file)
    for test_data in test_datas:
        result = clean_main(mapping_path, test_data)
        assert result.tag == Tag.SUCCESS
