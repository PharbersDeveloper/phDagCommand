import os
import yaml
from numpy import nan

from ph_data_clean.script.s3_traverse import LOCAL_CACHE_DIR
from ph_data_clean.util.yaml_utils import load_by_file


def override_to_file(obj, file):
    """
    覆盖写入 yaml 文件
    """
    with open(file, 'w', encoding='UTF-8') as file:
        yaml.dump(obj, file, default_flow_style=False, encoding='utf-8', allow_unicode=True)


def simple_file_duplication(file):
    dup_lst = load_by_file(file)
    simple_lst = [eval(t) for t in set([str(d) for d in dup_lst])]
    override_to_file(simple_lst, file)


if __name__ == '__main__':
    """
    清理缓存文件中的重复数据
    """
    for sub in os.listdir(LOCAL_CACHE_DIR):
        file = LOCAL_CACHE_DIR + sub
        simple_file_duplication(file)
