import boto3
import yaml
import copy
import pandas as pd

TEST_FILE = r'test_data.yaml'

# print(yaml.load(TEST_FILE, Loader=yaml.FullLoader))

# fr = open('test_data.yaml', 'w')
# data = {'user_info': {'name': 'A', 'age': 17}}
# yaml.dump(data, fr)

def load_cache_data():
    """
     获取本地缓存的文件信息

     :return: cache_data_lst: 已经处理的文件信息
     """
    with open(TEST_FILE, encoding='UTF-8') as file:
        return yaml.load(file, Loader=yaml.FullLoader)


def append_cache_data(all_data_lst):
    """
    向文件中追加新解析的文件信息

    :param all_data_lst: 解析成功的文件列表
    """
    with open(TEST_FILE, 'a', encoding='UTF-8') as file:
        yaml.dump(all_data_lst, file, default_flow_style=False, encoding='utf-8', allow_unicode=True)


def get_s3_increment(cache_data_lst, all_data_lst):
    """
    获取 s3 上的增量文件，对于同名但重新上传的文件暂不支持重新处理

    :param cache_data_lst: 缓存的文件列表
    :param s3_all_file_lst: s3 上所有可解析的文件列表

    :return: increment_file_lst: 增量文件列表
    """
    # 利用 set 去重当前缓存的所有 file
    cache_file_set = set()
    for cache_data in cache_data_lst:
        cache_file_set.add(cache_data['name']+cache_data['age'])

    for data in all_data_lst:
        if data['name']+data['age'] in cache_file_set:
            print(data['name']+data['age'])
        else:
            print(data)
            append_cache_data([data])
#
#     for s3_file in s3_all_file_lst:
#         if s3_file['file'] in cache_file_set:
#             s3_all_file_lst.remove(s3_file)
#
#     return s3_all_file_lst


all_data_lst = [{'name': 'A', 'age': 'B', 'data': {'first': 1, 'second': 2}},
{'name': 'Aaa', 'age': 'Bbb', 'data': {'444': 1, '555': 2}},
{'name': 'AAAAAAA', 'age': 'BBBBB', 'data': {'first': 1, 'second': 2}},]
#
# append_cache_data(parse_file_lst)

cache_data_lst = load_cache_data()
print(cache_data_lst)

get_s3_increment(cache_data_lst, all_data_lst)
# cache_file_set = set()
# for cache_data in cache_data_lst:
#     cache_file_set.add(cache_data['age']+cache_data['name'])
# print(cache_file_set)
# print(len(cache_data_lst))

# for data in all_data_lst:
#     if data['name']+data['age'] == cache_data['file']+cache_data['name']:
#         print(data['name']+data['age'])