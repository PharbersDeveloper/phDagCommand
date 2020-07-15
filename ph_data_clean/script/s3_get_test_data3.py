from ph_data_clean.util.yaml_utils import load_by_file
import yaml
import os

LOCAL_CACHE_DIR = r'../../file/ph_data_clean/s3_primitive_data/'
TEST_CACHE_DIR = r'../../file/ph_data_clean/s3_test_data/'




def get_test_data(s3_valid_data):
    """
    生成可用测试数据

    :param s3_valid_data: 爬取的s3非空数据 type = list
    :return: test_data_lst: 最终测试数据 type = list
    """
    test_data_lst = []
    for data in s3_valid_data:
        for i in (0, 1):
            test_data = {'data': {},
                         'metadata': {'fileName': data['file'],
                                      'providers': [data['company'], data['source']],
                                      'sheetName': data['sheet']}}
            for key in data['data'].keys():
                test_data['data'][key] = data['data'][key][i]

            # 加上tag
            test_data['data']['_Tag'] = 'yes'

            test_data_str = str(test_data['data'])
            test_data['data'] = test_data_str

            meta_data_str = str(test_data['metadata'])
            test_data['metadata'] = meta_data_str

            test_data_lst.append(test_data)
    return test_data_lst


def append_test_data(test_data_lst):
    """
    把可用数据写入指定名称的yaml文件

    :param test_data_lst: 拆开的可用数据
    """
    # print(os.listdir(TEST_CACHE_DIR))
    if sub.split('.')[0] + '-test.yaml' not in os.listdir(TEST_CACHE_DIR):
        with open(test_file, 'a', encoding='UTF-8') as file:
            yaml.dump(test_data_lst, file, default_flow_style=False, encoding='utf-8', allow_unicode=True)


if __name__ == '__main__':
    #     for sub in os.listdir(LOCAL_CACHE_DIR):
    sub = 'GYC-Astellas.yaml'
    file = LOCAL_CACHE_DIR + sub
    print('筛选：')
    print(file)
    test_file = TEST_CACHE_DIR + sub.split('.')[0] + '-test.yaml'
    print('存入：')
    print(test_file)
    file_lst = load_by_file(file)
    print(file_lst)
    s3_valid_data = file_lst
    test_data_lst = get_test_data(s3_valid_data)
    append_test_data(test_data_lst)
