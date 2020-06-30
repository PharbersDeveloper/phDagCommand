from ph_data_clean.test.s3_traverse import load_cache_data, LOCAL_CACHE_FILE

s3_valid_data = [load_cache_data()[4], load_cache_data()[5], load_cache_data()[6]]


# print(s3_valid_data)
def get_s3_valid_data(s3_data):
    """
    筛选出s3非空数据

    :param s3_data: 爬取的s3所有数据 type = list
    :return: s3_valid_data: 爬取的s3非空数据 type = list
             null_data_name: 检测出的测试无法使用的空数据路径 type = list
    """
    null_data_lst = []
    null_data_name = []
    s3_valid_data = []
    flag = True
    for data in s3_data:
        for key in data['data'].keys():
            if not flag:
                continue

            if 'Unnamed' in key.split(':')[0]:
                null_data_name.append(data['file'])
                null_data_lst.append(data)
                flag = False
                continue
    print(null_data_lst)
    print(null_data_name)

    for data in s3_data:
        if data not in null_data_lst:
            s3_valid_data.append(data)
    print(s3_valid_data)
    return s3_valid_data, null_data_name


def get_test_data(s3_valid_data):
    """
    生成可用测试数据

    :param s3_valid_data: 爬取的s3非空数据 type = list
    :return: test_data_lst: 最终测试数据 type = list
    """
    test_data_lst = []
    for data in s3_valid_data:
        # print(data)
        for i in (0, 1):
            test_data = {'company': data['company'].strip("'"),
                         'source': data['source'].strip("'"),
                         'file_name': data['file'],
                         'sheet_name': data['sheet'],
                         'raw_data': {}}
            for key in data['data'].keys():
                test_data['raw_data'][key] = data['data'][key][i]
            test_data_lst.append(test_data)
            print(test_data)
    # print(test_data_lst)
    return test_data_lst


get_test_data(s3_valid_data)
