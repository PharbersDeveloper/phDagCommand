from ph_data_clean.script.s3_traverse import load_cache_data

s3_data = [load_cache_data()[4], load_cache_data()[5], load_cache_data()[6]]


def get_s3_valid_data(s3_data):
    """
    筛选出s3非空数据

    :param s3_data: 爬取的s3所有数据: type = list
    :return: s3_valid_data: 爬取的s3非空数据: type = list
             null_data_name: 检测出的测试无法使用的空数据路径和sheet name: type = list
    """
    null_data_lst = []
    null_data_name = []
    s3_valid_data = []

    for data in s3_data:
        if len(data['data']) <= 4:
            null_data_name.append(data['file'])
            null_data_name.append(data['sheet'])
            null_data_lst.append(data)
            continue
        flag = True
        for key in data['data'].keys():
            if not flag:
                continue
            if 'Unnamed' in key.split(':')[0]:
                null_data_name.append(data['file'])
                null_data_name.append(data['sheet'])
                null_data_lst.append(data)
                flag = False
                continue

    for data in s3_data:
        if data not in null_data_lst:
            s3_valid_data.append(data)

    return s3_valid_data, null_data_name


def get_test_data(s3_valid_data):
    """
    生成可用测试数据

    :param s3_valid_data: 爬取的s3非空数据 type = list
    :return: test_data_lst: 最终测试数据 type = list
    """
    test_data_lst = []
    for data in s3_valid_data:
        for i in (0, 1):
            test_data = {'company': data['company'].strip("'"),
                         'source': data['source'].strip("'"),
                         'file_name': data['file'],
                         'sheet_name': data['sheet'],
                         'raw_data': {}}
            for key in data['data'].keys():
                test_data['raw_data'][key] = data['data'][key][i]
            test_data_lst.append(test_data)
    return test_data_lst


if __name__ == '__main__':
    s3_valid_data, null_data_name = get_s3_valid_data(s3_data)
    test_data_lst = get_test_data(s3_valid_data)
    print(f"共有爬取原始数据 {len(s3_data)} 条")
    print()

    print(f"共有检测出的测试无法使用的空数据 {len(null_data_name)//2} 条，文件路径和sheet name如下：")
    print(null_data_name)
    print()

    print(f"生可用测试数据 {len(test_data_lst)} 条，信息如下：")
    print(test_data_lst)
