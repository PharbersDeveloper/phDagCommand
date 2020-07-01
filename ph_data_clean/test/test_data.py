from ph_data_clean.script.s3_traverse import load_cache_data

# s3_data = [load_cache_data()[4], load_cache_data()[5], load_cache_data()[6]]
s3_data = load_cache_data()[7:20]
TEST_FILE = r'test_data.yaml'


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
    global test_data_lst
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
        cache_file_set.add(cache_data['file_name'] + cache_data['sheet_name'])

    for data in all_data_lst:
        if data['file'] + data['sheet'] in cache_file_set:
            print(1)
        else:
            print(data['file'] + data['sheet'])
            # print(data)
            # append_cache_data([data])
            s3_valid_data, null_data_name = get_s3_valid_data([data])
            test_data_lst = get_test_data(s3_valid_data)
            if len(test_data_lst):
                append_cache_data(test_data_lst)


if __name__ == '__main__':
    # s3_valid_data, null_data_name = get_s3_valid_data(s3_data)
    # test_data_lst = get_test_data(s3_valid_data)

    # print(f"共有爬取原始数据 {len(s3_data)} 条")
    # print()
    #
    # print(f"共有检测出的测试无法使用的空数据 {len(null_data_name) // 2} 条，文件路径和sheet name如下：")
    # print(null_data_name)
    # print()
    #
    # print(f"生成可用测试数据 {len(test_data_lst)} 条，信息如下：")
    # print(test_data_lst)

    cache_data_lst = load_cache_data()
    # print(f"已有缓存可用测试数据 {len(cache_data_lst)} 条")
    # print("继续生成可用测试数据并缓存")
    # print(cache_data_lst)

    get_s3_increment(cache_data_lst, s3_data)
