import boto3
import os
import pandas as pd

BUCKER_NAME = 'ph-origin-files'
filter_dir = ['OTHERS']

s3 = boto3.resource('s3')


# 得到所有目录名+文件名
def get_all_file_path():
    """
    获取指定桶中的所有文件信息
    :return: [[filter_file_lst], [all_file_path], [err_file_path]]
        filter_file_lst: 被过滤掉的文件信息
        all_file_path: 可爬取的文件信息
        err_file_path: 无法解析路径的文件信息
    """
    filter_file_lst = []
    all_file_lst = []
    err_file_lst = []

    bucket = s3.Bucket(BUCKER_NAME)
    for obj in bucket.objects.all():
        obj_key = obj.key
        obj_key_lst = obj_key.split("/")

        # 目录过滤掉
        if obj_key.endswith("/"):
            continue

        # 不需要的目录过滤掉
        if obj_key_lst[0] in filter_dir:
            filter_file_lst.append(obj_key)
            continue

        # 正常处理获取文件信息
        if len(obj_key_lst) == 3 and obj_key_lst[2].endswith(".xlsx") or obj_key_lst[2].endswith(".xls"):
            all_file_lst.append({
                "source": repr(obj_key_lst[0]),
                "company": repr(obj_key_lst[1]),
                "file": repr(obj_key_lst[2]),
            })
            continue

        # 如果到这里，证明文件路径问题，加入错误流程
        err_file_lst.append(obj_key)

    return filter_file_lst, all_file_lst, err_file_lst


filter_file_lst, all_file_lst, err_file_lst = get_all_file_path()
if len(filter_file_lst):
    print(f"存在过滤掉的文件 {len(filter_file_lst)} 个，信息如下：")
    print(filter_file_lst)

if len(err_file_lst):
    print(f"存在无法解析路径的文件 {len(err_file_lst)} 个，信息如下：")
    print(err_file_lst)

# for obj in bucket.objects.all():
#     key = obj.key  # 读取所有excel文件名
#     source = os.path.dirname(key)
#     # print(source)
#     if source == source.split("/")[-1]:  # 第一层文件夹名字=source
#         source_name = source
#         filename = ''
#     else:
#         company_name = source.split("/")[-1]
#         filename = os.path.basename(key)
#         print('source = ' + source_name)
#         print('company = ' + company_name)
#         print('filename = ' + filename)
#
#         if key.split(".")[-1] == 'xlsx':
#             df_dict = {}  # 创建最终返回的字典
#
#             # 读取s3数据储存到df里
#             s3 = boto3.resource('s3')
#             client = boto3.client('s3')
#             resource = client.get_object(
#                 Bucket='ph-origin-files',
#                 Key=key
#             )
#             excel_data = pd.ExcelFile(resource['Body'].read())
#             df = excel_data.parse(excel_data.sheet_names[0], nrows=2)
#
#             # 生成一个字典（列名+第一行数据）
#             for col in df.columns.values:
#                 df_dict[col] = df[col][0]
#             print(df_dict)
#
#     if filename == '':
#         print('文件夹')
