import boto3
import os
import pandas as pd
# s3 = boto3.client('s3')

# 得到所有目录名+文件名
s3 = boto3.resource('s3')
bucket = s3.Bucket('ph-origin-files')
for obj in bucket.objects.all():
    key = obj.key  # 读取所有excel文件名
    source = os.path.dirname(key)
    # print(source)
    if source == source.split("/")[-1]:  # 第一层文件夹名字=source
        source_name = source
        filename = ''
    else:
        company_name = source.split("/")[-1]
        filename = os.path.basename(key)
        print('source = ' + source_name)
        print('company = ' + company_name)
        print('filename = ' + filename)

        if key.split(".")[-1] == 'xlsx':
            df_dict = {}  # 创建最终返回的字典

            # 读取s3数据储存到df里
            s3 = boto3.resource('s3')
            client = boto3.client('s3')
            resource = client.get_object(
                Bucket='ph-origin-files',
                Key=key
            )
            excel_data = pd.ExcelFile(resource['Body'].read())
            df = excel_data.parse(excel_data.sheet_names[0], nrows=2)

            # 生成一个字典（列名+第一行数据）
            for col in df.columns.values:
                df_dict[col] = df[col][0]
            print(df_dict)

    if filename == '':
        print('文件夹')