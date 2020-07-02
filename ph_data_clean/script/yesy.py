from ph_data_clean.util.yaml_utils import load_by_file
import yaml
import os

LOCAL_CACHE_DIR = r'../../file/ph_data_clean/s3_primitive_data/'
TEST_CACHE_DIR = r'../../file/ph_data_clean/s3_test_data/'
sub = 'CPA-Astellas.yaml'
file = LOCAL_CACHE_DIR + sub
print(file)
test_file = TEST_CACHE_DIR + sub.split('.')[0] + '-test.yaml'
print(test_file)
file_lst = load_by_file(test_file)
print(file_lst[1])

for key in file_lst[1][0].keys():
    print(key)
    # if '包装\n单位' in key:
    #     print(1)

