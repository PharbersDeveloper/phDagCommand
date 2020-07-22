from ph_data_clean.model.clean_result import CleanResult, Tag
from enum import Enum


class SalesQtyTag(Enum):
    GRAIN = 'GRAIN'
    BOX = 'BOX'
    FULL = 'FULL'


def result_validation(*args, **kwargs):
    print('result_validation:mapping:' + str(args[0]))
    print('result_validation:raw_data:' + str(args[1]))
    print('result_validation:previou_data:' + str(kwargs))

    return CleanResult(data={},
                       metadata={},
                       raw_data={},
                       tag=Tag.SUCCESS,
                       err_msg='')


def check_format(*args, **kwargs):
    mapping = args[0]
    raw_data = args[1]
    return mapping, raw_data


def change_key(*args, **kwargs):
    raw_data = kwargs['check_format'][1]
    mapping = args[0]

    # standardise column name
    new_key_name = {}
    for raw_data_key in raw_data.keys():
        old_key = raw_data_key.split("#")[-1].replace('\n', '').strip()  # remove unwanted symbols
        for m in mapping:
            if old_key.lower() in [key.lower() for key in m["candidate"]]:
                new_key = m["col_name"]
                if new_key not in new_key_name:
                    new_key_name[new_key] = raw_data[raw_data_key]  # write new key name into dict

    # create ordered new dict
    final_data = {}
    for m in mapping:
        for n in new_key_name.keys():
            if m["col_name"] == n:
                final_data[m["col_name"]] = new_key_name[n]
            elif m["col_name"] not in final_data.keys():
                final_data[m["col_name"]] = None

    return final_data


def change_year_month(*args, **kwargs):
    final_data = kwargs['change_key']
    if final_data:
        input_year = kwargs['change_key']['YEAR']
        input_month = kwargs['change_key']['MONTH']

        flag = True
        if input_year not in [None, ""]:
            try:
                input_year = int(float(input_year))
                if len(str(input_year)) == 6:
                    output_month = input_year % 100  # month
                    output_year = (input_year - output_month) // 100  # year
                    flag = False
                elif len(str(input_year)) == 8:
                    date = input_year % 100  # date
                    year_month = (input_year - date) // 100  # year+month
                    output_month = year_month % 100  # month
                    output_year = (year_month - output_month) // 100  # year
                    flag = False
                else:
                    output_year = input_year
            except ValueError:
                output_year = input_year
        else:
            output_year = input_year

        if flag is True:
            if input_month not in [None, ""]:
                try:
                    input_month = int(float(input_month))
                    if len(str(input_month)) == 6:
                        output_month = input_month % 100  # month
                        output_year = (input_month - output_month) // 100  # year
                    elif len(str(input_month)) == 8:
                        date = input_month % 100  # date
                        year_month = (input_month - date) // 100  # year+month
                        output_month = year_month % 100  # month
                        output_year = (year_month - output_month) // 100  # year
                    else:
                        output_month = input_month
                        output_year = input_year
                except ValueError:
                    output_month = input_month
            else:
                output_month = input_month
                output_year = input_year

        final_data['YEAR'] = output_year
        final_data['MONTH'] = output_month

    return final_data


def change_sales_tag(*args, **kwargs):
    final_data = kwargs['change_year_month']
    if final_data:
        # TODO 整理销量情况
        if final_data['SALES_QTY_GRAIN'] not in [None, ""]:
            final_data['SALES_QTY_BOX'] = final_data['SALES_QTY_GRAIN']
            final_data['SALES_QTY_TAG'] = SalesQtyTag.GRAIN.value
        elif final_data['SALES_QTY_BOX'] not in [None, ""]:
            final_data['SALES_QTY_GRAIN'] = final_data['SALES_QTY_BOX']
            final_data['SALES_QTY_TAG'] = SalesQtyTag.BOX.value

    return final_data


def reformat_null(data_type):
    if data_type == "String":
        return ""
    elif data_type == "Double":
        return 0.0
    elif data_type == "Integer":
        return 0


def define_tag_err(*args, **kwargs):
    mapping = args[0]
    raw_data = args[1]
    final_data = kwargs['change_sales_tag']
    # print(mapping)
    i = kwargs['change_key']['SALES_QTY_TAG']
    j = kwargs['change_year_month']['SALES_QTY_TAG']
    k = kwargs['change_sales_tag']['SALES_QTY_TAG']
    print(i)
    print(j)
    print(k)
    for k in kwargs:
        print(k)

    if raw_data == {}:  # 若原始数据为空
        tag_value = Tag.EMPTY_DICT
        error_msg = 'Error message: empty raw_data'
    elif final_data == {}:  # 若最终字典没有内容
        tag_value = Tag.EMPTY_DICT
        error_msg = 'Error message: no mapping found'

    else:
        error_msg_flag = False
        error_msg = f'Error message: column missing-- '
        for maps in mapping:
            # 若某些必须有的列缺失数据
            if (maps['not_null']) and (final_data[maps['col_name']] is None):
                error_msg_flag = True
                tag_value = Tag.MISSING_COL
                error_msg += ' / ' + maps['col_name']
                final_data[maps['col_name']] = reformat_null(data_type=maps['type'])
                # continue

            elif (maps['not_null']) and (final_data[maps['col_name']] in ['', '/']) and (
                    maps['col_name'] != 'PRODUCT_NAME'):
                error_msg_flag = True
                tag_value = Tag.MISSING_COL
                error_msg += ' / rd_err: ' + maps['col_name']
                # continue

            elif (not maps['not_null']) and (final_data[maps['col_name']] is None):
                final_data[maps['col_name']] = reformat_null(data_type=maps['type'])

        if not error_msg_flag:
            tag_value = Tag.SUCCESS
            error_msg = 'Success'

    # return tag_value, error_msg
    print(tag_value)
    return CleanResult(data={},
                       metadata={},
                       raw_data={},
                       tag=tag_value,
                       err_msg=error_msg)


#
# def reformat_int(*args, **kwargs):
#     mapping = args[0]
#     raw_data = args[1]
#     final_data = kwargs['change_sales_tag']
#     tag_value = kwargs['define_tag_err'][0]
#     print(tag_value)
#     error_msg = kwargs['define_tag_err'][1]
#     # if final_data:
#     #     for m in mapping:
#     #         if (m['type'] == "Integer") and (final_data[m['col_name']] not in [None, ""]):
#     #             final_data[m['col_name']] = reformat_int(input_data=final_data[m['col_name']])
#
#     # return CleanResult(data=final_data,
#     #                    metadata={},
#     #                    raw_data=raw_data,
#     #                    tag=Tag.SUCCESS,
#     #                    err_msg=error_msg)
#     return CleanResult(data={},
#                        metadata={},
#                        raw_data={},
#                        tag=Tag.SUCCESS,
#                        err_msg='')
