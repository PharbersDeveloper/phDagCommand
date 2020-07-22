from enum import Enum
from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.model.clean_result import CleanResult, Tag
from ph_data_clean.clean.common_func import *

class SalesQtyTag(Enum):
    GRAIN = 'GRAIN'
    BOX = 'BOX'
    FULL = 'FULL'


class CpaGycDataClean(DataClean):
    """
    CPA & GYC 等元数据的清洗规则
    """

    def reformat_null(self, data_type):
        if data_type == "String":
            return ""
        elif data_type == "Double":
            return 0.0
        elif data_type == "Integer":
            return 0

    def reformat_int(self, input_data):
        try:
            return int(float(input_data))
        except ValueError:
            return input_data

    def cleaning_process_old(self, mapping: list, raw_data: dict) -> CleanResult:
        # standardise colunm name
        global tag_value
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

        # 当字典不为空时 change year and month
        try:
            final_data_year = int(float(final_data['YEAR']))
        except:
            # isinstance(final_data['YEAR'], str) and final_data == {}
            final_data_year = None

        if final_data and isinstance(final_data_year, int):
            if len(str(final_data_year)) == 6:
                final_data['MONTH'] = final_data_year % 100  # month
                final_data['YEAR'] = (final_data_year - final_data['MONTH']) // 100  # year

        # TODO 整理销量情况
        if final_data['SALES_QTY_GRAIN'] is not None:
            final_data['SALES_QTY_BOX'] = final_data['SALES_QTY_GRAIN']
            final_data['SALES_QTY_TAG'] = SalesQtyTag.GRAIN.value

        # define tag and error message
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
                    final_data[maps['col_name']] = self.reformat_null(data_type=maps['type'])
                    continue

                elif (maps['not_null']) and (final_data[maps['col_name']] == '') and (
                        maps['col_name'] != 'PRODUCT_NAME'):
                    error_msg_flag = True
                    tag_value = Tag.MISSING_COL
                    error_msg += ' / rd_err: ' + maps['col_name']
                    continue

                elif (not maps['not_null']) and (final_data[maps['col_name']] is None):
                    final_data[maps['col_name']] = self.reformat_null(data_type=maps['type'])

            if not error_msg_flag:
                tag_value = Tag.SUCCESS
                error_msg = 'Success'

        # 年月改为int格式
        for m in mapping:
            if (m['type'] == "Integer") and (final_data[m['col_name']] not in [None, ""]):
                final_data[m['col_name']] = self.reformat_int(input_data=final_data[m['col_name']])

        return CleanResult(data=final_data,
                           metadata={},
                           raw_data=raw_data,
                           tag=tag_value,
                           err_msg=error_msg)

    def print_one(self, *args, **kwargs):
        print('print_one:mapping:' + str(args[0]))
        print('print_one:raw_data:' + str(args[1]))
        print('print_one:previou_data:' + str(kwargs))
        return 'print_one'

    def print_two(self, *args, **kwargs):
        print('print_two:mapping:' + str(args[0]))
        print('print_two:raw_data:' + str(args[1]))
        print('print_two:previou_data:' + str(kwargs))
        return 'print_two'

    process = [
        print_one,
        print_two,
        print_two,
        result_validation,
        result_validation,
    ]
