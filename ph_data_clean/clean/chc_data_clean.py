from enum import Enum
from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.model.clean_result import CleanResult, Tag
import re


class SalesQtyTag(Enum):
    GRAIN = 'GRAIN'
    BOX = 'BOX'
    FULL = 'FULL'


class ChcDataClean(DataClean):
    """
    CHC 元数据的清洗规则
    """

    def pack_qty_unit(self, final_data_pack_unit):
        """
        将包装单位（数字+单位）转化成纯数字的价格转换比（pack_qty）

        :param final_data_pack_unit: 清洗后的包装单位数值

        :return: 纯数字价格转换比

        """
        pack_qty = re.findall(r"\d+", final_data_pack_unit)
        try:
            return int(pack_qty[0])
        except IndexError:
            return 0

    def reformat_null(self, data_type):
        """
        对mapping中规定的原始数据类型为int的情况规范final_data数据格式

        :param data_type: mapping中规定的每一列的数据类型

        :return: 默认值

        """
        if data_type == "String":
            return ""
        elif data_type == "Double":
            return 0.0
        elif data_type == "Integer":
            return 0

    def reformat_int(self, input_data):
        """
        对mapping中规定的原始数据类型为int的情况规范final_data数据格式

        :param input_data: 原来final_data中数据（格式可能不是int）

        :return: 转换为int的数据

        """
        try:
            return int(float(input_data))
        except ValueError:
            return input_data

    def cleaning_process(self, mapping: list, raw_data: dict) -> CleanResult:
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
        if final_data['SALES_QTY_GRAIN'] not in [None, ""]:
            final_data['SALES_QTY_BOX'] = final_data['SALES_QTY_GRAIN']
            final_data['SALES_QTY_TAG'] = SalesQtyTag.GRAIN.value
        elif final_data['SALES_QTY_BOX'] not in [None, ""]:
            final_data['SALES_QTY_GRAIN'] = final_data['SALES_QTY_BOX']
            final_data['SALES_QTY_TAG'] = SalesQtyTag.BOX.value

        # 价格转换比和包装单位整理
        if final_data['PACK_QTY'] in [None, ""] and final_data['PACK_UNIT'] not in [None, ""]:
            print(1)
            final_data['PACK_QTY'] = self.pack_qty_unit(final_data_pack_unit=final_data['PACK_UNIT'])

        # 医院编码和医院名称存在一个即可
        if final_data['HOSP_CODE'] is None and final_data['HOSP_NAME'] not in [None, ""]:
            final_data['HOSP_CODE'] = "无"
        elif final_data['HOSP_NAME'] is None and final_data['HOSP_CODE'] not in [None, ""]:
            # print(final_data['HOSP_NAME'])
            final_data['HOSP_NAME'] = "无"

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
