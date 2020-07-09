from enum import Enum
from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.model.clean_result import CleanResult, Tag


class SalesQtyTag(Enum):
    GRAIN = 'GRAIN'
    BOX = 'BOX'
    FULL = 'FULL'


class CpaGycDataClean(DataClean):
    """
    CPA & GYC 等元数据的清洗规则
    """

    def cleaning_process(self, mapping: list, raw_data: dict) -> CleanResult:
        # standardise colunm name
        new_key_name = {}
        for raw_data_key in raw_data.keys():
            old_key = raw_data_key.split("#")[-1].replace('\n', '').strip()  # remove unwanted symbols
            for m in mapping:
                if old_key.lower() in [key.lower() for key in m["candidate"]]:
                    new_key = m["col_name"]
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
            final_data_year = int(final_data['YEAR'])
        except:
            # isinstance(final_data['YEAR'], str) and final_data == {}
            final_data_year = None

        if final_data and isinstance(final_data_year, int):
            if len(str(final_data_year)) == 6:
                final_data['MONTH'] = final_data_year % 100  # month
                final_data['YEAR'] = (final_data_year - final_data['MONTH']) // 100  # year
            elif len(str(final_data_year)) == 8:
                date = final_data_year % 100  # date
                year_month = (final_data_year - date) // 100  # year+month
                final_data['MONTH'] = year_month % 100  # month
                final_data['YEAR'] = (year_month - final_data['MONTH']) // 100  # year
            else:
                pass

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
            error_msg = f'Error message: column missing - '
            for maps in mapping:
                # 若某些必须有的列缺失数据
                if (maps['not_null']) and (final_data[maps['col_name']] is None):
                    error_msg_flag = True
                    tag_value = Tag.MISSING_COL
                    error_msg += ' / ' + maps['col_name']
                    continue

            if not error_msg_flag:
                tag_value = Tag.SUCCESS
                error_msg = 'Success'

        return CleanResult(final_data, {}, tag_value, error_msg)
