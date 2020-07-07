from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.model.clean_result import CleanResult, Tag


class CpaGycDataClean(DataClean):
    """
    CPA & GYC 等元数据的清洗规则
    """

    def cleaning_process(self, mapping: list, raw_data: dict) -> CleanResult:
        # standardise colunm name
        # print(raw_data)
        new_key_name = {}
        for raw_data_key in raw_data.keys():
            old_key = raw_data_key.split("#")[-1].replace('\n', '').strip()  # remove unwanted symbols
            for m in mapping:
                if old_key in m["candidate"]:
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
        # print(final_data)
        if final_data and final_data['YEAR']:
            if len(final_data['YEAR']) == 6:
                final_data['MONTH'] = int(final_data['YEAR']) % 100  # month
                final_data['YEAR'] = (int(final_data['YEAR']) - final_data['MONTH']) // 100  # year
            elif len(final_data['YEAR']) == 8:
                date = int(final_data['YEAR']) % 100  # date
                year_month = (int(final_data['YEAR']) - date) // 100  # year+month
                final_data['MONTH'] = year_month % 100  # month
                final_data['YEAR'] = (year_month - final_data['MONTH']) // 100  # year
            else:
                pass

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
