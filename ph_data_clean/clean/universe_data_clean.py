from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.clean.common_func import *
import copy
from ph_data_clean.mapping.universe_mapping import *


class UniverseDataClean(DataClean):
    """
    universe 源数据的清洗规则
    """

    def change_key_for_blue(self, *args, **kwargs):
        mapping = args[0]
        raw_data = kwargs['prev']

        # standardise column name
        new_key_name = {}
        for raw_data_key in raw_data.keys():
            old_key = raw_data_key.strip()  # remove unwanted symbols
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
        final_data["UPDATE_LABEL"] = '2013_updated'

        return final_data

    def change_key_for_grey_orange(self, *args, **kwargs):
        raw_data = args[1]
        final_data_blue = kwargs['prev']
        final_data_for_three = []
        final_data_gery = copy.deepcopy(final_data_blue)
        final_data_orange = copy.deepcopy(final_data_blue)

        blue_grey_mapping = universe_blue_grey_mapping()
        blue_orange_mapping = universe_blue_orange_mapping()

        # 基于蓝色数据改2011年数据
        for m in blue_grey_mapping:
            final_data_gery[m["blue_col_name"]] = raw_data[m['grey_col_name']]
        final_data_gery["UPDATE_LABEL"] = '2011_initial'

        # 基于蓝色数据改2019年数据
        for m in blue_orange_mapping:
            final_data_orange[m["blue_col_name"]] = raw_data[m['orange_col_name']]
        final_data_orange["UPDATE_LABEL"] = '2019_updated'

        final_data_for_three.append(final_data_blue)  # 2013
        final_data_for_three.append(final_data_gery)  # 2011
        final_data_for_three.append(final_data_orange)  # 2019

        return final_data_for_three

    def define_tag_err_for_three(self, *args, **kwargs):
        final_data_for_three = kwargs['prev']

        for final_data in final_data_for_three:
            print(final_data['UPDATE_LABEL'])

        return CleanResult(data={},
                           metadata={},
                           raw_data={},
                           tag=Tag.SUCCESS,
                           err_msg="error_msg"),

    process = [
        check_format,
        change_key_for_blue,
        change_key_for_grey_orange,
        define_tag_err_for_three,
    ]
