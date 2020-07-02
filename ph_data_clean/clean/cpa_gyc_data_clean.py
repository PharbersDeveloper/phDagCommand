from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.model.clean_result import CleanResult, Tag


class CpaGycDataClean(DataClean):
    """
    CPA & GYC 等元数据的清洗规则
    """

    def cleaning_process(self, mapping: list, raw_data: dict) -> CleanResult:
        # standardise colunm name
        new_key_name = {}
        for raw_data_key in raw_data.keys():
            old_key = raw_data_key.split("#")[-1].strip()  # remove unwanted symbols
            # print(old_key)
            for m in mapping:
                # print(m["candidate"])
                # new_key_name[new_key] = None
                if old_key in m["candidate"]:
                    # print(1)
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

        # define tag
        for maps in mapping:
            if maps['not_null']:
                if final_data[maps['col_name']] is None:
                    # print(maps['col_name'])
                    tag_value = Tag.ERROR

        return CleanResult(final_data, {}, tag_value)

