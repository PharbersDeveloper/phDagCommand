from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.model.clean_result import CleanResult


class CpaGycDataClean(DataClean):
    """
    CPA & GYC 等元数据的清洗规则
    """

    def cleaning_process(self, mapping: list, raw_data: dict) -> CleanResult:
        print(mapping)
        print(raw_data)
        pass
