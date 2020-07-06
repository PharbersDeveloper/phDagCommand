from abc import ABCMeta, abstractmethod
from ph_data_clean.model.clean_result import CleanResult


class DataClean(object, metaclass=ABCMeta):
    """
    清洗方法的父类
    """

    @abstractmethod
    def cleaning_process(self, mapping: list, raw_data: dict) -> CleanResult:
        pass
