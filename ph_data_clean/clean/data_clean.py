from abc import ABCMeta
from ph_data_clean.model.clean_result import CleanResult


class DataClean(object, metaclass=ABCMeta):
    """
    清洗方法的父类
    """

    process = []

    def cleaning_process(self, *args, **kwargs) -> CleanResult:
        runtime_class_name = self.__class__.__name__

        func_result = None
        for func in self.process:
            # 类方法调用方式，需要传递 self
            if runtime_class_name in str(func):
                func_result = func(self, *args, **kwargs)
            # 函数调用方式
            else:
                func_result = func(*args, **kwargs)

            if func_result:
                kwargs[func.__name__] = func_result
        return func_result
