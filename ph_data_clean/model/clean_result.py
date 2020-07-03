from enum import Enum


class Tag(Enum):
    SUCCESS = 1
    MISSING_COL = -1
    EMPTY_DICT = 0


class CleanResult(object):
    """
    清洗结果
    """

    def __init__(self, data: dict, metadata: dict, tag: Tag, err_msg: str = ''):
        self.data = data
        self.metadata = metadata
        self.tag = tag
        self.err_msg = err_msg
