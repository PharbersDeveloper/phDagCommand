import json
from enum import Enum


class Tag(Enum):
    SUCCESS = 1
    EMPTY_DICT = 0
    MISSING_COL = -1
    PH_ERR = -9999


class CleanResult(object):
    """
    清洗结果
    """

    def __init__(self, data: dict, metadata: dict, tag: Tag, err_msg: str = ''):
        self.data = data
        self.metadata = metadata
        self.tag = tag
        self.err_msg = err_msg

    def __str__(self):
        result = {
            "data": self.data,
            "metadata": self.metadata,
            "tag": self.tag.value,
            "errMsg": self.err_msg
        }
        return json.dumps(result, ensure_ascii=False)
