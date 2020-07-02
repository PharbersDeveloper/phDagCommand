class ColCharactor(object):
    """
    对于每个单元格的匹配规则
    """

    def __init__(self, col_name: str, candidate: list, type: str, not_null: bool):
        self.col_name = col_name
        self.candidate = candidate
        self.type = type
        self.not_null = not_null

    def to_dict(self) -> dict:
        return self.__dict__


class DataMapping(object):
    """
    对于指定源的指定公司的匹配规则
    """
    def __init__(self, source: str, company: str, cols: list):
        self.source = source
        self.company = company
        self.cols = cols

    def to_dict(self) -> dict:
        self.__dict__['cols'] = [col.to_dict() for col in self.cols]
        return self.__dict__
