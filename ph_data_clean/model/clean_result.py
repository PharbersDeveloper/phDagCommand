class CleanResult(object):
    """
    清洗结果
    """

    def __init__(self, data: dict, metadata: dict, tag: str, err_msg: str):
        self.data = data
        self.metadata = metadata
        self.tag = tag
        self.err_msg = err_msg

