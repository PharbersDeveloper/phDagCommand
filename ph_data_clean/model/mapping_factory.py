class MappingFactory(object):
    """
    匹配规则的生成工厂
    """

    all_mapping = {}

    def __init__(self):
        """
        初始化 all_mapping
        """
        pass

    def get_specific_mapping(self, source, company) -> dict:
        """
        根据源和公司获取特定的匹配规则

        :param source: 清洗的元数据类型
        :param company: 清洗的公司名称

        :return: [dict] 返回指定的匹配规则
        """
        pass

    def __load(self):
        """
        从 yaml 文件中加载 all_mapping
        """
        return self

    def parsist_to_yaml(self):
        """
        持久化工厂内数据到 yaml 文件中
        """
        pass

# class A(object):
#     def __init__(self):
#         self.a = 1
#         self.b = 2
#
#     def get(self):
#         return self.a + self.b
#
#
# # append_to_file(A(), "test.yaml")
#
# a = load_by_file('test.yaml')
# print(a)
# print(a.a)
# print(a.b)
# print(a.get())
