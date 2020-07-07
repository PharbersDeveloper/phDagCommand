import os
from ph_data_clean.model.data_mapping import ColCharactor
from ph_data_clean.model.data_mapping import DataMapping
from ph_data_clean.util.yaml_utils import load_by_dir, override_to_file, load_by_file
from pherrs.ph_err import PhError


class MappingFactory(object):
    """
    匹配规则的生成工厂
    """

    all_mapping = []

    def __init__(self, mapping_path):
        """
        初始化 all_mapping
        """
        self.mapping_path = mapping_path
        self.all_mapping = self.__load()

    def get_specific_mapping(self, source, company) -> DataMapping:
        """
        根据源和公司获取特定的匹配规则

        :param source: 清洗的元数据类型
        :param company: 清洗的公司名称

        :return: [dict] 返回指定的匹配规则
        """
        finded = [mapping for mapping in self.all_mapping
                  if source.lower() == mapping.source.lower()
                  and company.lower() == mapping.company.lower()]

        if len(finded) == 1:
            return finded[0]
        elif len(finded) > 1:
            raise PhError("Find more Mapping" + str(finded))
        else:
            raise PhError(f"Not find Mapping, source={source}, company={company}")

    def __load(self):
        """
        从 yaml 文件中加载 all_mapping
        """
        if os.path.isdir(self.mapping_path):
            return load_by_dir(self.mapping_path)
        else:
            return load_by_file(self.mapping_path)

    def parsist_to_yaml(self):
        """
        持久化工厂内数据到 yaml 文件中
        """
        if not self.mapping_path.endswith("/"):
            self.mapping_path = self.mapping_path + '/'

        for mapping in self.all_mapping:
            path = f'{self.mapping_path}{mapping.source}-{mapping.company}.yaml'
            override_to_file(mapping, path)
