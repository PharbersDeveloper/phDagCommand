from pypinyin import lazy_pinyin


class MappingFactory(object):
    """
    匹配规则的生成工厂
    """

    def get_specific_mapping(self, source, company) -> list:
        """
        根据源和公司获取特定的匹配规则

        :param source: 清洗的元数据类型
        :param company: 清洗的公司名称

        :return: [dict] 返回指定的匹配规则
        """
        source = source.replace("&", "_")
        company = "".join(lazy_pinyin(company))
        ipt_module = __import__(f'ph_data_clean.mapping.{source.lower()}_{company.lower()}')
        mapping_pkg = getattr(ipt_module, 'mapping')
        mapping_file = getattr(mapping_pkg, f'{source.lower()}_{company.lower()}')
        mapping_table = getattr(mapping_file, 'mapping')
        return mapping_table()

