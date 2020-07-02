from ph_data_clean.clean.cpa_gyc_data_clean import CpaGycDataClean


class CleanerFactory(object):
    """
    清洗算法的生成工厂
    """

    all_clean = {
        ('cpa', 'gyc'): CpaGycDataClean,
    }

    def get_specific_cleaner(self, source, company='') -> CpaGycDataClean:
        """
        根据源和公司获取特定的清洗算法

        :param source: 清洗的元数据类型
        :param company: 清洗的公司名称

        :return: [DataClean] 特定清洗算法
        """

        finded = [clean for clean in self.all_clean.items() if source.lower() in clean[0]]

        if len(finded) == 1:
            return finded[0][1]()
        elif len(finded) > 1:
            raise Exception("Find more Cleaner" + str(finded))
        else:
            raise Exception("Not find Cleaner")
