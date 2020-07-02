from ph_data_clean.clean.cleaner_factory import CleanerFactory
from ph_data_clean.model.mapping_factory import MappingFactory

if __name__ == '__main__':
    source = 'cpa'
    company = ''

    cleaner = CleanerFactory().get_specific_cleaner(source, company)
    mapping = MappingFactory().get_specific_mapping(source, company)

    cleaner.cleaning_process(mapping, {'测试数据key': '测试数据value'})
