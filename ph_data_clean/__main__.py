from ph_data_clean.clean.cleaner_factory import CleanerFactory
from ph_data_clean.model.mapping_factory import MappingFactory
from ph_data_clean.util.yaml_utils import load_by_file


def get_test_data(file):
    return load_by_file(file)


if __name__ == '__main__':
    mapping_path = '../file/ph_data_clean/mapping_table/'
    test_file = '../file/ph_data_clean/s3_test_data/CPA-倍特-test.yaml'

    test_data = get_test_data(test_file)[0]
    source = test_data[1]['providers'][1]
    company = test_data[1]['providers'][0]

    cleaner = CleanerFactory().get_specific_cleaner(source, company)
    mapping = MappingFactory(mapping_path).get_specific_mapping(source, company)

    result = cleaner.cleaning_process([col.to_dict() for col in mapping.cols], test_data[0])
    print(result)

