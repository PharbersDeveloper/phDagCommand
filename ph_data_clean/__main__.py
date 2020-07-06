from ph_data_clean.clean.cleaner_factory import CleanerFactory
from ph_data_clean.model.mapping_factory import MappingFactory
from ph_data_clean.util.yaml_utils import load_by_file


def get_test_data(file):
    return load_by_file(file)


def main(mp, td):
    source = td[1]['providers'][1]
    company = td[1]['providers'][0]

    cleaner = CleanerFactory().get_specific_cleaner(source, company)
    mapping = MappingFactory(mp).get_specific_mapping(source, company)

    result = cleaner.cleaning_process([col.to_dict() for col in mapping.cols], td[0])
    print(result)
    print(result.data)
    # print(result.metadata)
    print(result.tag)
    print(result.err_msg)


if __name__ == '__main__':
    mapping_path = '../file/ph_data_clean/mapping_table/'
    test_file = '../file/ph_data_clean/s3_test_data/GYC&CPA-Sankyo-test.yaml'
    test_data = get_test_data(test_file)[0]

    main(mapping_path, test_data)

