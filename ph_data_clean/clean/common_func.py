from ph_data_clean.model.clean_result import CleanResult, Tag


def result_validation(*args, **kwargs):
    print('result_validation:mapping:' + str(args[0]))
    print('result_validation:raw_data:' + str(args[1]))
    print('result_validation:previou_data:' + str(kwargs))

    return CleanResult(data={},
                       metadata={},
                       raw_data={},
                       tag=Tag.SUCCESS,
                       err_msg='')
