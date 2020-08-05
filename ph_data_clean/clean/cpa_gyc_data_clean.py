# -*- coding: utf-8 -*-

from ph_data_clean.clean.data_clean import DataClean
from ph_data_clean.clean.common_func import *


class CpaGycDataClean(DataClean):
    """
    CPA & GYC 等源数据的清洗规则
    """

    process = [
        check_format,
        change_key,
        change_year_month,
        change_sales_tag,
        reformat_int,
        define_tag_err,
    ]
