def mapping():
    return [
        {
            "col_name": "COMPANY",
            "col_desc": "数据公司",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "SOURCE",
            "col_desc": "数据来源",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "TAG",
            "col_desc": "文件标识",
            "candidate": ['_TAG'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PROVINCE_NAME",
            "col_desc": "省份名",
            "candidate": ['省', '省/自治区/直辖市', '省份'],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "CITY_NAME",
            "col_desc": "城市名",
            "candidate": ['城市'],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "PREFECTURE_NAME",
            "col_desc": "区县名",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "YEAR",
            "col_desc": "年份",
            "candidate": ['年'],
            "type": "Integer",
            "not_null": True,
        },
        {
            "col_name": "QUARTER",
            "col_desc": "季度",
            "candidate": ['季度'],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MONTH",
            "col_desc": "月份",
            "candidate": ['月'],
            "type": "Integer",
            "not_null": True,
        },
        {
            "col_name": "HOSP_NAME",
            "col_desc": "医院名",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "HOSP_CODE",
            "col_desc": "医院编码",
            "candidate": ['医院编码'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "HOSP_LEVEL",
            "col_desc": "医院等级",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "ATC",
            "col_desc": "ATC编码",
            "candidate": ['ATC编码'],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MOLE_NAME",
            "col_desc": "分子名",
            "candidate": ['通用名', '通用名_中文'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PRODUCT_NAME",
            "col_desc": "商品名",
            "candidate": ['药品商品名', '商品名_中文'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SPEC",
            "col_desc": "规格",
            "candidate": ['规格'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "DOSAGE",
            "col_desc": "剂型",
            "candidate": ['剂型'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PACK_QTY",
            "col_desc": "包装数量",
            "candidate": ['包装规格'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SALES_QTY_GRAIN",
            "col_desc": "粒度销量",
            "candidate": ['最小制剂单位数量'],
            "type": "Double",
            "not_null": True,
        },
        {
            "col_name": "SALES_QTY_BOX",
            "col_desc": "盒装销量",
            "candidate": [],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SALES_QTY_TAG",
            "col_desc": r"销量标识(GRAIN \ BOX \ FULL)",
            "candidate": [],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SALES_VALUE",
            "col_desc": "销售额",
            "candidate": ['金额_元_', '金额(元)', '金额'],
            "type": "Double",
            "not_null": True,
        },
        {
            "col_name": "DELIVERY_WAY",
            "col_desc": "给药途径",
            "candidate": ['给药途径'],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MANUFACTURER_NAME",
            "col_desc": "生产厂商",
            "candidate": ['生产企业'],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "MKT",
            "col_desc": "所属市场",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
    ]
