def cpa_gyc_mapping():
    return [
        {
            "col_name": "COMPANY",
            "col_desc": "数据公司",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "SOURCE",
            "col_desc": "数据来源",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "PROVINCE_NAME",
            "col_desc": "省份名",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "CITY_NAME",
            "col_desc": "城市名",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "PREFECTURE_NAME",
            "col_desc": "区县名",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "YEAR",
            "col_desc": "年份",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "QUARTER",
            "col_desc": "季度",
            "type": "String",  # "Integer", TODO 月份里有个是2018Q1
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "MONTH",
            "col_desc": "月份",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "HOSP_NAME",
            "col_desc": "医院名",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "HOSP_CODE",
            "col_desc": "医院编码",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "HOSP_LEVEL",
            "col_desc": "医院等级",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "ATC",
            "col_desc": "ATC编码",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "MOLE_NAME",
            "col_desc": "分子名",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "KEY_BRAND",
            "col_desc": "通用名",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "PRODUCT_NAME",
            "col_desc": "商品名",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "PACK",
            "col_desc": "包装",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "SPEC",
            "col_desc": "规格",
            "type": "String",
            "candidate": [],
            "not_null": True # TODO: 需要决策树
        },
        {
            "col_name": "DOSAGE",
            "col_desc": "剂型",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "PACK_QTY",
            "col_desc": "包装数量",
            "type": "String",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "SALES_QTY",
            "col_desc": "销量",
            "type": "Double",
            "candidate": [],
            "not_null": False
        },
        {
            "col_name": "SALES_VALUE",
            "col_desc": "销售额",
            "type": "Double",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "DELIVERY_WAY",
            "col_desc": "给药途径",
            "type": "String",
            "candidate": [],
            "NotNull": False
        },
        {
            "col_name": "MANUFACTURER_NAME",
            "col_desc": "生产厂商",
            "type": "String",
            "candidate": [],
            "not_null": True
        },
        {
            "col_name": "MKT",
            "col_desc": "所属市场",
            "type": "String",
            "candidate": [],
            "NotNull": False
        },
        {
            "col_name": "TAG",
            "col_desc": "文件标识",
            "type": "String",
            "candidate": [],
            "not_null": True
        }
    ]

