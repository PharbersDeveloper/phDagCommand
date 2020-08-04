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
            "candidate": ["Province"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "CITY_NAME",
            "col_desc": "城市名",
            "candidate": ["City"],
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
            "candidate": ["Period"],
            "type": "Integer",
            "not_null": True,
        },
        {
            "col_name": "QUARTER",
            "col_desc": "季度",
            "candidate": ["季度"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MONTH",
            "col_desc": "月份",
            "candidate": ["MONTH"],
            "type": "Integer",
            "not_null": True,
        },
        {
            "col_name": "HOSP_NAME",
            "col_desc": "医院名",
            "candidate": ["Hospital Name", "Hospital_Name"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "HOSP_CODE",
            "col_desc": "医院编码",
            "candidate": ["Hospital Code", "Hospital_Code"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "HOSP_LEVEL",
            "col_desc": "医院等级",
            "candidate": ["Hospital Degree", "Hospital_Degree"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "ATC",
            "col_desc": "ATC编码",
            "candidate": ["通用名ATC编码", 'ATC4编码'],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MOLE_NAME",
            "col_desc": "分子名",
            "candidate": ["Molecule Name", "Molecule_Name"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PRODUCT_NAME",
            "col_desc": "商品名",
            "candidate": ["Product Name", "Product_Name"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SPEC",
            "col_desc": "规格",
            "candidate": ["Pack"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "DOSAGE",
            "col_desc": "剂型",
            "candidate": ["Dosage"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PACK_QTY",
            "col_desc": "包装数量",
            "candidate": ["PackNumber"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SALES_QTY_GRAIN",
            "col_desc": "粒度销量",
            "candidate": ["最小制剂单位数量"],
            "type": "String",
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
            "candidate": ["金额"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "DELIVERY_WAY",
            "col_desc": "给药途径",
            "candidate": ["ROAD"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MANUFACTURER_NAME",
            "col_desc": "生产厂商",
            "candidate": ["CORPORATION"],
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