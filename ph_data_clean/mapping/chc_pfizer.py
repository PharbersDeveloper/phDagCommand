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
            "candidate": ["省份", "BIAOZHUNSHENFEN"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "CITY_NAME",
            "col_desc": "城市名",
            "candidate": ["城市", "CITY"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "PREFECTURE_NAME",
            "col_desc": "区县名",
            "candidate": ["区县"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "YEAR",
            "col_desc": "年份",
            "candidate": ["年", "Year", "年份", "JIAOYIRIQI"],
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
            "candidate": ["月份", "月"],
            "type": "Integer",
            "not_null": True,
        },
        {
            "col_name": "HOSP_NAME",
            "col_desc": "医院名",
            "candidate": ["医院名称", "医疗机构", "YIYUAN", "标准医院"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "HOSP_CODE",
            "col_desc": "医院编码",
            "candidate": ["HOSPITAL_ID"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "HOSP_LEVEL",
            "col_desc": "医院等级",
            "candidate": ["医院级别"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "HOSP_TYPE",
            "col_desc": "医院类型",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "HOSP_LIBRARY_TYPE",
            "col_desc": "数据库类型",
            "candidate": ["医院库类型"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "HOSP_REGION_TYPE",
            "col_desc": "医院区域类型",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MOLE_NAME",
            "col_desc": "分子名",
            "candidate": ["化学名", "匹配名", "TONGYONGMING"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PRODUCT_NAME",
            "col_desc": "商品名",
            "candidate": ["商品名"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SPEC",
            "col_desc": "规格",
            "candidate": ["规格", "GUIGE"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "DOSAGE",
            "col_desc": "剂型",
            "candidate": ["剂型", "JIXING"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PACK_QTY",
            "col_desc": "包装数量/价格转换比",
            "candidate": ["价格转换比", "CONVERT"],
            "type": "Integer",
            "not_null": True,
        },
        {
            "col_name": "PACK_UNIT",
            "col_desc": "包装单位",
            "candidate": ["包装"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "SALES_QTY_GRAIN",
            "col_desc": "粒度销量",
            "candidate": ["最小使用单位数量", "最小使用单位数量之合计"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "SALES_QTY_BOX",
            "col_desc": "盒装销量",
            "candidate": ["数量", "CAIGOUSHULIANG"],
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
            "candidate": ["金额", "采购金额", "金额之合计", "CAIGOUJINE"],
            "type": "String",
            "not_null": True,
        },
        {
            "col_name": "PRICE",
            "col_desc": "片盒单价",
            "candidate": ["价格", "CAIGOUJIAGE"],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "DELIVERY_WAY",
            "col_desc": "给药途径",
            "candidate": [],
            "type": "String",
            "not_null": False,
        },
        {
            "col_name": "MANUFACTURER_NAME",
            "col_desc": "生产厂商",
            "candidate": ["生产企业", "SHENGCHANQIYE"],
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
