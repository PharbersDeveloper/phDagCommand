def cpa_mapping():
    return [
        {
            "ColName": "COMPANY",
            "ColDesc": "数据公司",
            "Type": "String",
            "Candidate": [],
            "NotNull": True
        },
        {
            "ColName": "SOURCE",
            "ColDesc": "数据来源",
            "Type": "String",
            "Candidate": ["数据来源"],
            "NotNull": True
        },
        {
            "ColName": "YEAR",
            "ColDesc": "年份",
            "Type": "String",
            "Candidate": ["年月", 'year', 'YEAR', '年'],
        },
        {
            "ColName": "MONTH",
            "ColDesc": "月份",
            "Type": "String",
            "Candidate": ["月", 'month', 'MONTH'],
        },
        {
            "ColName": "PROVINCE_NAME",
            "ColDesc": "省份名",
            "Type": "String",
            "Candidate": ["省", "省份", "省/自治区/直辖市", "PROVINCE", "PROVINCES",
                          "PROVINCE_NAME"],
        },
        {
            "ColName": "CITY_NAME",
            "ColDesc": "市名",
            "Type": "String",
            "Candidate": ["市", "城市", "city"],
        },
        {
            "ColName": "PREFECTURE_NAME",
            "ColDesc": "区县名",
            "Type": "String",
            "Candidate": ["区县"]
        },
        {
            "ColName": "HOSP_NAME",
            "ColDesc": "医院名",
            "Type": "String",
            "Candidate": ["LILLY_HSPTL_NAME", "医院名称", "HOSPITAL NAME", "HOSPITAL",
                          "HOSPITAL.NAME", "HOSNAME", "HOSPITAL_NAME", "医院"],
            # "NotNull": True
        },
        {
            "ColName": "HOSP_CODE",
            "ColDesc": "医院编码",
            "Type": "String",
            "Candidate": ["医院编码", "VEEVA_CUSTOMER_ID", "HOSP_ID", "HOSPITAL CODE", "CODE",
                          "HOSPITAL.CODE", "HOSPITAL_CODE", "医院.代码", "UCBHOSCODE",
                          "HOSCODE", "医院.编码", "ID", "DSCN医院编码", "BI_CODE"]
        },
        {
            "ColName": "HOSP_LEVEL",
            "ColDesc": "医院等级",
            "Type": "String",
            "Candidate": ["医院等级", "HOSPITAL DEGREE", "LEVEL", "级别", "等级", "医院级别"]
        },
        {
            "ColName": "ATC",
            "ColDesc": "ATC编码",
            "Type": "String",
            "Candidate": ["ATC编码", "ATC码", "通用名ATC编码", "ATC", "ATC_CODE", "ATC3编码",
                          "ATC3_CODE", "ATC3名称"]
        },
        {
            "ColName": "MOLE_NAME",
            "ColDesc": "分子名",
            "Type": "String",
            "Candidate": ["MOLECULENAME", "MOLECULE NAME", "MOLECULE_NAME", "MCL_NAME", "药品名",
                          "分子名", "药品名称", "类别名称", "MOLECULE", "MOLECULE",
                          "MOLE_NAME", "MOLECULE.NAME", "化学名", "通用名", "KEY_BRAND"],
            "NotNull": True
        },
        {
            "ColName": "KEY_BRAND",
            "ColDesc": "通用名",
            "Type": "String",
            "Candidate": []
        },
        {
            "ColName": "PRODUCT_NAME",
            "ColDesc": "商品名",
            "Type": "String",
            "Candidate": ["商品名", "药品商品名", "PRODUCT", "PRODUCT_NAME",
                          "PRODUCT.NAME", "PRODUCTNAME", "商品名称"],
            "NotNull": True
        },
        {
            "ColName": "PACK",
            "ColDesc": "包装",
            "Type": "String",
            "Candidate": ["包装", "包装单位", "PACKAGE", "包装.单位", "包.装"],
            # "NotNull": True
        },
        {
            "ColName": "SPEC",
            "ColDesc": "规格",
            "Type": "String",
            "Candidate": ["药品规格", "包装规格", "规格", "统一规格", "SPECIFICAT", "PACK_DES", "品规",
                          "PACK_DESCRIPTION", "PACK", "SKU"],
            "NotNull": True  # TODO: 需要决策树
        },
        {
            "ColName": "DOSAGE",
            "ColDesc": "剂型",
            "Type": "String",
            "Candidate": ["剂型", "FORM", "DOSAGE", "FORMULATION_NAME", "CONTENT_TYPE", "CONTENTTYPE", "APP2_COD"],
            "NotNull": True
        },
        {
            "ColName": "PACK_QTY",
            "ColDesc": "包装数量",
            "Type": "String",
            "Candidate": ["包装数量", "PACKAGE_QTY", "PACK_NUMBER", "PACKNUMBER", "数量.（支/片）",
                          "数量支/片", "数量(支/片)", "数量.(支/片)",
                          "数量.支/片", "包装.数量", "SIZE"],
            # "NotNull": True
        },
        {
            "ColName": "SALES_QTY",
            "ColDesc": "销量",
            "Type": "Double",
            "Candidate": ["数量", "STANDARD_UNIT", "最小包装单位数量", "最小制剂单位数量", "QUANTITY",
                          "销售数量", "TOTAL_UNITS", "UNIT", "SALES_QTY", "数量（支/片）",],
            # "NotNull": True
        },
        {
            "ColName": "SALES_VALUE",
            "ColDesc": "销售额",
            "Type": "Double",
            "Candidate": ["金额", "金额元", "金额(元)", "金额（元）", "金额.元", "销售金AM", "VALUE", "SALES",
                          "SALES VALUE (RMB)", "SALES VALUERMB", "SALESVALUERMB", "SALES_VALUE",
                          "销售金额", "金额.（元）", "金额.(元)"],
            "NotNull": True
        },

        {
            "ColName": "DELIVERY_WAY",
            "ColDesc": "给药途径",
            "Type": "String",
            "Candidate": ["给药途径", "ROAD", "途径", "ADMINST", "DELIVERY_WAY", "给药.途径"]
        },
        {
            "ColName": "MANUFACTURER_NAME",
            "ColDesc": "生产厂商",
            "Type": "String",
            "Candidate": ["生产厂商", "生产厂家", "企业名称", "CORP_NAME", "生产企业",
                          "CORPORATIO", "MANUFACTUER_NAME", "CORPORATION", "MANUFACTUER",
                          "药厂名称", "集团名称", "COMPANY_NAME", "集团"],
            "NotNull": True
        },
        {
            "ColName": "MKT",
            "ColDesc": "所属市场",
            "Type": "String",
            "Candidate": ["竞品市场", "MARKET", "定义市场", "市场定义", "市场"],
            # "NotNull": True
        },
        {
            "ColName": "TAG",
            "ColDesc": "文件标识",
            "Type": "String",
            "Candidate": ["_TAG"],
            "NotNull": True
        }
    ]
for item in cpa_mapping():
    print(item)
    if item['NotNull']:
        print(1)
print(cpa_mapping())