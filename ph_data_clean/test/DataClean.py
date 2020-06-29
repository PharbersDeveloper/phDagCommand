import mapping


class MappingOne(object):
    def __init__(self):
        self.company = 'Pfizer'
        self.source = 'CPA'
        self.specific_mapping = mapping.cpa_mapping()


class MappingTwo(object):
    def __init__(self):
        self.company = 'Pfizer_2'
        self.source = 'GYC'
        self.specific_mapping = mapping.cpa_mapping_2()


class MappingFactory(object):
    def __init__(self):
        self.all_mapping = []
        mapping1 = MappingOne()
        self.all_mapping.append(mapping1)
        mapping2 = MappingTwo()
        self.all_mapping.append(mapping2)

    def get_specific_mapping(self, company, source):
        for maps in self.all_mapping:
            # print(maps.company)
            if (maps.source == source) & (maps.company == company):
                return maps.specific_mapping
                # print(maps.specific_mapping)


class DataClean(object):
    # def __init__(self):

    def cleaning_process(self, specific_mapping, raw_data):
        # standardise colunm name
        new_key_name = {}
        for raw_data_key in raw_data.keys():
            old_key = raw_data_key.split("#")[-1].strip()  # remove unwanted symbols
            for m in specific_mapping:
                # new_key_name[new_key] = None
                if old_key in m["Candidate"]:
                    new_key = m["ColName"]
                    new_key_name[new_key] = raw_data[raw_data_key]  # write new key name into dict

        # create ordered new dict
        final_data = {}
        for m in specific_mapping:
            for n in new_key_name.keys():
                if m["ColName"] == n:
                    final_data[m["ColName"]] = new_key_name[n]
                elif m["ColName"] not in final_data.keys():
                    final_data[m["ColName"]] = None
        return final_data
        # print(final_data)

    def get_mapping(self, company, source):
        specific_mapping = MappingFactory().get_specific_mapping(company, source)
        return specific_mapping
        # print(specific_mapping)

    def get_raw_data(self):
        raw_data = {"1#   省": "北京",
                    "2#城市": "北京市",
                    "3#年月": "201610",
                    "4#医院编码": "100782",
                    "5#竞品市场 ": "阿洛刻市场",
                    "6#ATC码 ": "R06AE07 ",
                    "7#药品名称 ": "西替利嗪 ",
                    "9#商品名 ": "喜宁 ",
                    "10#药品规格 ": "10MG ",
                    "11#包装数量": "24",
                    "12#金额（元）": "1814",
                    "13#数量（支/片）": "2400",
                    "14#剂型": "TAB",
                    "15#给药途径": "OR",
                    "16#集团": "宜昌长江药业有限公司",
                    "17#包装": "H"
                    }
        return raw_data


class Util(object):

    def change_year(self, final_data):
        if len(final_data['YEAR']) == 6:
            final_data['MONTH'] = int(final_data['YEAR']) % 100  # month
            final_data['YEAR'] = (int(final_data['YEAR']) - final_data['MONTH']) // 100  # year
        elif len(final_data['YEAR']) == 8:
            date = int(final_data['YEAR']) % 100  # date
            year_month = (int(final_data['YEAR']) - date) // 100  # year+month
            final_data['MONTH'] = year_month % 100  # month
            final_data['YEAR'] = (year_month - final_data['MONTH']) // 100  # year
        elif len(final_data['YEAR']) == 4:
            pass
        else:
            print("Invalid date data")
        print(final_data)
        # return final_data


class CpaDataClean(DataClean):
    def change_year(self, final_data):
        Util.change_year(self, final_data)


cpa = CpaDataClean()

final_data = cpa.cleaning_process(cpa.get_mapping('Pfizer', 'CPA'), cpa.get_raw_data())
cpa.change_year(final_data)

# MappingFactory().get_specific_mapping('Pfizer', 'CPA')
