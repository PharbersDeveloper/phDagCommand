import sys
import os
import json
import click
from pherrs.ph_err import PhError
from ph_data_clean.clean.cleaner_factory import CleanerFactory
from ph_data_clean.model.mapping_factory import MappingFactory
from ph_data_clean.model.clean_result import CleanResult, Tag


def block_print():
    sys.stdout = open(os.devnull, 'w')


def enable_print():
    sys.stdout = sys.__stdout__


def clean(mp, rd):
    """
    选择清洗算法和匹配表，并对结果进行包装
    :param mp: 匹配表位置
    :param rd: 原始数据，数据格式为 [{rawdata},{metadata}]
    :return: CleanResult
    """
    source = rd[1]['providers'][1]
    company = rd[1]['providers'][0]

    cleaner = CleanerFactory().get_specific_cleaner(source, company)
    mapping = MappingFactory(mp).get_specific_mapping(source, company)

    result = cleaner.cleaning_process([col.to_dict() for col in mapping.cols], rd[0])
    if result.tag.value > 0:
        result.metadata = mapping.get_metadata()

    return result


@click.command("clean", short_help='源数据清洗与 Schema 统一')
@click.option('-m', '--mapping_path', default=r'file/ph_data_clean/mapping_table/', type=click.Path(exists=True))
@click.argument("raw_data")
def main(mapping_path, raw_data):
    """
    Python 实现的数据清洗，并根据 Source 和 Company 选择清洗算法和清洗结构，以此同源数据的 Schema 统一
    """
    block_print()
    try:
        result = clean(mapping_path, json.loads(raw_data))
        enable_print()
    except PhError as err:
        result = CleanResult(data={}, metadata={}, tag=Tag.PH_ERR, err_msg=str(err))

    sys.stdout.write(str(result))
    sys.stdout.flush()


if __name__ == '__main__':
    main()

# cli test of unix
# python phcli clean [{"0#省/自治区/直辖市":"辽宁省","1#城市":"大连市","2#年":"2017","3#月":"1","4#医院编码":"2102014","5#通用名":"头孢孟多","6#商品名":"二叶莫","7#包装":"0.5g","8#包装数量":"1.0","9#金额":"74520.0","10#最小制剂单位数量":"5400.0","11#剂型":"注射剂","12#给药途径":"注射","13#生产企业":"苏州二叶制药有限公司","_tag":"phf\u001Fowner\u001F2017年1月辉瑞头孢孟多产品数据.xlsx\u001F2017年1月辉瑞头孢孟多产品数据\u001F10000"},{"assetId":"5dd5224783de972f084b001f","providers":["Pfizer","CPA&GYC"],"geoCover":[],"dataCover":["201701"],"label":["原始数据"],"fileName":"2017年1月辉瑞头孢孟多产品数据.xlsx","molecules":[],"schema":[{"key":"0#省/自治区/直辖市","type":"String"},{"key":"1#城市","type":"String"},{"key":"2#年","type":"String"},{"key":"3#月","type":"String"},{"key":"4#医院编码","type":"String"},{"key":"5#通用名","type":"String"},{"key":"6#商品名","type":"String"},{"key":"7#包装","type":"String"},{"key":"8#包装数量","type":"String"},{"key":"9#金额","type":"String"},{"key":"10#最小制剂单位数量","type":"String"},{"key":"11#剂型","type":"String"},{"key":"12#给药途径","type":"String"},{"key":"13#生产企业","type":"String"},{"key":"_tag","type":"String"}],"markets":[],"sheetName":"2017年1月辉瑞头孢孟多产品数据","length":145.0}]

# cli test of windows
# python phcli clean [{\"0#省/自治区/直辖市\":\"辽宁省\",\"1#城市\":\"大连市\",\"2#年\":\"2017\",\"3#月\":\"1\",\"4#医院编码\":\"2102014\",\"5#通用名\":\"头孢孟多\",\"6#商品名\":\"二叶莫\",\"7#包装\":\"0.5g\",\"8#包装数量\":\"1.0\",\"9#金额\":\"74520.0\",\"10#最小制剂单位数量\":\"5400.0\",\"11#剂型\":\"注射剂\",\"12#给药途径\":\"注射\",\"13#生产企业\":\"苏州二叶制药有限公司\",\"_tag\":\"phf\u001Fowner\u001F2017年1月辉瑞头孢孟多产品数据.xlsx\u001F2017年1月辉瑞头孢孟多产品数据\u001F10000\"},{\"assetId\":\"5dd5224783de972f084b001f\",\"providers\":[\"Pfizer\",\"CPA&GYC\"],\"geoCover\":[],\"dataCover\":[\"201701\"],\"label\":[\"原始数据\"],\"fileName\":\"2017年1月辉瑞头孢孟多产品数据.xlsx\",\"molecules\":[],\"schema\":[{\"key\":\"0#省/自治区/直辖市\",\"type\":\"String\"},{\"key\":\"1#城市\",\"type\":\"String\"},{\"key\":\"2#年\",\"type\":\"String\"},{\"key\":\"3#月\",\"type\":\"String\"},{\"key\":\"4#医院编码\",\"type\":\"String\"},{\"key\":\"5#通用名\",\"type\":\"String\"},{\"key\":\"6#商品名\",\"type\":\"String\"},{\"key\":\"7#包装\",\"type\":\"String\"},{\"key\":\"8#包装数量\",\"type\":\"String\"},{\"key\":\"9#金额\",\"type\":\"String\"},{\"key\":\"10#最小制剂单位数量\",\"type\":\"String\"},{\"key\":\"11#剂型\",\"type\":\"String\"},{\"key\":\"12#给药途径\",\"type\":\"String\"},{\"key\":\"13#生产企业\",\"type\":\"String\"},{\"key\":\"_tag\",\"type\":\"String\"}],\"markets\":[],\"sheetName\":\"2017年1月辉瑞头孢孟多产品数据\",\"length\":145.0}]
