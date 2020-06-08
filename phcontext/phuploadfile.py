# -*- coding: utf-8 -*-
"""alfredyang@pharbers.com.

This module document the usage of class pharbers command context,
"""
import pandas as pd


def phUploadOriginalFilesFunc(path):
    df = pd.read_excel(path, sheet_name=u"原始数据位置")
    df["Sheet"].fillna(value="", inplace=True)
    df[u"客户是否标准化"].fillna(value="否", inplace=True)
    df["Start_Row"].fillna(value=1, inplace=True)
    df["Name"].fillna(value="", inplace=True)
    df = df[df[u"公司名"] != ""]
    for row_index in range(0, df.shape[0]):
        row = df.loc[row_index]
        if not row[u"公司名"]:
            raise Exception("Company name should not be NaN")
        print(row)


