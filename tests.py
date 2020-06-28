# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 15:20
# @Author  : liudongyang
# @FileName: tests.py
# @Software: PyCharm
from save_data import SaveFile, ConnectMysql
from make_data import MakeData
from Common import CommonFunction

makedata = MakeData()
commonfunction = CommonFunction()

def test_str(data):
    for elem in data:
        if elem is None:
            raise ValueError("elem type is not allow None")


def test_make_relation_data():
    datas = makedata.make_stan_relation()
    test_str(datas)
    print(datas)

def test_common_org_name():
    org_name = commonfunction.org_name()
    print(org_name, type(org_name))


# test_common_org_name()
# test_make_relation_data()


def test_save_file():
    data = [10000, 'zhangsansan', 'C01', 3, '2', 'Lcpyngo Yrxq Ofepbiy Dwptzxw', 'LYOD', '', '29', '', 'wCw431729', '20310225', 'VGB', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '20200625181307', '22591', '20200625181307', '16816']
    datas = [data]
    file = SaveFile()
    file.write_to_csv(datas, 't_stan_relation')


test_save_file()