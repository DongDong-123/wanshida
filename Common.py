# -*- coding: utf-8 -*-
# @Time    : 2020-06-24 22:16
# @Author  : liudongyang
# @FileName: Common.py
# @Software: PyCharm
# 公共方法
import random
import time


class CommonFunction:
    def __init__(self):
        self.rel_sctp = '2'
        self.year = time.strftime("%Y", time.localtime())
        self.month = time.strftime("%m", time.localtime())
        self.day = time.strftime("%d", time.localtime())

    def random_str(self, num):
        words = 'abcdefghijklmnopqrstuvwxyz'
        strs = ''.join(random.choices(words, k=num))
        return strs.capitalize()

    def person_fir_name(self):  # 个人客户first name
        name = self.random_str(random.randint(3,7))
        return name

    def org_name(self):  # 机构客户名称
        name = [self.random_str(random.randint(4,7)) for i in range(random.randint(3,5))]
        return ' '.join(name)

    def relation_type(self):
        """
        关系人类型，个人关系、机构关系，其他
        :return:
        """
        cust_type = ''
        if cust_type == 1:
            relation_type = random.choice([
                "B01",  # 夫妻关系
                "B02",  # 子女
                "B03",  # 父母
                "B04",  # 其他血亲
                "B05",  # 其他姻亲
                "B06",  # 同学
                "B07" ] # 朋友
            )
        elif cust_type == 2:
            relation_type = random.choice([
                "A01",  # 对公客户与法人代表
                "A02",  # 对公客户与联系人
                "A03",  # 对公客户与负责人
                "A04",  # 对公客户与董事
                "A05",  # 对公客户与股东
                "A06",  # 母公司与子公司
                "A07",  # 代理
                "A08",  # 投资与被投资
                "A09",  # 其他关联单位
                "A10",  # 企业团体
                "A11",  # 银行团体
                "A12" ] # 家族企业
            )
        else:
            relation_type = random.choice(
                  ["X",  # 未说明
                "C01"]  # 受益所有人
            )

        return relation_type


    def rel_layer(self):
        layer = random.randint(0,5)
        return layer

    def cert_type(self):  # 证件类型
        if self.rel_sctp == '1':
            cstp = random.choice([
            "11",  # 居民身份证或临时身份证
            "12",  # 军人或武警身份证件
            "13",  # 港澳台通行证
            "14",  # 外国公民护照
            "19"]  # 其他个人有效证件(需进一步说明)
        )
        else:
            cstp = random.choice([
                "21",  # 组织机构代码
                "29"]  # 其他机构代码(需进一步说明)
            )
        return cstp

    def person_cert_num(self):  # 个人证件号码
        ctid = self.random_num(18)

        return ctid

    def org_cert_num(self,num=9):  # 机构证件号码 默认9位数字字母组合
        strs = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        nums = "0123456789"
        n = random.randint(3,6)
        cert = ''.join(random.choices(strs,k=n)) + ''.join(random.choices(nums, k=(num-n)))
        return cert

    def cert_dateline(self):  # 证件有效期
        # 年
        cert_year = int(self.year) + random.randint(-3, 20)
        # 月
        cert_month = int(self.month) + random.randint(0,11)
        if cert_month > 12:
            cert_month = cert_month % 12

        # 日
        cert_day = int(self.day) + random.randint(-5, 10)
        if cert_day < 0:
            cert_day = -cert_day
        elif cert_day == 0:
            cert_day = 1
        else:
            if cert_month == 2 and cert_day > 28:
                cert_day = cert_day % 28
            elif cert_month in (4,6,9,11) and cert_day >30:
                cert_day = cert_day % 30
            elif cert_day > 31:
                cert_day = cert_day % 31

        # 转为字符串
        cert_year = str(cert_year)

        if cert_month < 10:
            cert_month = '0' + str(cert_month)
        else:
            cert_month = str(cert_month)

        if cert_day < 10:
            cert_day = '0' + str(cert_day)
        else:
            cert_day = str(cert_day)

        return cert_year + cert_month + cert_day

    # 国籍
    def chiose_country(self):
        countrys = random.choice(["CHN", "ALB", "DZA", "AFG", "ARG", "ARE", "ABW", "OMN", "AZE", "EGY", "ETH", "IRL", "EST", "AND", "AGO", "AIA", "ATG", "AUT", "ALA", "AUS", "MAC", "BRB", "PNG", "BHS", "PAK", "PRY", "PSE", "BHR", "PAN", "BRA", "BLR", "BMU", "BGR", "MNP", "BEN", "BEL", "ISL", "PRI", "BIH", "POL", "BOL", "BLZ", "BWA", "BTN", "BFA", "BDI", "BVT", "PRK", "GNQ", "DNK", "DEU", "TLS", "TGO", "DOM", "DMA", "RUS", "ECU", "ERI", "FRA", "FRO", "PYF", "GUF", "ATF", "MAF", "VAT", "PHL", "FJI", "FIN", "CPV", "GMB", "COG", "COD", "COL", "CRI", "GRD", "GRL", "GEO", "GGY", "CUB", "GLP", "GUM", "GUY", "KAZ", "HTI", "KOR", "NLD", "BES", "SXM", "HMD", "MNE", "HND", "KIR", "DJI", "KGZ", "GIN", "GNB", "CAN", "GHA", "GAB", "KHM", "CZE", "ZWE", "CMR", "QAT", "CYM", "CCK", "COM", "CIV", "KWT", "HRV", "KEN", "COK", "CUW", "LVA", "LSO", "LAO", "LBN", "LTU", "LBR", "LBY", "LIE", "REU", "LUX", "RWA", "ROU", "MDG", "IMN", "MDV", "FLK", "MLT", "MWI", "MYS", "MLI", "MKD", "MHL", "MTQ", "MYT", "MUS", "MRT", "USA", "UMI", "ASM", "VIR", "MNG", "MSR", "BGD", "PER", "FSM", "MMR", "MDA", "MAR", "MCO", "MOZ", "MEX", "NKR", "NAM", "ZAF", "ATA", "SGS", "SSD", "NRU", "NPL", "NIC", "NER", "NGA", "NIU", "NOR", "NFK", "PLW", "PCN", "PRT", "JPN", "SWE", "CHE", "SLV", "WSM", "SRB", "SLE", "SEN", "CYP", "SYC", "SAU", "BLM", "CXR", "STP", "SHN", "KNA", "LCA", "SMR", "SPM", "VCT", "LKA", "SVK", "SVN", "SJM", "SWZ", "SDN", "SUR", "SLB", "SOM", "TJK", "THA", "TZA", "TON", "TCA", "TTO", "TUN", "TUV", "TUR", "TKM", "TKL", "WLF", "VUT", "GTM", "VEN", "BRN", "UGA", "UKR", "URY", "UZB", "ESP", "ESH", "GRC", "HKG", "SGP", "NCL", "NZL", "HUN", "SYR", "JAM", "ARM", "YEM", "IRQ", "IRN", "ISR", "ITA", "IND", "IDN", "GBR", "VGB", "IOT", "JOR", "VNM", "ZMB", "JEY", "TCD", "GIB", "CHL", "CAF", "TWN"])
        return countrys

    def data_time(self):
        """数据生成时间"""
        datatime = time.strftime("%Y%m%d%H%M%S", time.localtime())
        return datatime

    def random_num(self, num):
        """ 接收int类型参数num，根据参数随机生成数字,返回字符串"""
        res_list = []
        while len(res_list) < num:
            elem = random.randint(0, 9)
            if res_list or elem:
                res_list.append(str(elem))

        return "".join(res_list)

