# -*- coding: utf-8 -*-
# @Time    : 2020-06-24 22:16
# @Author  : liudongyang
# @FileName: Common.py
# @Software: PyCharm
# 公共方法
import random
import time
import redis
from redis_data import RedisConnect
pool = RedisConnect()


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

    # 电话号码数据
    def make_tel_num(self, tp=11):
        """
        随机生成手机号码
        11:家庭电话
        12:工作电话
        21:固定电话
        22:移动电话
        23:传真
        :return: 返回字符串类型
        """
        if tp == '21' or tp == '23':
            phone = self.random_num(9)
        else:
            one_two = random.choice(["13", "14", "15", "16", "17", "18", "19"])
            three_nine = []
            for num in range(9):
                elem = random.randint(0, 9)
                three_nine.append(str(elem))
            phone = one_two + "".join(three_nine)
        return phone

    # 地址数据
    def make_address(self, code):
        """

        :param code: 省市区代码
        :return: 详细地址
        """
        three_level_addr = pool.get_data(code)
        three_level_addr.replace('-')
        street_address = random.choice([
            "解放路", "千佛山", "趵突泉", "泉城路", "大明湖", "东关", "文东", "建新", "甸柳", "燕山", "姚家", "龙洞", "智远", "舜华路", "大观园", "杆石桥",
            "四里村",
            "魏家庄", "二七", "七里山", "六里山", "舜玉路", "泺源", "王官庄", "舜耕", "白马山", "七贤", "十六里河", "兴隆", "党家", "陡沟", "振兴街", "中大槐树",
            "道德街", "西市场", "五里沟", "营市街", "青年公园", "南辛庄", "段店北路", "张庄路", "匡山", "美里湖", "吴家堡", "腊山", "兴福", "玉清湖", "无影山",
            "天桥东街",
            "北村", "南村", "堤口路", "北坦", "制锦市", "宝华", "官扎营", "纬北路", "药山", "北园", "泺口", "桑梓店", "大桥", "山大路", "洪家楼", "东风", "全福",
            "孙村", "巨野河", "华山", "荷花路", "王舍人", "鲍山", "郭店", "唐冶", "港沟", "遥墙", "临港", "仲宫", "柳埠", "董家", "彩石", "文昌", "崮云湖",
            "平安",
            "五峰山", "归德", "万德", "张夏", "明水", "双山", "圣井", "埠村", "枣园", "龙山", "普集", "官庄", "相公庄", "绣惠", "文祖", "曹范", "白云湖",
            "高官寨",
            "宁家埠", "济阳", "济北", "回河", "孙耿", "崔寨", "太平", "榆山", "锦水"
        ])
        areas_name = random.choice([
            "万豪国际公寓", "晓月苑", "永定路商住中心", "橙色年代", "嘉慧苑", "致雅居", "彩虹城", "松园小区", "燕归园", "北京青年城", "金宝纯别墅", "翌景嘉园", "涧桥·泊屋馆",
            "京东丽景", "旭风苑公寓", "朝阳无限", "庄胜二期", "潇雅居", "GOGO新世代", "飞腾家园", "英嘉公寓", "高第", "金榜园", "迎曦园", "风格与林",
            "太阳国际公馆(瑞景嘉园)",
            "永合馨苑", "澳洲新星", "丰润世家", "洋桥花园", "长安新城", "金隅丽港城", "兴涛社区", "糖人街", "时代芳群", "运河园", "浉城百郦", "测试项目", "新洲商务大厦",
            "加来小镇",
            "新新公寓", "颍泽洲", "城市印象", "上河美墅", "同泰苑", "和枫雅居", "建兴家园", "昊腾花园", "高苑·花样年华", "金码大厦", "天辉公寓", "NOLITA那里", "政馨家园",
            "文林商苑", "蝶翠华庭", "晋元庄小区", "幸福源", "当代城市家园", "非常生活", "祥瑞苑", "雪梨澳乡", "清欣园", "晟丰阁", "倚林佳园", "华龙小区", "秀安园",
            "新华联锦园",
            "乐澜宝邸", "棉花城", "CLASS", "金宸公寓", "燕景佳园", "珠江帝景", "龙山新新小镇", "万景公寓", "飘HOME", "蓝堡", "新纪元公寓", "中信红树湾", "海德堡花园",
            "天缘公寓", "长城盛世", "鲁艺上河村", "瑞馨公寓", "鼎诚国际MM", "德胜世嘉", "榆园新居", "远洋天地", "星河城", "黎明新座", "世纪城", "大观园中华商住区",
            "中国第一商城",
            "后现代城", "中海凯旋", "新都丽苑", "陶然北岸", "观河锦苑", "星光公寓", "观筑", "绿城星洲花园", "御鹿家园", "都市心海岸", "山水汇豪", "漪内轩", "颐园(碧水云天)",
            "新荣家园", "双桥温泉北里住宅", "恬心家园", "正邦嘉园", "依翠园", "万科西山庭院", "新御景", "天行建商务大厦", "浉城百丽", "华腾园", "同仁园", "格林小镇",
            "东华经典(东华金座)", "俊景苑", "朗琴园", "快乐洋城", "新中环公寓", "非常宿舍", "清城名苑", "兴都苑(水榭楼台)", "雍景台", "风林绿洲(奕翠庭)", "团结公寓"
        ])
        building_name = str(random.randint(1, 50))
        unit_num = str(random.randint(1, 9))
        floor_num = str(random.randint(1, 30))
        room_num = str(random.randint(1, 4))
        return three_level_addr + street_address + "街道" + areas_name + building_name + "楼" + unit_num + "单元" + floor_num + "层" + room_num + "号"
