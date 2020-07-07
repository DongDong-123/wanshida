# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 15:20
# @Author  : liudongyang
# @FileName: tests.py
# @Software: PyCharm
from save_data import SaveFile, ConnectMysql
from make_data import MakeData
from Common import CommonFunction
from faker import Faker
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
    # file.write_to_csv(datas, 't_stan_relation')


# test_save_file()



fake = Faker(locale='zh_CN')

"""
1、地理信息类
fake.city_suffix()：市，县
fake.country()：国家
fake.country_code()：国家编码
fake.district()：区
fake.geo_coordinate()：地理坐标
fake.latitude()：地理坐标(纬度)
fake.longitude()：地理坐标(经度)
fake.postcode()：邮编
fake.province()：省份
fake.address()：详细地址
fake.street_address()：街道地址
fake.street_name()：街道名
fake.street_suffix()：街、路
2、基础信息类
fake.ssn()：生成身份证号
fake.bs()：随机公司服务名
fake.company()：随机公司名（长）
fake.company_prefix()：随机公司名（短）
fake.company_suffix()：公司性质
fake.credit_card_expire()：随机信用卡到期日
fake.credit_card_full()：生成完整信用卡信息
fake.credit_card_number()：信用卡号
fake.credit_card_provider()：信用卡类型
fake.credit_card_security_code()：信用卡安全码
fake.job()：随机职位
fake.first_name_female()：女性名
fake.first_name_male()：男性名
fake.last_name_female()：女姓
fake.last_name_male()：男姓
fake.name()：随机生成全名
fake.name_female()：男性全名
fake.name_male()：女性全名
fake.phone_number()：随机生成手机号
fake.phonenumber_prefix()：随机生成手机号段
fake.3、计算机基础、Internet信息类
fake.ascii_company_email()：随机ASCII公司邮箱名
fake.ascii_email()：随机ASCII邮箱：
fake.company_email()：
fake.email()：
fake.safe_email()：安全邮箱
fake.4、网络基础信息类
fake.domain_name()：生成域名
fake.domain_word()：域词(即，不包含后缀)
fake.ipv4()：随机IP4地址
fake.ipv6()：随机IP6地址
fake.mac_address()：随机MAC地址
fake.tld()：网址域名后缀(.com,.net.cn,等等，不包括.)
fake.uri()：随机URI地址
fake.uri_extension()：网址文件后缀
fake.uri_page()：网址文件（不包含后缀）
fake.uri_path()：网址文件路径（不包含文件名）
fake.url()：随机URL地址
fake.user_name()：随机用户名
fake.image_url()：随机URL地址
fake.5、浏览器信息类
fake.chrome()：随机生成Chrome的浏览器user_agent信息
fake.firefox()：随机生成FireFox的浏览器user_agent信息
fake.internet_explorer()：随机生成IE的浏览器user_agent信息
fake.opera()：随机生成Opera的浏览器user_agent信息
fake.safari()：随机生成Safari的浏览器user_agent信息
fake.linux_platform_token()：随机Linux信息
fake.user_agent()：随机user_agent信息
fake.6、数字类
fake.numerify()：三位随机数字
fake.random_digit()：0~9随机数
fake.random_digit_not_null()：1~9的随机数
fake.random_int()：随机数字，默认0~9999，可以通过设置min,max来设置
fake.random_number()：随机数字，参数digits设置生成的数字位数
fake.pyfloat()：
fake.left_digits=5 #生成的整数位数, right_digits=2 #生成的小数位数, positive=True #是否只有正数
fake.pyint()：随机Int数字（参考random_int()参数）
fake.pydecimal()：随机Decimal数字（参考pyfloat参数）
7、文本、加密类
fake.pystr()：随机字符串
fake.random_element()：随机字母
fake.random_letter()：随机字母
fake.paragraph()：随机生成一个段落
fake.paragraphs()：随机生成多个段落
fake.sentence()：随机生成一句话
fake.sentences()：随机生成多句话，与段落类似
fake.text()：随机生成一篇文章
fake.word()：随机生成词语
fake.words()：随机生成多个词语，用法与段落，句子，类似
fake.binary()：随机生成二进制编码
fake.boolean()：True/False
fake.language_code()：随机生成两位语言编码
fake.locale()：随机生成语言/国际 信息
fake.md5()：随机生成MD5
fake.null_boolean()：NULL/True/False
fake.password()：随机生成密码,可选参数：length：密码长度；special_chars：是否能使用特殊字符；digits：是否包含数字；upper_case：是否包含大写字母；lower_case：是否包含小写字母
fake.sha1()：随机SHA1
fake.sha256()：随机SHA256
fake.uuid4()：随机UUID
fake.8、时间信息类
fake.date()：随机日期
fake.date_between()：随机生成指定范围内日期，参数：start_date，end_date
fake.date_between_dates()：随机生成指定范围内日期，用法同上
fake.date_object()：随机生产从1970-1-1到指定日期的随机日期。
fake.date_time()：随机生成指定时间（1970年1月1日至今）
fake.date_time_ad()：生成公元1年到现在的随机时间
fake.date_time_between()：用法同dates
fake.future_date()：未来日期
fake.future_datetime()：未来时间
fake.month()：随机月份
fake.month_name()：随机月份（英文）
fake.past_date()：随机生成已经过去的日期
fake.past_datetime()：随机生成已经过去的时间
fake.time()：随机24小时时间
fake.timedelta()：随机获取时间差
fake.time_object()：随机24小时时间，time对象
fake.time_series()：随机TimeSeries对象
fake.timezone()：随机时区
fake.unix_time()：随机Unix时间
fake.year()：随机年份
fake.9、python 相关方法
fake.profile()：随机生成档案信息
fake.simple_profile()：随机生成简单档案信息
fake.pyiterable()
fake.pylist()
fake.pyset()
fake.pystruct()
fake.pytuple()
fake.pydict()
可以用dir(fake)，看Faker库都可以fake哪些数据，目前Faker支持近300种数据，此外还支持自己进行扩展。
"""

name = fake.name()
address = fake.address()
city = fake.city_suffix()
dis = fake.district()
citd = fake.ssn()
contant = fake.sentence()
print(city,dis,citd,contant)
print(name, address)