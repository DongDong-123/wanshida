# -*- coding: utf-8 -*-
# @Time    : 2020/6/28 17:46
# @Author  : liudongyang
# @FileName: parm.py
# @Software: PyCharm
# 加载所有参数
from readconfig import Setting, RedisConfig
import os, sys


parm_ob = Setting()
# 数据参数
savenum = parm_ob.get_num()  # 多少条数据写入文件一次
datannum = int(parm_ob.data_num())  # 总数据数
stifnum = int(parm_ob.stif_num())  # 交易数据条数
winsavepath = parm_ob.get_save_path()  # 数据存储路径 win
linuxsavepath = parm_ob.get_linux_path()  # 数据存储路径 linux
datadate = parm_ob.get_data_date()  # 开始数据日期，
filenum = int(parm_ob.get_file_num())  # 每个数据文件储存的数据条数


# redis 连接信息
redis_conf = RedisConfig()
redis_host = redis_conf.host()
redis_port = int(redis_conf.port())
redis_db = int(redis_conf.db())
redis_passwd = int(redis_conf.passwd())

if sys.platform == 'linux':
    os.chdir('/home/admin/make_data/mcaml')
    zip_floder = linuxsavepath
elif sys.platform == 'win32':
    zip_floder = winsavepath
else:
    zip_floder = os.path.join(os.getcwd(), 'data')
    if not os.path.exists(zip_floder):
        os.mkdir(zip_floder)