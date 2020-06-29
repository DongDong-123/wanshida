# -*- coding: utf-8 -*-
# @Time    : 2020/6/28 17:46
# @Author  : liudongyang
# @FileName: parm.py
# @Software: PyCharm
# 加载所有参数
from readconfig import Setting, RedisConfig

parm_ob = Setting()

savenum = parm_ob.get_num()
beginnum = parm_ob.data_num()
stifnum = parm_ob.stif_num()


redis_conf = RedisConfig()
redis_host = redis_conf.host()
redis_port = int(redis_conf.port())
redis_db = int(redis_conf.db())