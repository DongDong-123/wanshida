# -*- coding: utf-8 -*-
# @Time    : 2020-06-24 22:14
# @Author  : liudongyang
# @FileName: run.py
# @Software: PyCharm
# 启动文件
from parm import datannum, zip_floder
from schedule import main
import os
import zipfile
import time
import datetime
from Common import CommonFunction
comm = CommonFunction()

current_path = os.getcwd()


def get_parm():
    with open(os.path.join(current_path, 'parm.txt'), 'r', encoding='utf-8') as f:
        res = f.read()

    parm = res.split(',')
    n = int(parm[0])
    t = int(parm[1])


    return n, t


def updtae_parm(n, pt):
    """执行完后，写入最新的编号和跑批日期"""
    # pt = process_time(pt)
    with open(os.path.join(current_path, 'parm.txt'), 'w', encoding='utf-8') as f:
        f.write("{},{}".format(n, pt))


def zip_file(start_dir, date):
    """压缩数据文件和控制文件"""
    print('开始压缩文件')
    os.chdir(start_dir)
    start_dir = start_dir  # 要压缩的文件夹路径
    file_news = '{}'.format(date) + '_1.zip'  # 压缩后文件的名字
    print("压缩包名称：" , file_news)
    z = zipfile.ZipFile(file_news, 'w', zipfile.ZIP_DEFLATED)
    for dir_path, dir_names, file_names in os.walk(start_dir):
        f_path = dir_path.replace(start_dir, '')  # 不replace的话，就从根目录开始复制
        f_path = f_path and f_path + os.sep or ''  # 实现当前文件夹以及包含的所有文件的压缩
        # print('f_path', f_path)
        for filename in file_names:
            if date in filename and filename[-3:] in ("csv", "txt"):  # 添加数据文件和控制文件
                print("添加{}到压缩文件：".format(filename))
                z.write(os.path.join(dir_path, filename), f_path + filename)
                # print('tt', os.path.join(dir_path, filename), f_path + filename)
                os.remove(filename)
            else:
                # print(filename)
                print('文件{}不符合压缩条件'.format(filename))
    z.close()
    return file_news


def running():
    n, t = get_parm()
    t = 20190201   # 临时写死
    start_time = time.time()
    o = datannum

    for m in range(3):
        control_file_time = round(time.time() * 1000)
        print('客户号起始编号{}'.format(n))
        print('数据交易日期{}'.format(t))
        # st = datetime.datetime.strptime(str(t), "%Y%m%d")
        # file_date_time = str(st)[:10]
        file_date_time = str(t)
        # stif_time = "{}100000".format(t)
        stif_time = "{}".format(t)

        main(n, n + o, stif_time, file_date_time, control_file_time)
        n += o
        t += 1
        t = int(comm.process_time(t))  # 处理日期
        # zip_file(zip_floder, file_date_time)

    end_time = time.time()
    print("执行时间：", end_time - start_time)  # 13

    updtae_parm(n, t)

def process_control_file(datatime, num=1):
    curr_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    data_flode = os.path.join(zip_floder, 'data')
    file_list = os.listdir(data_flode)
    for file in file_list:
        if file[-3:] == 'txt' and datatime in file:
            with open(file, 'r', encoding='utf-8') as f:
                res = f.read()

            control_file = "D{}-T{}-000{}.txt".format(datatime, curr_time,num)
            with open(control_file, '+a', encoding='UTF-8') as f:
                f.read()


if __name__ == "__main__":
    running()