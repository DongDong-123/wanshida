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


current_path = os.getcwd()


def get_parm():
    with open(os.path.join(current_path, 'parm.txt'), 'r', encoding='utf-8') as f:
        res = f.read()

    parm = res.split(',')
    n = int(parm[0])
    t = int(parm[1])
    print('开始序号{}'.format(n))
    print('parm文件交易日期{}'.format(t))

    return n, t


def updtae_parm(n, t):
    with open(os.path.join(current_path, 'parm.txt'), 'w', encoding='utf-8') as f:
        f.write("{},{}".format(n, t))


def zip_file(start_dir, date):
    """压缩数据文件和控制文件"""
    os.chdir(start_dir)
    start_dir = start_dir  # 要压缩的文件夹路径
    file_news = '{}'.format(date) + '_1.zip'  # 压缩后文件夹的名字
    print(file_news)
    z = zipfile.ZipFile(file_news, 'w', zipfile.ZIP_DEFLATED)
    for dir_path, dir_names, file_names in os.walk(start_dir):
        f_path = dir_path.replace(start_dir, '')  # 不replace的话，就从根目录开始复制
        f_path = f_path and f_path + os.sep or ''  # 实现当前文件夹以及包含的所有文件的压缩
        print('f_path', f_path)
        for filename in file_names:
            if date in filename and filename[-3:] in ("csv", "txt"):  # 添加数据文件和控制文件
                print(filename)
                z.write(os.path.join(dir_path, filename), f_path + filename)
                print('tt', os.path.join(dir_path, filename), f_path + filename)
                os.remove(filename)
            else:
                print(filename)
    z.close()
    return file_news


def running():
    n, t = get_parm()
    start_time = time.time()
    o = datannum

    for m in range(1):
        # st = datetime.datetime.strptime(str(t), "%Y%m%d")
        # file_date_time = str(st)[:10]
        file_date_time = str(t)
        stif_time = "{}100000".format(t)

        main(n, n + o, stif_time, file_date_time)
        n += o
        t += 1
        zip_file(zip_floder, file_date_time)

    end_time = time.time()
    print(end_time - start_time)  # 13

    updtae_parm(n, t)

if __name__ == "__main__":
    running()