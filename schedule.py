# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 11:37
# @Author  : liudongyang
# @FileName: schedule.py
# @Software: PyCharm
# 任务调度
import threading
import time
import os
from make_data import MakeData
from save_data import SaveFile
from parm import savenum, stifnum, filenum, zip_floder
from Common import CommonFunction
comm = CommonFunction()
savedata = SaveFile()
# 临时存储生成数据
orgs = []
relations = []
ptxns = []
dtxns = []
txns = []
stifs = []
survey_info1 = []
survey_info2 = []
survey_info3 = []
# data_path = os.path.join(zip_floder, 'data')
data_path = zip_floder

def __threads(all_data, all_table_name, file_date_time, order, sign):
    """抽出多线程部分"""
    threads = []
    for ind, dat in enumerate(all_data):
        if len(eval(dat)):
            thr = threading.Thread(target=savedata.write_to_csv, args=(
                eval(dat), all_table_name[ind], file_date_time, order, sign))
            thr.start()
            threads.append(thr)

    for t in threads:
        t.join()

def __control_file(file_name, file_date_time, file_num):
    currt_time = time.strftime('%Y%m%d', time.localtime())
    file_full = os.path.join(data_path, 'D{}-T{}_00{}.txt'.format(
        file_date_time, currt_time, 1))
    filename = '{}-D{}-T{}_00{}.csv'.format(file_name, file_date_time, currt_time, file_num)
    with open(file_full, '+a', encoding="UTF-8") as f:
        f.write(','.join([filename, str(savenum)]) + "\n")

def main(beg, end, stif_time, file_date_time):
    currt_time = time.strftime('%Y%m%d', time.localtime())
    makedata = MakeData()
    # 日期格式转换
    stif_time = comm.turn_date10(stif_time)

    # 临时数据变量名
    all_data = ["orgs", "relations", "survey_info1", "survey_info2", "survey_info3"]
    # 表名，需和all_data一一对应。
    all_table_name = ["org", "relation", "survey_info1", "survey_info2", "survey_info3"]
    save_ci = filenum//savenum  # 每个数据文件需要储存的次数
    sign_other = 0  # 其他表数量标识
    sign_txn = 0  # 交易数量标识
    sc_other = 0  # 其他表保存次数
    sc_stif = 0  # 交易保存次数
    stif_data_num = 1  # 交易数据文件编号
    file_ord = 1  # 其他数据文件名编号
    sign_stif = 0  # 可疑交易数量
    for num in range(beg, end):
        t_stan_org = makedata.make_stan_org(num)
        orgs.append(t_stan_org)
        t_stan_relation = makedata.make_stan_relation()
        relations.append(t_stan_relation)

        t_stan_survey_info1 = makedata.make_stan_survey_info1()
        survey_info1.append(t_stan_survey_info1)
        t_stan_survey_info2 = makedata.make_stan_survey_info2()
        survey_info2.append(t_stan_survey_info2)
        t_stan_survey_info3 = makedata.make_stan_survey_info3()
        survey_info3.append(t_stan_survey_info3)
        # 单独生成交易数据,根据原始交易生产标准交易
        for i in range(stifnum):
            t_stan_ptxn, all_dict_data = makedata.make_stan_ptxn(stif_time)

            t_stan_txn = makedata.make_stan_txn(stif_time, all_dict_data)
            txns.append(t_stan_txn)

            if num %10 == 0:
                t_stan_stif = makedata.make_stan_stif(stif_time, all_dict_data)
                stifs.append(t_stan_stif)
                sign_stif += 1

        # 除交易外的存储
        sign_other += 1
        if sign_other % savenum == 0:  # 符合条件，多线程存储
            sc_other += 1
            print('{} 存储数据{}条，文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),savenum, file_ord))
            __threads(all_data, all_table_name, file_date_time, file_ord, sign_other)

            if sc_other == save_ci:
                for name in all_table_name:
                    __control_file(name, file_date_time, file_ord)
                file_ord += 1
                sc_other = 0
                sign_other = 0

            for data in all_data:  # 清空已写入数据
                eval(data).clear()

        # 交易单独储存
        sign_txn += stifnum
        if (sign_txn) % savenum == 0:  # 符合条件，多线程存储
            sc_stif += 1
            print('{} 存储交易数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),savenum, stif_data_num))

            __threads(["txns"], ["txn"], file_date_time, stif_data_num, sign_txn)

            if sc_stif == save_ci:
                __control_file("txn", file_date_time, stif_data_num)
                # file_full = os.path.join(data_path, 'D{}-T{}_00{}.txt'.format(
                #     file_date_time, currt_time, 1))
                # filename = '{}-D{}-T{}_00{}.csv'.format("txn", file_date_time, currt_time, stif_data_num)
                # with open(file_full, '+a', encoding="UTF-8") as f:
                #     f.write(','.join([filename, str(savenum)])+ "\n")
                stif_data_num += 1
                sc_stif = 0
                sign_txn = 0

            txns.clear()  # 清空已写入交易数据

    if sign_other > 0:
        print('{} 存储剩余数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_other, file_ord))
        __threads(all_data, all_table_name, file_date_time, file_ord, sign_other)
        for name in all_table_name:
            __control_file(name, file_date_time, file_ord)
        for data in all_data:  # 清空已写入数据
            eval(data).clear()

    if sign_txn > 0:
        print('{} 存储剩余交易数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_txn, stif_data_num))
        __threads(["txns"], ["txn"], file_date_time, stif_data_num, sign_txn)
        __control_file("txn", file_date_time, stif_data_num)

        txns.clear()

    if sign_stif > 0:
        print('{} 存储可疑交易数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_stif, 1))
        __threads(["stifs"], ["stif"], file_date_time, 1, sign_stif)
        __control_file("stif", file_date_time, sign_stif)

        txns.clear()


