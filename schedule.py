# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 11:37
# @Author  : liudongyang
# @FileName: schedule.py
# @Software: PyCharm
# 任务调度
import threading

from make_data import MakeData
from save_data import SaveFile
from parm import savenum, stifnum, filenum


def main(beg, end, stif_time, file_date_time):
    savedata = SaveFile()
    makedata = MakeData()
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
    # 临时数据变量名
    all_data = ["orgs", "relations", "ptxns", "dtxns", "stifs", "survey_info1", "survey_info2",
                "survey_info3"]
    # 表名，需和all_data一一对应。
    all_table_name = ["org", "relation", "ptxn", "dtxn", "stif",
                      "survey_info1", "survey_info2", "survey_info3"]
    save_ci = filenum//savenum  # 每个数据文件需要储存的次数
    sign_other = 0  # 其他表数量标识
    sign_stif = 0  # 交易数量标识
    sc_other = 0  # 其他表保存次数
    sc_stif = 0  # 交易保存次数
    stif_data_num = 1  # 交易数据文件编号
    file_ord = 1  # 其他数据文件名编号

    for num in range(beg, end):
        t_stan_org = makedata.make_stan_org(num)
        orgs.append(t_stan_org)
        t_stan_relation = makedata.make_stan_relation()
        relations.append(t_stan_relation)
        t_stan_stif = makedata.make_stan_stif(stif_time)
        stifs.append(t_stan_stif)
        t_stan_survey_info1 = makedata.make_stan_survey_info1()
        survey_info1.append(t_stan_survey_info1)
        t_stan_survey_info2 = makedata.make_stan_survey_info2()
        survey_info2.append(t_stan_survey_info2)
        t_stan_survey_info3 = makedata.make_stan_survey_info3()
        survey_info3.append(t_stan_survey_info3)
        # 单独生成交易数据
        for i in range(stifnum):
            t_stan_ptxn, all_dict_data = makedata.make_stan_ptxn(stif_time)
            ptxns.append(t_stan_ptxn)
            t_stan_dtxn = makedata.make_stan_dtxn(all_dict_data)
            dtxns.append(t_stan_dtxn)
            t_stan_txn = makedata.make_stan_txn(stif_time, all_dict_data)
            txns.append(t_stan_txn)

        # 除交易外的存储
        sign_other += 1
        if sign_other % savenum == 0:  # 符合条件，多线程存储
            sc_other += 1
            print('存储数据')
            threads = []
            for ind, dat in enumerate(all_data):
                if len(eval(dat)):
                    thr = threading.Thread(target=savedata.write_to_csv, args=(
                        eval(dat), all_table_name[ind], file_date_time, file_ord, sign_other))
                    thr.start()
                    threads.append(thr)
            if sc_other == save_ci:
                file_ord += 1
                sc_other = 0
                sign_other = 0
            for t in threads:
                t.join()

            for data in all_data:  # 清空已写入数据
                eval(data).clear()

        # 交易单独储存
        sign_stif += stifnum
        if (sign_stif) % savenum == 0:  # 符合条件，多线程存储
            sc_stif += 1
            print('存储交易数据')
            if len(txns):
                thr_stif = threading.Thread(target=savedata.write_to_csv, args=(
                    txns, "txn", file_date_time, stif_data_num, sign_stif))
                thr_stif.start()
                thr_stif.join()

            if sc_stif == save_ci:
                stif_data_num += 1
                sc_stif = 0
                sign_stif = 0

            txns.clear()  # 清空已写入交易数据


    if sign_other > 0:
        print('存储剩余数据')
        threads = []
        for ind, dat in enumerate(all_data):
            if eval(dat):
                thr = threading.Thread(target=savedata.write_to_csv, args=(
                    eval(dat), all_table_name[ind], file_date_time, file_ord,sign_other))
                thr.start()
                threads.append(thr)

        for t in threads:
            t.join()

        for data in all_data:  # 清空已写入数据
            eval(data).clear()

    if sign_stif > 0:
        print('存储剩余交易数据')
        if len(txns):
            thr_stif = threading.Thread(target=savedata.write_to_csv, args=(
                txns, "txn", file_date_time, stif_data_num,sign_stif))
            thr_stif.start()
            thr_stif.join()

        txns.clear()

