# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 11:37
# @Author  : liudongyang
# @FileName: schedule.py
# @Software: PyCharm
# 任务调度
import threading

from make_data import MakeData
from save_data import SaveFile
from parm import savenum, stifnum


def main(beg, end, stif_time, file_date_time):
    savedata = SaveFile()
    makedata = MakeData()
    orgs = []
    relations = []
    ptxns = []
    dtxns = []
    txns = []
    stifs = []
    survey_info1 = []
    survey_info2 = []
    survey_info3 = []
    all_data = ["orgs", "relations", "ptxns", "dtxns", "stifs", "survey_info1", "survey_info2",
                "survey_info3"]
    all_table_name = ["org", "relation", "ptxn", "dtxn", "stif",
                      "survey_info1", "survey_info2", "survey_info3"]
    # save_ci = 1000000//savenum
    save_ci = 100//savenum
    sign_other = 0
    sign_stif = 0
    sc_other = 0
    sc_stif = 0
    stif_data_num = 1
    file_num = 1
    for num in range(beg, end):
        t_stan_org = makedata.make_stan_org(num)
        orgs.append(t_stan_org)
        t_stan_relation = makedata.make_stan_relation()
        relations.append(t_stan_relation)
        t_stan_ptxn = makedata.make_stan_ptxn()
        ptxns.append(t_stan_ptxn)
        t_stan_dtxn = makedata.make_stan_dtxn()
        dtxns.append(t_stan_dtxn)
        t_stan_stif = makedata.make_stan_stif()
        stifs.append(t_stan_stif)
        t_stan_survey_info1 = makedata.make_stan_survey_info1()
        survey_info1.append(t_stan_survey_info1)
        t_stan_survey_info2 = makedata.make_stan_survey_info2()
        survey_info2.append(t_stan_survey_info2)
        t_stan_survey_info3 = makedata.make_stan_survey_info3()
        survey_info3.append(t_stan_survey_info3)
        for i in range(stifnum):
            t_stan_txn = makedata.make_stan_txn(stif_time)
            txns.append(t_stan_txn)

        # 除交易外的存储
        sign_other += 1
        if sign_other % savenum == 0:  # 符合条件，多线程存储
            sc_other += 1
            print('存储数据')
            threads = []
            for ind, dat in enumerate(all_data):
                if len(eval(dat)):
                    print(dat)

                    thr = threading.Thread(target=savedata.write_to_csv, args=(
                        eval(dat), all_table_name[ind], file_date_time, file_num))
                    thr.start()
                    threads.append(thr)
                    # savedata.write_to_csv(eval(dat), all_table_name[ind])
            if sc_other == save_ci:
                file_num += 1

            for t in threads:
                t.join()
            sign_other = 0

            for data in all_data:  # 清空已写入数据
                eval(data).clear()

        # 交易单独储存
        if (sign_stif * stifnum) % savenum == 0:  # 符合条件，多线程存储
            sc_stif += 1
            print('存储交易数据')
            if len(txns):
                thr = threading.Thread(target=savedata.write_to_csv, args=(
                    txns, "txn", file_date_time, stif_data_num))
                thr.start()
                thr.join()

            if sc_stif == save_ci:
                stif_data_num += 1

            sign_stif = 0
            txns.clear()  # 清空已写入交易数据


    if sign_other > 0:
        print('存储剩余数据')
        threads = []
        for ind, dat in enumerate(all_data):
            if eval(dat):
                thr = threading.Thread(target=savedata.write_to_csv, args=(
                    eval(dat), all_table_name[ind], file_date_time, file_num))
                thr.start()
                threads.append(thr)
                # savedata.write_to_csv(eval(dat), all_table_name[ind])

        for t in threads:
            t.join()
        # sign = 0

        for data in all_data:  # 清空已写入数据
            eval(data).clear()

    if sign_stif > 0:
        print('存储剩余交易数据')
        if len(txns):
            thr = threading.Thread(target=savedata.write_to_csv, args=(
                txns, "txn", file_date_time, stif_data_num))
            thr.start()
            thr.join()

        txns.clear()

