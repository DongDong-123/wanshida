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
from parm import savenum,trade_filenum, stifnum, filenum, zip_floder
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
mappings = []

def __threads(all_data, all_table_name, file_date_time, order, sign,delimiter, control_file_time):
    """
    抽出多线程部分
    :param all_data:  数据
    :param all_table_name:  文件名
    :param file_date_time:  文件时间
    :param order:  文件编号
    :param sign:  暂时没用
    :param delimiter:   分割符
    :param control_file_time: 控制文件时间
    :return:
    """

    threads = []
    for ind, dat in enumerate(all_data):
        if len(eval(dat)):
            if delimiter:
                thr = threading.Thread(target=savedata.write_to_csv, args=(
                    eval(dat), all_table_name[ind], file_date_time, order, sign,control_file_time, delimiter))
                thr.start()
                threads.append(thr)
            else:
                thr = threading.Thread(target=savedata.write_to_csv, args=(
                    eval(dat), all_table_name[ind], file_date_time, order, sign,control_file_time))
                thr.start()
                threads.append(thr)

    for t in threads:
        t.join()

def __control_file(file_name, file_date_time, file_num,filepath, control_file_time,data_num):
    if file_name == 'txn':
        file_full = os.path.join(filepath, 'TXN-D{}-T{}.txt'.format(
        file_date_time, control_file_time))
    elif file_name == 'mapping':
        file_full = os.path.join(filepath, 'MAPPING-D{}-T{}.txt'.format(
        file_date_time, control_file_time))
    elif file_name == 'dic':
        file_full = os.path.join(filepath, 'DIC-D{}-T{}.txt'.format(
        file_date_time, control_file_time))
    else:
        file_full = os.path.join(filepath, 'D{}-T{}.txt'.format(
        file_date_time, control_file_time))

    if file_num < 10:
        filename = '{}-D{}-T{}-000{}.csv'.format(file_name.upper(), file_date_time, control_file_time, file_num)
    else:
        filename = '{}-D{}-T{}-00{}.csv'.format(file_name.upper(), file_date_time, control_file_time, file_num)

    if file_name == 'txn' or file_name == 'mapping' or file_name == 'dic':
        with open(file_full, '+a', encoding="UTF-8") as f:
            # print('-----------------创建{}-------------------'.format(file_full))
            f.write('||'.join([filename, str(data_num)]) + "\n")
    else:
        with open(file_full, '+a', encoding="UTF-8") as f:
            # print('-----------------创建{}-------------------'.format(file_full))
            f.write(','.join([filename, str(data_num)]) + "\n")


def main(beg, end, stif_time, file_date_time):
    # 创建存储文件夹
    file_path1 = os.path.join(zip_floder, 'custom', file_date_time)
    file_path2 = os.path.join(zip_floder, 'txn', file_date_time)
    file_path3 = os.path.join(zip_floder, 'mapping', file_date_time)
    file_path4 = os.path.join(zip_floder, 'dic', file_date_time)

    if not os.path.exists(file_path1):
        os.makedirs(file_path1)
    if not os.path.exists(file_path2):
        os.makedirs(file_path2)
    if not os.path.exists(file_path3):
        os.makedirs(file_path3)
    if not os.path.exists(file_path4):
        os.makedirs(file_path4)
    # 控制文件时间戳
    control_file_time = round(time.time() * 1000)

    currt_time = time.strftime('%Y%m%d', time.localtime())
    update_date = comm.process_time(int(stif_time.replace("-",""))+1)
    random_time = comm.make_time()
    update_time = update_date+random_time.replace(':',"")

    makedata = MakeData(update_time)
    # 日期格式转换
    stif_time = comm.turn_date10(stif_time)

    # 临时数据变量名
    all_data = ["orgs", "relations", "survey_info1", "survey_info2", "survey_info3"]
    # 表名，需和all_data一一对应。
    # all_table_name = ["org", "relation", "survey_info1", "survey_info2", "survey_info3"]
    all_table_name = ["org", "relation", "info1", "info2", "info3"]
    save_ci = filenum//savenum  # 非交易数据文件需要储存的次数
    trade_save_ci = trade_filenum//savenum  # 交易数据文件需要储存的次数
    map_save_ci = trade_filenum*2//savenum  # map数据文件需要储存的次数
    sign_other = 0  # 其他表数量标识
    sign_txn = 0  # 交易数量标识
    sc_other = 0  # 其他表保存次数
    sc_stif = 0  # 交易保存次数
    stif_data_num = 1  # 交易数据文件编号
    file_ord = 1  # 其他数据文件名编号
    map_file_ord = 1  # map文件名编号
    sign_stif = 0  # 可疑交易数量
    sign_map = 0  # mapping文件数量
    sc_map = 0  # map保存次数

    for num in range(beg, end):
        t_stan_org = makedata.make_stan_org(num)
        customer_id = 'org_1_{}'.format(num)
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

            t_stan_txn, map_data = makedata.make_stan_txn(stif_time, all_dict_data)
            txns.append(t_stan_txn)
            mappings.extend([[customer_id, ica, 'l',currt_time] for ica in map_data])
            # ---------可疑交易数据，暂时不用---------------
            # if num %10 == 0:
            #     t_stan_stif = makedata.make_stan_stif(stif_time, all_dict_data)
            #     stifs.append(t_stan_stif)
            #     sign_stif += 1
            # -----------------------------------------------
        # 除交易外的存储
        sign_other += 1
        if sign_other % savenum == 0:  # 符合条件，多线程存储
            sc_other += 1
            print('{} 存储数据{}条，文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),savenum, file_ord))
            __threads(all_data, all_table_name, file_date_time, file_ord, sign_other,'', control_file_time)
            print(len(orgs),)
            if sc_other == save_ci:
                filepath = os.path.join(zip_floder, 'custom', file_date_time)
                for name in all_table_name:
                    __control_file(name, file_date_time, file_ord,filepath, control_file_time, filenum)
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

            __threads(["txns"], ["txn"], file_date_time, stif_data_num, sign_txn, '||', control_file_time)

            if sc_stif == trade_save_ci:
                filepath = os.path.join(zip_floder, 'txn', file_date_time)
                __control_file("txn", file_date_time, stif_data_num, filepath, control_file_time,trade_filenum)
                # file_full = os.path.join(data_path, 'D{}-T{}_00{}.txt'.format(
                #     file_date_time, currt_time, 1))
                # filename = '{}-D{}-T{}_00{}.csv'.format("txn", file_date_time, currt_time, stif_data_num)
                # with open(file_full, '+a', encoding="UTF-8") as f:
                #     f.write(','.join([filename, str(savenum)])+ "\n")
                stif_data_num += 1
                sc_stif = 0
                sign_txn = 0

            txns.clear()  # 清空已写入交易数据

        #   ---------------mapping文件--------------
        sign_map += stifnum*2
        if (sign_map) % savenum == 0:  # 符合条件，多线程存储
            sc_map += 1
            print('{} 存储map数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),savenum, map_file_ord))

            __threads(["mappings"], ["mapping"], file_date_time, map_file_ord, sign_map, 'map', control_file_time)

            if sc_map == map_save_ci:
                filepath = os.path.join(zip_floder, 'mapping', file_date_time)
                __control_file("mapping", file_date_time, map_file_ord, filepath, control_file_time,trade_filenum*2)

                map_file_ord += 1
                sc_map = 0
                sign_map = 0

            mappings.clear()  # 清空已写入交易数据
        #   ---------------mapping文件--------------

    if sign_other > 0:
        print('{} 存储剩余数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_other, file_ord))
        __threads(all_data, all_table_name, file_date_time, file_ord, sign_other,'', control_file_time)
        filepath = os.path.join(zip_floder, 'custom', file_date_time)
        for name in all_table_name:
            __control_file(name, file_date_time, file_ord, filepath, control_file_time,sign_other)
        for data in all_data:  # 清空已写入数据
            eval(data).clear()

    if sign_txn > 0:
        print('{} 存储剩余交易数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_txn, stif_data_num))
        __threads(["txns"], ["txn"], file_date_time, stif_data_num, sign_txn,'||', control_file_time)
        filepath = os.path.join(zip_floder, 'txn', file_date_time)
        __control_file("txn", file_date_time, stif_data_num,filepath, control_file_time,sign_txn)

        txns.clear()

    if sign_map > 0:
        print('{} 存储剩余map数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_map, map_file_ord))
        __threads(["mappings"], ["mapping"], file_date_time, map_file_ord, sign_map,'||', control_file_time)
        filepath = os.path.join(zip_floder, 'mapping', file_date_time)
        __control_file("mapping", file_date_time, map_file_ord,filepath, control_file_time,sign_map)

        mappings.clear()

    # dic 文件
    __control_file('dic', file_date_time, 1, file_path4, control_file_time, 1)

    # -----------------可疑交易存储--------------------------------
    # if sign_stif > 0:
    #     print('{} 存储可疑交易数据{}条,文件编号{}'.format(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),sign_stif, 1))
    #     __threads(["stifs"], ["stif"], file_date_time, 1, sign_stif,'')
    #     __control_file("stif", file_date_time, sign_stif, filepath)
    #
    #     txns.clear()
    # -------------------------------------------------------------

