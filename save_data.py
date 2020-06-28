# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 11:54
# @Author  : liudongyang
# @FileName: save_data.py
# @Software: PyCharm
# 存储数据
import pymysql
import os, csv
from readconfig import ReadMySqlConfig
conf = ReadMySqlConfig()
t_stan_org = ("busi_reg_no", "ctnm", "ctsnm", "cten", "ctsen", "busi_name", "appli_country", "sub_company", "former_name", "citp", "citp_nt", "ctid", "ctid_edt", "state", "city", "address", "post_code", "tel", "fax", "m_state", "m_city", "m_address", "m_post_code", "m_tel", "m_fax", "pr_mr_ms", "pr_name", "pr_title", "pr_phone", "pr_fax", "pr_email", "pr_address", "sec_mr_ms", "sec_name", "sec_title", "sec_phone", "sec_fax", "sec_email", "sec_address", "aml_mr_ms", "aml_name", "aml_title", "aml_phone", "aml_fax", "aml_email", "aml_address", "client_tp", "lfa_type", "lfa_type_explain", "fud_date", "assets_size", "country", "other_oper_country", "desc_business", "tin", "busi_type", "ctvc", "indu_code", "indu_code_nt", "crnm", "crit", "crit_nt", "crid", "crid_edt", "reg_cptl", "reg_cptl_code", "remark_ctvc", "eecp", "scale", "rgdt", "cls_dt", "unit_code", "remark", "stat_flag_ori", "stat_flag", "mer_unit", "cmgr", "act_cd", "acc_type1", "bank_acc_name", "cabm", "country", "statement_type", "reals", "complex", "clear", "data_crdt", "data_cruser", "data_updt", "data_upuser")
t_stan_relation = ("ctif_id", "ctnm", "rel_tp", "rel_layer", "rel_cstp", "fir_name", "sec_name", "last_name", "citp", "citp_nt", "ctid", "ctid_edt", "rcnt", "dob", "cob", "years_comp", "years_indu", "rel_prov", "rel_city", "rel_area", "rear", "retl", "ret_mphone", "rel_fax", "rel_email", "gov_owned", "hold_per", "hold_amt", "remark", "data_crdt", "data_cruser", "data_updt", "data_upuser")
t_stan_ptxn = ("msg_id", "msg_type", "inter_tran_type", "uuid", "trace_id", "tran_group_id", "tran_init", "tran_res", "card_bin", "card_type", "card_product", "card_brand", "card_media", "token_pan", "encrypt_pan", "hash_pan", "digsit", "crdhldr_tran_type", "crdhldr_acc_tp_from", "crdhldr_acc_tp_to", "tran_amount", "sett_amount", "bill_amount", "tran_datetime", "crdhldr_bill_fee", "sett_conv_rate", "bill_conv_rate", "sys_trace_audit_nbr", "local_tran_datetime", "exp_date", "sett_date", "conv_date", "mcc", "pos_entry_cd", "card_seq_num", "pos_pin_cptr_cd", "tran_fee_indi", "acq_srchg_amount", "acq_ins_id_cd", "fwd_ins_id_cd", "trk2_prsnt_sw", "retriv_ref_num", "auth_cd", "resp_cd", "pos_term_id", "acq_merch_id", "acq_merch_name", "acq_merch_city", "acq_merch_state", "frmt_resp_data", "additional_data", "funding_payment_tti", "tran_curr_cd", "sett_curr_cd", "bill_curr_cd", "data_integrated", "paym_account", "advice_reason_cd", "advice_reason_dt_cd", "advice_reason_dt_txt", "advice_reason_add_txt", "pos_data", "pos_crdhldr_present", "pos_tran_status", "inf_data", "ntw_mng_inf_cd", "org_mti", "org_stan", "org_tran_datetime", "org_acq_ins_id_cd", "org_fwd_ins_id_cd", "org_trace_id", "rcv_ins_id_cd", "iss_mti_cd", "iss_pcode", "iss_ins_id_cd", "acq_msg_flag", "iss_msg_flag", "single_dual_flag", "tran_buss_st", "tran_advice_st", "inter_resp_cd", "dc_id", "insert_timestamp", "insert_by", "last_update_timestamp", "last_update_by", "channel_type", "cash_back_amount", "cash_back_indicator", "mcht_data_srv", "tcc", "cvv2", "pos_cat_level", "merch_advic_cd", "src_member_id", "dest_member_id", "group_tran_type", "fee_category", "fan_ntw_cd", "int_rate_id", "net_ref_num", "bnk_ref_num", "acq_ref_num", "gcms_prc_num", "act_tran_amount", "act_sett_amount", "act_bill_amount", "zero_fill_amount", "reserve1", "reserve2", "reserve3", "data_transfer_dt")
t_stan_dtxn = ("batclr_sngl_dspt_msg_id", "dspt_sys_id", "orig_trace_id", "card_type", "card_product", "card_brand", "token_pan", "encrypt_pan", "crdhldr_tran_type", "crdhldr_acc_tp_from", "crdhldr_acc_tp_to", "sett_conv_rate", "dspt_trace_aud_num", "orig_local_tran_datetime", "sett_date", "mcc", "pos_entry_cd", "retriv_ref_num", "auth_cd", "resp_cd", "pos_term_id", "tran_curr_cd", "sett_curr_cd", "dspt_advic_rsn_cd", "dspt_advic_rsn_dtl_cd", "org_stan", "channel_type", "cash_back_amount", "orig_tran_type", "dspt_tran_type", "send_ica", "rcvr_ica", "send_rl", "rcvr_rl", "dspt_tran_amt", "dspt_setl_amt", "orig_sett_date", "db_cr_flag", "tran_amt", "setl_amt", "actl_tran_amt", "setl_tran_amt", "cash_back_indicator", "mcht_data_srv", "dspt_ref_num", "insert_timestamp", "last_update_timestamp", "reserve1", "reserve2", "reserve3", "version", "case_id", "msg_rev_ind", "dspt_tran_dttm", "data_transfer_dt")
t_stan_txn = ("id", "tran_kd", "uuid", "trace_id", "card_bin", "card_type", "card_type_pboc", "card_product", "card_brand", "token_pan", "encrypt_pan", "crdhldr_tran_type", "crdhldr_acc_tp_from", "crdhldr_acc_tp_to", "tran_datetime", "orig_local_tran_datetime", "tsdr", "tran_amount", "sett_amount", "tran_curr_cd", "sett_curr_cd", "sett_conv_rate", "sett_date", "crat_u", "crat_c", "mcc", "pos_entry_cd", "retriv_ref_num", "auth_cd", "resp_cd", "pos_term_id", "rcv_ins_id_cd", "iss_mti_cd", "iss_pcode", "iss_ins_id_cd", "acq_merch_id", "acq_merch_name", "acq_merch_city", "acq_merch_state", "acq_ins_id_cd", "fwd_ins_id_cd", "TRCD", "CBIF", "channel_type", "TSTP", "cash_back_amount", "cash_back_indicator", "tran_type", "dspt_tran_type", "org_stan", "tran_buss_st", "tran_advice_st", "mcht_data_srv", "additional_data", "insert_timestamp", "insert_by", "last_update_timestamp", "last_update_by", "mer_unit", "data_transfer_dt")
t_stan_stif = ("unit_code", "warn_dt", "rule_id", "rule_type", "warn_kd", "susp_value", "ctif_tp", "tran_kd", "card_type", "MCNO", "MCNM", "ACCD", "fwd_ins_id_cd", "STCT", "card_product", "card_brand", "STCI", "IUCD", "rcv_ins_id_cd", "tstm", "tsdr", "TCPP", "TCTP", "TCAT", "TCMN", "TCNM", "CACD", "c_fwd_ins_id_cd", "TCCT", "T_card_product", "T_card_brand", "TCCI", "TCIC", "c_rcv_ins_id_cd", "bptc", "ticd", "busi_type", "trans_type", "trans_stat", "tran_advice_st", "acq_merch_city", "acq_merch_state", "TRCD", "CBIF", "trans_channel", "PCTP", "PCAT", "crat_u", "crat_c", "TSTP", "mcc", "pos_entry_cd", "retriv_ref_num", "auth_cd", "resp_cd", "pos_term_id", "mer_unit", "run_dt", "data_transfer_dt")





class ConnectMysql:
    def __init__(self):
        self.host = conf.host()
        self.user = conf.user()
        self.passwd = conf.passwd()
        self.db = conf.db()
        self.port = conf.port()


    def save_to_mysql(self, datas, table_name):
        conn = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.db, port=self.port, charset="utf8")
        curs = conn.cursor()
        for data in datas:
            sql = "insert into {} {} VALUES {}".format(table_name, eval(table_name), tuple(data))
            curs.execute(sql)

        try:
            conn.commit()
        except Exception as e:
            print(e)

        curs.close()
        conn.close()





class SaveFile:
    def __init__(self, path=None):
        self.file_path = path
        self.t_stan_org = t_stan_org
        self.t_stan_relation = t_stan_relation
        self.t_stan_ptxn = t_stan_ptxn
        self.t_stan_dtxn = t_stan_dtxn
        self.t_stan_txn = t_stan_txn
        self.t_stan_stif = t_stan_stif


    def write_to_csv(self, datas, file_name):
        if not self.file_path:
            self.file_path = os.path.join(os.getcwd(), 'datas')
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)

        file_full = os.path.join(self.file_path, (file_name + '.csv'))
        if not os.path.exists(file_full):
            title = eval('self.' + file_name)
            csvfile = open(file_full, 'a', encoding="utf-8-sig", newline='')
            writer = csv.writer(csvfile)
            writer.writerow(title)
            csvfile.close()

        csvfile = open(file_full, 'a', encoding="utf-8-sig", newline='')
        writer = csv.writer(csvfile)
        writer.writerows(datas)
        csvfile.close()