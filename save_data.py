# -*- coding: utf-8 -*-
# @Time    : 2020-06-25 11:54
# @Author  : liudongyang
# @FileName: save_data.py
# @Software: PyCharm
# 存储数据
import pymysql
import os, csv
from readconfig import ReadMySqlConfig
from parm import zip_floder
import time
import copy
conf = ReadMySqlConfig()
# t_stan_org = ("busi_reg_no", "ctnm", "ctsnm", "cten", "ctsen", "busi_name", "appli_country", "sub_company", "former_name", "citp", "citp_nt", "ctid", "ctid_edt", "state", "city", "address", "post_code", "tel", "fax", "m_state", "m_city", "m_address", "m_post_code", "m_tel", "m_fax", "pr_mr_ms", "pr_name", "pr_title", "pr_phone", "pr_fax", "pr_email", "pr_address", "sec_mr_ms", "sec_name", "sec_title", "sec_phone", "sec_fax", "sec_email", "sec_address", "aml_mr_ms", "aml_name", "aml_title", "aml_phone", "aml_fax", "aml_email", "aml_address", "client_tp", "lfa_type", "lfa_type_explain", "fud_date", "assets_size", "country", "other_oper_country", "desc_business", "tin", "busi_type", "ctvc", "indu_code", "indu_code_nt", "crnm", "crit", "crit_nt", "crid", "crid_edt", "crid_country", "reg_cptl", "reg_cptl_code", "remark_ctvc", "eecp", "scale", "rgdt", "cls_dt", "unit_code", "remark", "stat_flag_ori", "stat_flag", "mer_unit", "cmgr", "reals", "complex", "clear", "data_crdt", "data_cruser", "data_updt", "data_upuser")

# t_stan_org = [
#     'csnm', 'custormer_name', 'custormer_sname', 'custormer_ename', 'custormer_sename', 'busi_name', 'appli_country', 'sub_company', 'former_name', 'cert_tp', 'cert_tp_explain', 'cert_num', 'cert_validity', 'state', 'city', 'address', 'post_code', 'tel', 'fax', 'm_state', 'm_city', 'm_address', 'm_post_code', 'm_tel', 'm_fax', 'pr_mr_ms', 'pr_name', 'pr_title', 'pr_phone', 'pr_fax', 'pr_email', 'pr_address', 'sec_mr_ms', 'sec_name', 'sec_title', 'sec_phone', 'sec_fax', 'sec_email', 'sec_address', 'aml_mr_ms', 'aml_name', 'aml_title', 'aml_phone', 'aml_fax', 'aml_email', 'aml_address', 'client_tp', 'lfa_type', 'lfa_type_explain', 'found_date', 'assets_size', 'country', 'other_oper_country', 'desc_business', 'tin', 'busi_type', 'industry_type', 'indu_code', 'indu_code_nt', 'legal_p_name', 'legal_p_ename', 'legal_p_cert_tp', 'legal_p_cert_explain', 'legal_p_cert_num', 'legal_cert_validity', 'crid_country', 'registered_capital', 'registered_capital_currency', 'business_scope', 'enps_ecic_sectors', 'scale', 'establish_busi_date', 'end_busi_date', 'unit_code', 'remark', 'stat_flag_ori', 'stat_flag', 'mer_unit', 'account_manager', 'reals', 'complex', 'clear', 'create_time', 'update_time', 'creator', 'updator'
# ]
# t_stan_relation = [
#     'ctif_id', 'ctnm', 'rel_tp', 'rel_layer', 'rel_cstp', 'fir_name', 'sec_name', 'last_name', 'citp', 'citp_nt', 'ctid', 'ctid_edt', 'rcnt', 'dob', 'cob', 'years_comp', 'years_indu', 'rel_prov', 'rel_city', 'rel_area', 'rear', 'retl', 'ret_mphone', 'rel_fax', 'rel_email', 'gov_owned', 'hold_per', 'hold_amt', 'remark', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
# ]
# t_stan_ptxn = [
#     'msg_id', 'msg_type', 'inter_tran_type', 'uuid', 'trace_id', 'tran_group_id', 'tran_init', 'tran_res', 'card_bin', 'card_type', 'card_product', 'card_brand', 'card_media', 'token_pan', 'encrypt_pan', 'hash_pan', 'digsit', 'crdhldr_tran_type', 'crdhldr_acc_tp_from', 'crdhldr_acc_tp_to', 'tran_amount', 'sett_amount', 'bill_amount', 'tran_datetime', 'crdhldr_bill_fee', 'sett_conv_rate', 'bill_conv_rate', 'sys_trace_audit_nbr', 'local_tran_datetime', 'exp_date', 'sett_date', 'conv_date', 'mcc', 'pos_entry_cd', 'card_seq_num', 'pos_pin_cptr_cd', 'tran_fee_indi', 'acq_srchg_amount', 'acq_ins_id_cd', 'fwd_ins_id_cd', 'trk2_prsnt_sw', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'acq_merch_id', 'acq_merch_name', 'acq_merch_city', 'acq_merch_state', 'frmt_resp_data', 'additional_data', 'funding_payment_tti', 'tran_curr_cd', 'sett_curr_cd', 'bill_curr_cd', 'data_integrated', 'paym_account', 'advice_reason_cd', 'advice_reason_dt_cd', 'advice_reason_dt_txt', 'advice_reason_add_txt', 'pos_data', 'pos_crdhldr_present', 'pos_tran_status', 'inf_data', 'ntw_mng_inf_cd', 'org_mti', 'org_stan', 'org_tran_datetime', 'org_acq_ins_id_cd', 'org_fwd_ins_id_cd', 'org_trace_id', 'rcv_ins_id_cd', 'iss_mti_cd', 'iss_pcode', 'iss_ins_id_cd', 'acq_msg_flag', 'iss_msg_flag', 'single_dual_flag', 'tran_buss_st', 'tran_advice_st', 'inter_resp_cd', 'dc_id', 'insert_timestamp', 'insert_by', 'last_update_timestamp', 'last_update_by', 'channel_type', 'cash_back_amount', 'cash_back_indicator', 'mcht_data_srv', 'tcc', 'cvv2', 'pos_cat_level', 'merch_advic_cd', 'src_member_id', 'dest_member_id', 'group_tran_type', 'fee_category', 'fan_ntw_cd', 'int_rate_id', 'net_ref_num', 'bnk_ref_num', 'acq_ref_num', 'gcms_prc_num', 'act_tran_amount', 'act_sett_amount', 'act_bill_amount', 'zero_fill_amount', 'reserve1', 'reserve2', 'reserve3', 'data_transfer_dt'
# ]
# t_stan_dtxn = [
#     'batclr_sngl_dspt_msg_id', 'dspt_sys_id', 'orig_trace_id', 'card_type', 'card_product', 'card_brand', 'token_pan', 'encrypt_pan', 'crdhldr_tran_type', 'crdhldr_acc_tp_from', 'crdhldr_acc_tp_to', 'sett_conv_rate', 'dspt_trace_aud_num', 'orig_local_tran_datetime', 'sett_date', 'mcc', 'pos_entry_cd', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'tran_curr_cd', 'sett_curr_cd', 'dspt_advic_rsn_cd', 'dspt_advic_rsn_dtl_cd', 'org_stan', 'channel_type', 'cash_back_amount', 'orig_tran_type', 'dspt_tran_type', 'send_ica', 'rcvr_ica', 'send_rl', 'rcvr_rl', 'dspt_tran_amt', 'dspt_setl_amt', 'orig_sett_date', 'db_cr_flag', 'tran_amt', 'setl_amt', 'actl_tran_amt', 'setl_tran_amt', 'cash_back_indicator', 'mcht_data_srv', 'dspt_ref_num', 'insert_timestamp', 'last_update_timestamp', 'reserve1', 'reserve2', 'reserve3', 'version', 'case_id', 'msg_rev_ind', 'dspt_tran_dttm', 'data_transfer_dt'
# ]
# t_stan_txn = [
#     'id', 'tran_kd', 'uuid', 'trace_id', 'card_bin', 'card_type', 'card_type_pboc', 'card_product', 'card_brand', 'token_pan', 'encrypt_pan', 'crdhldr_tran_type', 'crdhldr_acc_tp_from', 'crdhldr_acc_tp_to', 'tran_datetime', 'orig_local_tran_datetime', 'tsdr', 'tran_amount', 'sett_amount', 'tran_curr_cd', 'sett_curr_cd', 'sett_conv_rate', 'sett_date', 'crat_u', 'crat_c', 'mcc', 'pos_entry_cd', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'rcv_ins_id_cd', 'iss_mti_cd', 'iss_pcode', 'iss_ins_id_cd', 'acq_merch_id', 'acq_merch_name', 'acq_merch_city', 'acq_merch_state', 'acq_ins_id_cd', 'fwd_ins_id_cd', 'TRCD', 'CBIF', 'channel_type', 'TSTP', 'cash_back_amount', 'cash_back_indicator', 'tran_type', 'dspt_tran_type', 'org_stan', 'tran_buss_st', 'tran_advice_st', 'mcht_data_srv', 'additional_data', 'insert_timestamp', 'insert_by', 'last_update_timestamp', 'last_update_by', 'mer_unit', 'data_transfer_dt'
# ]
# t_stan_stif = [
#     'unit_code', 'warn_dt', 'rule_id', 'rule_type', 'warn_kd', 'susp_value', 'ctif_tp', 'tran_kd', 'card_type', 'MCNO', 'MCNM', 'ACCD', 'fwd_ins_id_cd', 'STCT', 'card_product', 'card_brand', 'STCI', 'IUCD', 'rcv_ins_id_cd', 'tstm', 'tsdr', 'TCPP', 'TCTP', 'TCAT', 'TCMN', 'TCNM', 'CACD', 'c_fwd_ins_id_cd', 'TCCT', 'T_card_product', 'T_card_brand', 'TCCI', 'TCIC', 'c_rcv_ins_id_cd', 'bptc', 'ticd', 'busi_type', 'trans_type', 'trans_stat', 'tran_advice_st', 'acq_merch_city', 'acq_merch_state', 'TRCD', 'CBIF', 'trans_channel', 'PCTP', 'PCAT', 'crat_u', 'crat_c', 'TSTP', 'mcc', 'pos_entry_cd', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'mer_unit', 'run_dt', 'data_transfer_dt'
# ]
# t_stan_info1 = [
#     'ctif_id', 'ctnm', 'info_a_bool', 'laws_name', 'info_a_bool2', 'info_a_bool3', 'supervisor_name', 'inspection_time', 'info_a_explain', 'info_a_explain2', 'info_b_bool', 'info_b_bool2', 'info_b_bool3', 'info_b_explain', 'info_c_bool', 'info_c_explain', 'info_d_bool', 'info_d_bool2', 'info_d_explain', 'payment_card_org ', 'compliance_org', 'chartered_institution', 'info_e_bool', 'info_e_bool2', 'info_e_bool3', 'supervision_trace_doc', 'info_f_bool', 'list_type', 'other_list_type', 'info_f_explain', 'info_g_bool', 'info_g_explain', 'info_h_bool', 'info_h_explain', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
# ]
# t_stan_info2 = [
#     'ctif_id', 'ctnm', 'info2_a_bool', 'info2_a_explain', 'info2_b_bool', 'info2_b_explain', 'agents_num', 'aml_role_explain', 'compliance_name', 'aml_workers', 'aml_position', 'info2_c_bool', 'info2_c_bool2', 'info2_c_explain', 'info2_d_bool', 'info2_d_explain', 'info2_e_bool', 'info2_f_bool', 'info2_g_bool', 'info2_g_explain', 'info2_h_bool', 'info2_h_explain', 'info2_i_bool', 'info2_i_explain', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
# ]
# t_stan_info3 = [
#     'ctif_id', 'ctnm', 'fi_mcard_principal', 'fi_mcard_affillate', 'fi_mcard_association', 'fi_mcard_issuing', 'fi_mcard_acquiring_merchants', 'fi_mcard_acquiring_atm', 'fi_mcard_acquiring_mcd', 'fi_mcard_optrpt_msd', 'fi_mcard_optrpt_ms', 'fi_mcard_optrpt_mscb', 'fi_mcard_optrpt_mpqr', 'fi_mstro_principal', 'fi_mstro_affillate', 'fi_mstro_issuing', 'fi_mstro_acquiring_merchants', 'fi_mstro_acquiring_atm', 'fi_mstro_optrpt_msd', 'fi_mstro_optrpt_ms', 'fi_mstro_optrpt_mscb', 'fi_mstro_optrpt_mpqr', 'fi_cirrus_principal', 'fi_cirrus_affillate', 'fi_cirrus_issuing_atm', 'fi_cirrus_acquiring_atm', 'fi_cirrus_optp2p_ms', 'fi_cirrus_optp2p_mscb', 'fi_cirrus_optp2p_mpqr', 'cgi_mcard_principal', 'cgi_mcard_affillate', 'cgi_mcard_issuing_credit', 'cgi_mcard_issuing_debit', 'cgi_mcard_issuing_prepaid', 'cgi_mcard_acquiring_atm', 'cgi_mcard_acquiring_mcd', 'cgi_mcard_acquiring_merchants', 'cgi_mcard_acquiring_poi', 'cgi_mcard_optrpt_msd', 'cgi_mcard_optrpt_ms', 'cgi_mcard_optrpt_mscb', 'cgi_mcard_optrpt_mpqr', 'cgi_mstro_principal', 'cgi_mstro_affillate', 'cgi_mstro_issuing_debit', 'cgi_mstro_issuing_prepaid', 'cgi_mstro_acquiring_atm', 'cgi_mstro_acquiring_merchants', 'cgi_mstro_acquiring_poi', 'cgi_mstro_optrpt_msd', 'cgi_mstro_optrpt_ms', 'cgi_mstro_optrpt_mscb', 'cgi_mstro_optrpt_mpqr', 'cgi_cirrus_principal', 'cgi_cirrus_affillate', 'cgi_cirrus__issuing', 'cgi_cirrus_acquiring_atm', 'cgi_cirrus_optp2p_ms', 'cgi_cirrus_optp2p_mscb', 'cgi_cirrus_optp2p_mpqr', 'info_a_bool', 'info_a_explain', 'additional_services_transfer', 'acquiring_rePower', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
# ]

t_stan_org = [
    'csnm', 'custormer_name','custormer_ename', 'custormer_sename', 'busi_name', 'appli_country', 'sub_company', 'former_name', 'cert_tp', 'cert_tp_explain', 'cert_num', 'cert_validity', 'state', 'city', 'address', 'post_code', 'tel', 'fax', 'pr_mr_ms', 'pr_name', 'pr_title', 'pr_phone', 'pr_fax', 'pr_email', 'sec_mr_ms', 'sec_name', 'sec_title', 'sec_phone', 'sec_fax', 'sec_email', 'aml_mr_ms', 'aml_name', 'aml_title', 'aml_phone', 'aml_fax', 'aml_email', 'client_tp', 'lfa_type', 'found_date', 'assets_size', 'country', 'other_oper_country', 'desc_business', 'busi_type', 'industry_type', 'legal_p_name', 'legal_p_ename', 'legal_p_cert_tp', 'legal_p_cert_explain', 'legal_p_cert_num', 'legal_cert_validity', 'registered_capital', 'registered_capital_currency', 'business_scope','establish_busi_date', 'end_busi_date', 'stat_flag_ori', 'stat_flag', 'mer_unit', 'account_manager', 'create_time', 'creator', 'update_time', 'updator'
]
t_stan_relation = [
    'ctif_id', 'ctnm', 'rel_tp', 'rel_cstp', 'fir_name', 'sec_name', 'last_name', 'dob', 'cob', 'years_comp', 'years_indu','hold_per', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
]
t_stan_ptxn = [
    'msg_id', 'msg_type', 'inter_tran_type', 'uuid', 'trace_id', 'tran_group_id', 'tran_init', 'tran_res', 'card_bin', 'card_type', 'card_product', 'card_brand', 'card_media', 'token_pan', 'encrypt_pan', 'hash_pan', 'digsit', 'crdhldr_tran_type', 'crdhldr_acc_tp_from', 'crdhldr_acc_tp_to', 'tran_amount', 'sett_amount', 'bill_amount', 'tran_datetime', 'crdhldr_bill_fee', 'sett_conv_rate', 'bill_conv_rate', 'sys_trace_audit_nbr', 'local_tran_datetime', 'exp_date', 'sett_date', 'conv_date', 'mcc', 'pos_entry_cd', 'card_seq_num', 'pos_pin_cptr_cd', 'tran_fee_indi', 'acq_srchg_amount', 'acq_ins_id_cd', 'fwd_ins_id_cd', 'trk2_prsnt_sw', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'acq_merch_id', 'acq_merch_name', 'acq_merch_city', 'acq_merch_state', 'frmt_resp_data', 'additional_data', 'funding_payment_tti', 'tran_curr_cd', 'sett_curr_cd', 'bill_curr_cd', 'data_integrated', 'paym_account', 'advice_reason_cd', 'advice_reason_dt_cd', 'advice_reason_dt_txt', 'advice_reason_add_txt', 'pos_data', 'pos_crdhldr_present', 'pos_tran_status', 'inf_data', 'ntw_mng_inf_cd', 'org_mti', 'org_stan', 'org_tran_datetime', 'org_acq_ins_id_cd', 'org_fwd_ins_id_cd', 'org_trace_id', 'rcv_ins_id_cd', 'iss_mti_cd', 'iss_pcode', 'iss_ins_id_cd', 'acq_msg_flag', 'iss_msg_flag', 'single_dual_flag', 'tran_buss_st', 'tran_advice_st', 'inter_resp_cd', 'dc_id', 'insert_timestamp', 'insert_by', 'last_update_timestamp', 'last_update_by', 'channel_type', 'cash_back_amount', 'cash_back_indicator', 'mcht_data_srv', 'tcc', 'cvv2', 'pos_cat_level', 'merch_advic_cd', 'src_member_id', 'dest_member_id', 'group_tran_type', 'fee_category', 'fan_ntw_cd', 'int_rate_id', 'net_ref_num', 'bnk_ref_num', 'acq_ref_num', 'gcms_prc_num', 'act_tran_amount', 'act_sett_amount', 'act_bill_amount', 'zero_fill_amount', 'reserve1', 'reserve2', 'reserve3', 'data_transfer_dt'
]
t_stan_dtxn = [
    'batclr_sngl_dspt_msg_id', 'dspt_sys_id', 'orig_trace_id', 'card_type', 'card_product', 'card_brand', 'token_pan', 'encrypt_pan', 'crdhldr_tran_type', 'crdhldr_acc_tp_from', 'crdhldr_acc_tp_to', 'sett_conv_rate', 'dspt_trace_aud_num', 'orig_local_tran_datetime', 'sett_date', 'mcc', 'pos_entry_cd', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'tran_curr_cd', 'sett_curr_cd', 'dspt_advic_rsn_cd', 'dspt_advic_rsn_dtl_cd', 'org_stan', 'channel_type', 'cash_back_amount', 'orig_tran_type', 'dspt_tran_type', 'send_ica', 'rcvr_ica', 'send_rl', 'rcvr_rl', 'dspt_tran_amt', 'dspt_setl_amt', 'orig_sett_date', 'db_cr_flag', 'tran_amt', 'setl_amt', 'actl_tran_amt', 'setl_tran_amt', 'cash_back_indicator', 'mcht_data_srv', 'dspt_ref_num', 'insert_timestamp', 'last_update_timestamp', 'reserve1', 'reserve2', 'reserve3', 'version', 'case_id', 'msg_rev_ind', 'dspt_tran_dttm', 'data_transfer_dt'
]
t_stan_txn = [
    'tran_kd', 'uuid', 'trace_id', 'card_bin', 'card_type', 'card_type_pboc', 'card_product', 'card_brand', 'token_pan', 'encrypt_pan', 'crdhldr_tran_type', 'crdhldr_acc_tp_from', 'crdhldr_acc_tp_to', 'tran_datetime', 'orig_local_tran_datetime', 'tsdr', 'tran_amount', 'sett_amount', 'tran_curr_cd', 'sett_curr_cd', 'sett_conv_rate', 'sett_date', 'crat_u', 'crat_c', 'mcc', 'pos_entry_cd', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'rcv_ins_id_cd', 'iss_mti_cd', 'iss_pcode', 'iss_ins_id_cd', 'acq_merch_id', 'acq_merch_name', 'acq_merch_city', 'acq_merch_state', 'acq_ins_id_cd', 'fwd_ins_id_cd', 'TRCD', 'CBIF', 'channel_type', 'TSTP', 'cash_back_amount', 'cash_back_indicator', 'tran_type', 'dspt_tran_type', 'org_stan', 'tran_buss_st', 'tran_advice_st', 'mcht_data_srv', 'additional_data', 'insert_timestamp', 'insert_by', 'last_update_timestamp', 'last_update_by', 'mer_unit', 'data_transfer_dt'
]
t_stan_stif = [
    'unit_code', 'warn_dt', 'rule_id', 'rule_type', 'warn_kd', 'susp_value', 'ctif_tp', 'tran_kd', 'card_type', 'MCNO', 'MCNM', 'ACCD', 'fwd_ins_id_cd', 'STCT', 'card_product', 'card_brand', 'STCI', 'IUCD', 'rcv_ins_id_cd', 'tstm', 'tsdr', 'TCPP', 'TCTP', 'TCAT', 'TCMN', 'TCNM', 'CACD', 'c_fwd_ins_id_cd', 'TCCT', 'T_card_product', 'T_card_brand', 'TCCI', 'TCIC', 'c_rcv_ins_id_cd', 'bptc', 'ticd', 'busi_type', 'trans_type', 'trans_stat', 'tran_advice_st', 'acq_merch_city', 'acq_merch_state', 'TRCD', 'CBIF', 'trans_channel', 'PCTP', 'PCAT', 'crat_u', 'crat_c', 'TSTP', 'mcc', 'pos_entry_cd', 'retriv_ref_num', 'auth_cd', 'resp_cd', 'pos_term_id', 'mer_unit', 'run_dt', 'data_transfer_dt'
]
t_stan_info1 = [
    'ctif_id', 'ctnm', 'info_a_bool', 'laws_name', 'info_a_bool2', 'info_a_bool3', 'supervisor_name', 'info_a_explain', 'info_a_explain2', 'info_b_bool', 'info_b_bool2', 'info_b_bool3', 'info_b_explain', 'info_d_bool', 'info_d_bool2', 'info_d_explain', 'payment_card_org ', 'compliance_org', 'chartered_institution', 'info_e_bool', 'info_e_bool2', 'info_e_bool3', 'info_f_bool', 'list_type', 'other_list_type', 'info_g_bool','info_h_bool', 'info_h_explain', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
]
t_stan_info2 = [
    'ctif_id', 'ctnm', 'info2_a_bool', 'info2_a_explain', 'info2_b_bool', 'info2_b_explain', 'agents_num',  'compliance_name', 'aml_position', 'info2_c_bool', 'info2_e_bool', 'info2_f_bool', 'info2_h_bool', 'info2_h_explain', 'info2_i_bool', 'info2_i_explain', 'data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
]
t_stan_info3 = [
    'ctif_id', 'ctnm', 'fi_mcard_issuing', 'fi_mcard_acquiring_merchants', 'fi_mcard_acquiring_atm', 'fi_mcard_acquiring_mcd','data_crdt', 'data_cruser', 'data_updt', 'data_upuser'
]

t_stan_mapping = ["cid", "ica", "status", "create_time"]

t_stan_dic = ["type", "code", "name", "ename", "insert_timestamp", "last_update_timestamp", "data_transfer_dt"]
# t_stan_dic = ["type", "code", "name", "ename"]

class ConnectMysql:
    def __init__(self):
        self.host = conf.host()
        self.user = conf.user()
        self.passwd = conf.passwd()
        self.db = conf.db()
        self.port = int(conf.port())


    def save_to_mysql(self, datas, table_name):
        conn = pymysql.connect(host=self.host, user=self.user, password=self.passwd, db=self.db, port=self.port, charset="utf8")
        curs = conn.cursor()
        for data_t in datas:
            if table_name == 't_stan_txn':
                data_tmp = copy.deepcopy(data_t)
                data_tmp.insert(0,0)
            else:
                data_tmp = data_t

            sql = "insert into {} VALUES {}".format(table_name, tuple(data_tmp))
            print("sql", sql)
            curs.execute(sql)

        try:
            conn.commit()
        except Exception as e:
            print(e)

        curs.close()
        conn.close()


class SaveFile:
    def __init__(self):
        self.file_path = zip_floder
        self.currt_time = time.strftime('%Y%m%d', time.localtime())
        self.t_stan_org = t_stan_org
        self.t_stan_relation = t_stan_relation
        self.t_stan_ptxn = t_stan_ptxn
        self.t_stan_dtxn = t_stan_dtxn
        self.t_stan_txn = t_stan_txn
        self.t_stan_stif = t_stan_stif
        self.t_stan_info1 = t_stan_info1
        self.t_stan_info2 = t_stan_info2
        self.t_stan_info3 = t_stan_info3
        self.t_stan_mapping = t_stan_mapping
        self.t_stan_dic = t_stan_dic
        currt_time = round(time.time() * 1000)


    def write_to_csv(self, datas, file_name, date_time, num, total_num,control_file_time,delimiter=','):
        """

        :param datas: 写入数据
        :param file_name: 文件名
        :param date_time: 文件名日期
        :param num: 文件编号
        :param total_num: 控制文件内数据数量
        :return:
        """

        if delimiter == ',':
            file_path = os.path.join(self.file_path,'custom',date_time)
        elif delimiter == 'map':
            file_path = os.path.join(self.file_path,'mapping',date_time)
        elif delimiter == 'dic':
            file_path = os.path.join(self.file_path, 'dic', date_time)
        elif delimiter == '||':
            file_path = os.path.join(self.file_path,'txn',date_time)
        else:
            file_path = os.path.join(self.file_path,'stif',date_time)

        if num < 10:
            file_full = os.path.join(file_path, '{}-D{}-T{}-000{}.csv'.format(file_name.upper(), date_time, control_file_time, num))
        else:
            file_full = os.path.join(file_path, '{}-D{}-T{}-00{}.csv'.format(file_name.upper(), date_time, control_file_time, num))
        # ============交易单独写入==================
        # 交易数据分割符为||，需要无法直接写入CSV，改用TXT写入
        if delimiter == '||':
            if not os.path.exists(file_full):
                title = eval('self.' + 't_stan_' + file_name)

                with open(file_full, 'a', encoding="utf-8-sig") as f:
                    f.write("||".join(title)+'\n')

            with open(file_full, 'a', encoding="utf-8-sig") as f:
                for da in datas:
                    f.write("||".join([str(tt) for tt in da]) + '\n')
        # ============================================
        elif delimiter == 'map':
            if not os.path.exists(file_full):
                title = eval('self.' + 't_stan_' + file_name)

                with open(file_full, 'a', encoding="utf-8-sig") as f:
                    f.write("||".join(title)+'\n')

            with open(file_full, 'a', encoding="utf-8-sig") as f:
                for dat in datas:
                    f.write(dat + '\n')
        elif delimiter == 'dic':
            if not os.path.exists(file_full):
                title = eval('self.' + 't_stan_' + file_name)
                with open(file_full, 'a', encoding="utf-8-sig") as f:
                    f.write("||".join(title)+'\n')
            with open(file_full, 'a', encoding="utf-8-sig") as f:
                for da in datas:
                    f.write("||".join([str(tt) for tt in da]) + '\n')
        else:
            # if not os.path.exists(file_full):
            #     title = eval('self.' + 't_stan_' + file_name)
            #     csvfile = open(file_full, 'a', encoding="utf-8-sig", newline='')
            #     writer = csv.writer(csvfile,delimiter=delimiter)
            #     writer.writerow(title)
            #     csvfile.close()
            #
            # csvfile = open(file_full, 'a', encoding="utf-8-sig", newline='')
            # writer = csv.writer(csvfile,delimiter=delimiter)
            # writer.writerows(datas)
            # csvfile.close()

            if not os.path.exists(file_full):
                title = eval('self.' + 't_stan_' + file_name)

                with open(file_full, 'a', encoding="utf-8-sig") as f:
                    f.write(",".join(['"'+tit+'"' for tit in title])+'\n')

            with open(file_full, 'a', encoding="utf-8-sig") as f:
                for da in datas:
                    res = [str(tt)  for tt in da]
                    f.write(",".join(['"'+dd+'"' for dd in res]) + '\n')
