# -*- coding: utf-8 -*-
# @Time    : 2020-06-24 22:18
# @Author  : liudongyang
# @FileName: make_data.py
# @Software: PyCharm
# 生成数据
from Common import CommonFunction
comm = CommonFunction()



class MakeData:
    def __init__(self):
        self.busi_reg_no = None
        self.ctnm = 'zhangsansan'

    def make_stan_org(self, num):
        """
        机构数据
        :return:
        """
        self.busi_reg_no = 'org_1_{}'.format(num)  # 客户号    必填
        ctnm = comm.org_name()  # 客户名称    必填
        ctsnm = ''.join([elem[0] for elem in ctnm.split()])  # 客户简称
        cten = ''  # 拼音/英文名称
        ctsen = ''  # 英文缩写
        busi_name = ''  # doing business as name
        appli_country = comm.chiose_country()  # 申请许可国家    必填
        sub_company = ''  # 子公司
        former_name = ''  # 申请过的合法名称
        citp = comm.cert_type()  # 证件类型    必填
        citp_nt = ''  # 证件类型说明    必填
        ctid = comm.org_cert_num()  # 证件号码    必填
        ctid_edt = comm.cert_dateline()  # 证件有效期    必填
        state = ''  # 注册地址-省    应填
        city = ''  # 注册地址-市    应填
        address = ''  # 注册地址    必填
        post_code = ''  # 注册地址-邮编
        tel = ''  # 注册地址-联系电话    必填
        fax = ''  # 注册地址-传真
        m_state = ''  # 邮寄地址-省
        m_city = ''  # 邮寄地址-市
        m_address = ''  # 邮寄地址
        m_post_code = ''  # 邮寄地址-邮编
        m_tel = ''  # 邮寄地址-联系电话
        m_fax = ''  # 邮寄地址-传真
        pr_mr_ms = ''  # 第一联系人称呼
        pr_name = ''  # 第一联系人姓名    必填
        pr_title = ''  # 第一联系人职务
        pr_phone = ''  # 第一联系人电话    必填
        pr_fax = ''  # 第一联系人传真
        pr_email = ''  # 第一联系人电子邮箱    应填
        pr_address = ''  # 第一联系人使用地址
        sec_mr_ms = ''  # 第二联系人称呼
        sec_name = ''  # 第二联系人姓名
        sec_title = ''  # 第二联系人职务
        sec_phone = ''  # 第二联系人电话
        sec_fax = ''  # 第二联系人传真
        sec_email = ''  # 第二联系人电子邮箱
        sec_address = ''  # 第二联系人使用地址
        aml_mr_ms = ''  # 反洗钱联系人称呼
        aml_name = ''  # 反洗钱联系人姓名    必填
        aml_title = ''  # 反洗钱联系人职务    必填
        aml_phone = ''  # 反洗钱联系人电话    必填
        aml_fax = ''  # 反洗钱联系人传真
        aml_email = ''  # 反洗钱联系人电子邮箱    应填
        aml_address = ''  # 反洗钱联系人使用地址
        client_tp = ''  # 客户类别    必填
        lfa_type = ''  # 组织机构类别    应填
        lfa_type_explain = ''  # 组织机构其他类别说明
        fud_date = ''  # 成立日期    必填
        assets_size = ''  # 资产规模(美元，当年）
        country = ''  # 注册国家    必填
        other_oper_country = ''  # 其他运营国家
        desc_business = ''  # 经营说明
        tin = ''  # TIN
        busi_type = ''  # 业务类型    必填
        ctvc = ''  # 主体的行业类别    必填
        indu_code = ''  # 主体的行业代码原值    应填
        indu_code_nt = ''  # 主体的行业代码原值说明    应填
        crnm = ''  # 主体的法定代表人姓名    必填
        crit = ''  # 主体的法定代表人身份证件类型    必填
        crit_nt = ''  # 主体的法定代表人证件类型说明    必填
        crid = ''  # 主体的法定代表人身份证件号码    必填
        crid_edt = ''  # 主体的法定代表人证件有效期    必填
        reg_cptl = ''  # 注册资本
        reg_cptl_code = ''  # 注册资本金币种
        remark_ctvc = ''  # 经营范围    必填
        eecp = ''  # 企业经济成份
        scale = ''  # 企业人数规模
        rgdt = ''  # 建立业务日期    必填
        cls_dt = ''  # 终止业务日期    （注销的情况下）应填
        unit_code = ''  # 成员机构代码
        remark = ''  # 备注
        stat_flag_ori = ''  # 客户状态原值    应填
        stat_flag = ''  # 客户状态    必填
        mer_unit = ''  # 管理机构    必填
        cmgr = ''  # 客户经理
        act_cd = ''  # 账户号    四元组
        acc_type1 = ''  # 账户类型    四元组
        bank_acc_name = ''  # 账户名称    四元组
        cabm = ''  # 账号开户机构名称    四元组
        country = ''  # 开户机构所在地
        statement_type = ''  # 结算类型
        reals = ''  # 客户真实有效性
        complex = ''  # 非自然人结构复杂度
        clear = ''  # 非自然人股权可辨识度
        data_crdt = ''  # 数据创建时间    必填
        data_cruser = ''  # 数据创建人id    必填
        data_updt = ''  # 数据更新时间    必填
        data_upuser = ''  # 数据更新人id    必填

        all_col = [self.busi_reg_no, ctnm, ctsnm, cten, ctsen, busi_name, appli_country, sub_company, former_name, citp, citp_nt, ctid, ctid_edt, state, city, address, post_code, tel, fax, m_state, m_city, m_address, m_post_code, m_tel, m_fax, pr_mr_ms, pr_name, pr_title, pr_phone, pr_fax, pr_email, pr_address, sec_mr_ms, sec_name, sec_title, sec_phone, sec_fax, sec_email, sec_address, aml_mr_ms, aml_name, aml_title, aml_phone, aml_fax, aml_email, aml_address, client_tp, lfa_type, lfa_type_explain, fud_date, assets_size, country, other_oper_country, desc_business, tin, busi_type, ctvc, indu_code, indu_code_nt, crnm, crit, crit_nt, crid, crid_edt, reg_cptl, reg_cptl_code, remark_ctvc, eecp, scale, rgdt, cls_dt, unit_code, remark, stat_flag_ori, stat_flag, mer_unit, cmgr, act_cd, acc_type1, bank_acc_name, cabm, country, statement_type, reals, complex, clear, data_crdt, data_cruser, data_updt, data_upuser]
        return all_col


    def make_stan_relation(self):
        """
        关系人表
        :return:
        """
        ctif_id = self.busi_reg_no  # 客户号  必填
        ctnm = self.ctnm  # 客户名称  必填
        rel_tp = comm.relation_type()  # 关系类型  必填
        rel_layer = comm.rel_layer()  # 关系人层级
        rel_cstp = '2'  # 关系人类别  必填
        if rel_cstp == '2':
            fir_name = comm.org_name()  # 关系人first name  必填
            sec_name = ''.join([elem[0] for elem in fir_name.split()])  # 关系人second name
            last_name = ''  # 关系人last name
        else:
            fir_name = comm.person_fir_name()
            sec_name = comm.person_fir_name()  # 关系人second name
            last_name = comm.person_fir_name()  # 关系人last name
        citp = comm.cert_type()  # 关系人证件类型
        citp_nt = ''  # 关系人证件类型说明
        if rel_cstp == '1':
            ctid = comm.random_num(18)  # 临时18位数字字符串
            # ctid = comm.person_cert_num()  # 关系人证件号码
        else:
            ctid = comm.org_cert_num()
        ctid_edt = comm.cert_dateline()  # 关系人证件有效期
        rcnt = comm.chiose_country()  # 关系人国籍/国家
        dob = ''  # 关系人出生日期
        cob = ''  # 关系人出生国家
        years_comp = ''  # 关系人入职年限
        years_indu = ''  # 关系人从业年限
        rel_prov = ''  # 关系人省
        rel_city = ''  # 关系人市
        rel_area = ''  # 关系人区县
        rear = ''  # 关系人详细地址
        retl = ''  # 关系人联系电话
        ret_mphone = ''  # 关系人手机
        rel_fax = ''  # 关系人传真
        rel_email = ''  # 关系人电子邮箱
        gov_owned = ''  # 关系人是否国有持股
        hold_per = ''  # 持股比例
        hold_amt = ''  # 持股金额
        remark = ''  # 备注
        data_crdt = comm.data_time()  # 数据创建时间  必填
        data_cruser = comm.random_num(5)  # 数据创建人id  必填
        data_updt = comm.data_time()  # 数据更新时间  必填
        data_upuser = comm.random_num(5)  # 数据更新人id  必填

        all_col = [ctif_id, ctnm, rel_tp, rel_layer, rel_cstp, fir_name, sec_name, last_name, citp, citp_nt, ctid, ctid_edt, rcnt, dob, cob, years_comp, years_indu, rel_prov, rel_city, rel_area, rear, retl, ret_mphone, rel_fax, rel_email, gov_owned, hold_per, hold_amt, remark, data_crdt, data_cruser, data_updt, data_upuser]

        return all_col


    def make_stan_ptxn(self):
        """"
        """
        msg_id = ''  # 消息id    必填
        msg_type = ''  # MTI消息类型标识    必填
        inter_tran_type = ''  # 联机系统内部交易类型
        uuid = ''  # 交易唯一标识    必填
        trace_id = ''  # 联机流水号    必填
        tran_group_id = ''  # 交易分组号
        tran_init = ''  # 交易发起方：0-联机平台,1-成员行发起,2-手工平台发起    必填
        tran_res = ''  # 收单应答标识：0-联机应答,1-成员行应答    必填
        card_bin = ''  # 卡bin
        card_type = ''  # 卡类型：借贷记    必填
        card_product = ''  # 卡产品    必填
        card_brand = ''  # 卡品牌
        card_media = ''  # 01磁条卡,02芯片卡
        token_pan = ''  # token卡号
        encrypt_pan = ''  # 加密后的卡号    必填
        hash_pan = ''  # 卡号hash
        digsit = ''  # 前六后四卡号
        crdhldr_tran_type = ''  # 持卡人交易类型（内部使用）    必填
        crdhldr_acc_tp_from = ''  # 持卡人出方账户类型    必填
        crdhldr_acc_tp_to = ''  # 持卡人入方账户类型    必填
        tran_amount = ''  # 交易金额    必填
        sett_amount = ''  # 结算金额    必填
        bill_amount = ''  # 账单金额
        tran_datetime = ''  # 交易时间(YYYY-MM-DDHH:mm:ss)    必填
        crdhldr_bill_fee = ''  # 持卡人账单费用（目前没有用上）
        sett_conv_rate = ''  # 结算汇率    必填
        bill_conv_rate = ''  # 账单汇率
        sys_trace_audit_nbr = ''  # 系统跟踪号    必填
        local_tran_datetime = ''  # 本地交易时间    必填
        exp_date = ''  # 卡有效期
        sett_date = ''  # 结算日期    必填
        conv_date = ''  # 汇率转换日期
        mcc = ''  # 商户类型    必填
        pos_entry_cd = ''  # POS机输入方式码    必填
        card_seq_num = ''  # 卡序列号
        pos_pin_cptr_cd = ''  # PIN获取码
        tran_fee_indi = ''  # 交易费借贷标识（暂不收费）
        acq_srchg_amount = ''  # 交易费金额
        acq_ins_id_cd = ''  # 收单机构号    必填
        fwd_ins_id_cd = ''  # 收单代理机构号    必填
        trk2_prsnt_sw = ''  # 2磁是否出现
        retriv_ref_num = ''  # 检索参考号
        auth_cd = ''  # 授权码
        resp_cd = ''  # 应答码    必填
        pos_term_id = ''  # POS机终端id    必填
        acq_merch_id = ''  # 收单商户id    必填
        acq_merch_name = ''  # 收单商户名称    必填
        acq_merch_city = ''  # 收单商户城市    必填
        acq_merch_state = ''  # 收单商户状态    必填
        frmt_resp_data = ''  # 应答附加信息    必填
        additional_data = ''  # 附加信息    必填
        funding_payment_tti = ''  # 转出或者转入的交易类型标识    必填
        tran_curr_cd = ''  # 交易币种    必填
        sett_curr_cd = ''  # 结算币种    必填
        bill_curr_cd = ''  # 账单币种    必填
        data_integrated = ''  # 芯片卡信息    必填
        paym_account = ''  # 支付账户    必填
        advice_reason_cd = ''  # 通知码    必填
        advice_reason_dt_cd = ''  # 通知详细码    必填
        advice_reason_dt_txt = ''  # 通知详细描述    必填
        advice_reason_add_txt = ''  # 通知附加描述    必填
        pos_data = ''  # POS数据    必填
        pos_crdhldr_present = ''  # POS持卡人是否出现    必填
        pos_tran_status = ''  # POS交易状态    必填
        inf_data = ''  # 网络设施数据    必填
        ntw_mng_inf_cd = ''  # 网络管理信息码    必填
        org_mti = ''  # 原交易的消息类型标识    必填
        org_stan = ''  # 原交易的系统跟踪号    必填
        org_tran_datetime = ''  # 原交易的交易时间    必填
        org_acq_ins_id_cd = ''  # 原交易的收单机构号    必填
        org_fwd_ins_id_cd = ''  # 原交易的收单代理机构号    必填
        org_trace_id = ''  # 原交易的联机流水号    必填
        rcv_ins_id_cd = ''  # 发卡代理机构号    必填
        iss_mti_cd = ''  # 发给发卡方的交易类型标识    必填
        iss_pcode = ''  # 发给发卡方的持卡人交易类型    必填
        iss_ins_id_cd = ''  # 发卡行机构代码    必填
        acq_msg_flag = ''  # 收单行单双标识    必填
        iss_msg_flag = ''  # 发卡行收单标识    必填
        single_dual_flag = ''  # 单双转换信息标识    必填
        tran_buss_st = ''  # 交易业务状态    必填
        tran_advice_st = ''  # 交易通知状态    必填
        inter_resp_cd = ''  # 内部应答码    必填
        dc_id = ''  # 中心标识    必填
        insert_timestamp = ''  # 记录创建时间    必填
        insert_by = ''  # 记录创建人    必填
        last_update_timestamp = ''  # 记录最后更新时间    必填
        last_update_by = ''  # 记录最后更新人    必填
        channel_type = ''  # 渠道类型    必填
        cash_back_amount = ''  # 余额信息
        cash_back_indicator = ''  # 余额借贷标识
        mcht_data_srv = ''  # 商户数据服务
        tcc = ''  # 交易类型码
        cvv2 = ''  # 卡片cvv2
        pos_cat_level = ''  # POS持卡人激活终端级别
        merch_advic_cd = ''  # 商户通知类型
        src_member_id = ''  # 源成员行标识
        dest_member_id = ''  # 目的成员行标识
        group_tran_type = ''  # 交易类型分组    必填
        fee_category = ''  # 收费类型    必填
        fan_ntw_cd = ''  # 金融网络编码
        int_rate_id = ''  # 转换率标识
        net_ref_num = ''  # 网络参考号
        bnk_ref_num = ''  # 银行网络参考号
        acq_ref_num = ''  # 收单参考号
        gcms_prc_num = ''  # GCMS处理数据和场次号
        act_tran_amount = ''  # 实际交易金额
        act_sett_amount = ''  # 实际结算金额
        act_bill_amount = ''  # 实际账单金额
        zero_fill_amount = ''  # 全零填充金额
        reserve1 = ''  # 保留域1
        reserve2 = ''  # 保留域2
        reserve3 = ''  # 保留域3
        data_transfer_dt = ''  # 数据传输日期

        all_col = [msg_id, msg_type, inter_tran_type, uuid, trace_id, tran_group_id, tran_init, tran_res, card_bin, card_type, card_product, card_brand, card_media, token_pan, encrypt_pan, hash_pan, digsit, crdhldr_tran_type, crdhldr_acc_tp_from, crdhldr_acc_tp_to, tran_amount, sett_amount, bill_amount, tran_datetime, crdhldr_bill_fee, sett_conv_rate, bill_conv_rate, sys_trace_audit_nbr, local_tran_datetime, exp_date, sett_date, conv_date, mcc, pos_entry_cd, card_seq_num, pos_pin_cptr_cd, tran_fee_indi, acq_srchg_amount, acq_ins_id_cd, fwd_ins_id_cd, trk2_prsnt_sw, retriv_ref_num, auth_cd, resp_cd, pos_term_id, acq_merch_id, acq_merch_name, acq_merch_city, acq_merch_state, frmt_resp_data, additional_data, funding_payment_tti, tran_curr_cd, sett_curr_cd, bill_curr_cd, data_integrated, paym_account, advice_reason_cd, advice_reason_dt_cd, advice_reason_dt_txt, advice_reason_add_txt, pos_data, pos_crdhldr_present, pos_tran_status, inf_data, ntw_mng_inf_cd, org_mti, org_stan, org_tran_datetime, org_acq_ins_id_cd, org_fwd_ins_id_cd, org_trace_id, rcv_ins_id_cd, iss_mti_cd, iss_pcode, iss_ins_id_cd, acq_msg_flag, iss_msg_flag, single_dual_flag, tran_buss_st, tran_advice_st, inter_resp_cd, dc_id, insert_timestamp, insert_by, last_update_timestamp, last_update_by, channel_type, cash_back_amount, cash_back_indicator, mcht_data_srv, tcc, cvv2, pos_cat_level, merch_advic_cd, src_member_id, dest_member_id, group_tran_type, fee_category, fan_ntw_cd, int_rate_id, net_ref_num, bnk_ref_num, acq_ref_num, gcms_prc_num, act_tran_amount, act_sett_amount, act_bill_amount, zero_fill_amount, reserve1, reserve2, reserve3, data_transfer_dt]

        return all_col


    def make_stan_dtxn(self):
        """

        :return:
        """
        batclr_sngl_dspt_msg_id = ''  # 表主键    必填
        dspt_sys_id = ''  # 差错系统唯一标识    必填
        orig_trace_id = ''  # 原始交易跟踪号    必填
        card_type = ''  # 卡类型    必填
        card_product = ''  # 卡产品    必填
        card_brand = ''  # 卡品牌    必填
        token_pan = ''  # token卡号    N
        encrypt_pan = ''  # 加密卡号    必填
        crdhldr_tran_type = ''  # 持卡人交易类型    必填
        crdhldr_acc_tp_from = ''  # 持卡人出方账户类型    N
        crdhldr_acc_tp_to = ''  # 持卡人入方账户类型    N
        sett_conv_rate = ''  # 结算汇率    必填
        dspt_trace_aud_num = ''  # 全零    N
        orig_local_tran_datetime = ''  # 原交易本地交易时间    必填
        sett_date = ''  # 差错交易结算日期    必填
        mcc = ''  # 商户类型    必填
        pos_entry_cd = ''  # POS机输入方式码    必填
        retriv_ref_num = ''  # 检索参考号    必填
        auth_cd = ''  # 授权码    必填
        resp_cd = ''  # 应答码    必填
        pos_term_id = ''  # POS机终端id    必填
        tran_curr_cd = ''  # 交易币种    必填
        sett_curr_cd = ''  # 结算币种    必填
        dspt_advic_rsn_cd = ''  # 差错原因码    必填
        dspt_advic_rsn_dtl_cd = ''  # 差错原因详细码    必填
        org_stan = ''  # 原交易的系统跟踪号    必填
        channel_type = ''  # 交易渠道    必填
        cash_back_amount = ''  # 余额信息    N
        orig_tran_type = ''  # 原交易交易类型    必填
        dspt_tran_type = ''  # 差错交易交易类型    必填
        send_ica = ''  # 交易发起方机构号    必填
        rcvr_ica = ''  # 交易接收方机构号    必填
        send_rl = ''  # 交易发起方角色    必填
        rcvr_rl = ''  # 交易接收方角色    必填
        dspt_tran_amt = ''  # 差错交易金额    必填
        dspt_setl_amt = ''  # 差错结算金额    必填
        orig_sett_date = ''  # 原始交易结算日期    必填
        db_cr_flag = ''  # 本金借贷标识    必填
        tran_amt = ''  # 交易金额    必填
        setl_amt = ''  # 结算金额    必填
        actl_tran_amt = ''  # 差错交易实际金额    必填
        setl_tran_amt = ''  # 差错交易结算金额    必填
        cash_back_indicator = ''  # 余额借贷记标识    N
        mcht_data_srv = ''  # 商户数据服务    N
        dspt_ref_num = ''  # 差错系统编号    必填
        insert_timestamp = ''  # 记录创建时间
        last_update_timestamp = ''  # 记录最后更新时间
        reserve1 = ''  # 保留域1
        reserve2 = ''  # 保留域2
        reserve3 = ''  # 保留域3
        version = ''  # 版本号
        case_id = ''  # 立案案件号    C
        msg_rev_ind = ''  # 消息冲正标识    必填
        dspt_tran_dttm = ''  # 差错交易发起时间    必填
        data_transfer_dt = ''  # 数据传输日期

        all_col = [batclr_sngl_dspt_msg_id, dspt_sys_id, orig_trace_id, card_type, card_product, card_brand, token_pan, encrypt_pan, crdhldr_tran_type, crdhldr_acc_tp_from, crdhldr_acc_tp_to, sett_conv_rate, dspt_trace_aud_num, orig_local_tran_datetime, sett_date, mcc, pos_entry_cd, retriv_ref_num, auth_cd, resp_cd, pos_term_id, tran_curr_cd, sett_curr_cd, dspt_advic_rsn_cd, dspt_advic_rsn_dtl_cd, org_stan, channel_type, cash_back_amount, orig_tran_type, dspt_tran_type, send_ica, rcvr_ica, send_rl, rcvr_rl, dspt_tran_amt, dspt_setl_amt, orig_sett_date, db_cr_flag, tran_amt, setl_amt, actl_tran_amt, setl_tran_amt, cash_back_indicator, mcht_data_srv, dspt_ref_num, insert_timestamp, last_update_timestamp, reserve1, reserve2, reserve3, version, case_id, msg_rev_ind, dspt_tran_dttm, data_transfer_dt]
        return all_col


    def make_stan_txn(self):
        """

        :return:
        """
        id = ''  # 交易表主键    必填
        tran_kd = ''  # 交易种类    必填
        uuid = ''  # 交易唯一标识    必填
        trace_id = ''  # 联机流水号    必填
        card_bin = ''  # 卡bin
        card_type = ''  # 卡类型：借贷记    必填
        card_type_pboc = ''  # 报送卡类型
        card_product = ''  # 卡产品    必填
        card_brand = ''  # 卡品牌
        token_pan = ''  # token卡号
        encrypt_pan = ''  # 加密卡号    必填
        crdhldr_tran_type = ''  # 持卡人交易类型（内部使用）    必填
        crdhldr_acc_tp_from = ''  # 持卡人出方账户类型
        crdhldr_acc_tp_to = ''  # 持卡人入方账户类型
        tran_datetime = ''  # 交易时间    必填
        orig_local_tran_datetime = ''  # 原交易本地交易时间    必填
        tsdr = ''  # 持卡人资金收付标志    必填
        tran_amount = ''  # 交易金额    必填
        sett_amount = ''  # 结算金额    必填
        tran_curr_cd = ''  # 交易币种    必填
        sett_curr_cd = ''  # 结算币种    必填
        sett_conv_rate = ''  # 结算汇率    必填
        sett_date = ''  # 结算日期    必填
        crat_u = ''  # 交易金额折合美元    必填
        crat_c = ''  # 交易金额折合人民币    必填
        mcc = ''  # 商户类型    必填
        pos_entry_cd = ''  # POS机输入方式码    必填
        retriv_ref_num = ''  # 检索参考号
        auth_cd = ''  # 授权码
        resp_cd = ''  # 应答码    必填
        pos_term_id = ''  # POS机终端id    必填
        rcv_ins_id_cd = ''  # 发卡代理机构号
        iss_mti_cd = ''  # 发给发卡方的交易类型标识
        iss_pcode = ''  # 发给发卡方的持卡人交易类型
        iss_ins_id_cd = ''  # 发卡行机构代码    必填
        acq_merch_id = ''  # 收单商户id    必填
        acq_merch_name = ''  # 收单商户名称    必填
        acq_merch_city = ''  # 收单商户城市    必填
        acq_merch_state = ''  # 收单商户状态
        acq_ins_id_cd = ''  # 收单机构号    必填
        fwd_ins_id_cd = ''  # 收单代理机构号    必填
        TRCD = ''  # 交易发生地    必填
        CBIF = ''  # 境内外标识    必填
        channel_type = ''  # 交易渠道    必填
        TSTP = ''  # 交易方式    必填
        cash_back_amount = ''  # 余额信息
        cash_back_indicator = ''  # 余额借贷标识
        tran_type = ''  # 交易类型    必填
        dspt_tran_type = ''  # 差错交易类型
        org_stan = ''  # 原交易的系统跟踪号
        tran_buss_st = ''  # 交易业务状态    必填
        tran_advice_st = ''  # 交易通知状态
        mcht_data_srv = ''  # 商户数据服务
        additional_data = ''  # 附加信息
        insert_timestamp = ''  # 记录创建时间    必填
        insert_by = ''  # 记录创建人
        last_update_timestamp = ''  # 记录最后更新时间
        last_update_by = ''  # 记录最后更新人
        mer_unit = ''  # 管理机构    必填
        data_transfer_dt = ''  # 数据传输日期    必填

        all_col = [id, tran_kd, uuid, trace_id, card_bin, card_type, card_type_pboc, card_product, card_brand, token_pan, encrypt_pan, crdhldr_tran_type, crdhldr_acc_tp_from, crdhldr_acc_tp_to, tran_datetime, orig_local_tran_datetime, tsdr, tran_amount, sett_amount, tran_curr_cd, sett_curr_cd, sett_conv_rate, sett_date, crat_u, crat_c, mcc, pos_entry_cd, retriv_ref_num, auth_cd, resp_cd, pos_term_id, rcv_ins_id_cd, iss_mti_cd, iss_pcode, iss_ins_id_cd, acq_merch_id, acq_merch_name, acq_merch_city, acq_merch_state, acq_ins_id_cd, fwd_ins_id_cd, TRCD, CBIF, channel_type, TSTP, cash_back_amount, cash_back_indicator, tran_type, dspt_tran_type, org_stan, tran_buss_st, tran_advice_st, mcht_data_srv, additional_data, insert_timestamp, insert_by, last_update_timestamp, last_update_by, mer_unit, data_transfer_dt]

        return all_col


    def make_stan_stif(self):
        """

        :return:
        """
        unit_code = ''  # 成员机构代码  必填
        warn_dt = ''  # 预警日期  必填
        rule_id = ''  # 预警规则  必填
        rule_type = ''  # 预警类型  必填
        warn_kd = ''  # 预警方式
        susp_value = ''  # 可疑分数
        ctif_tp = ''  # 可疑主体类别  必填
        tran_kd = ''  # 交易种类  必填
        card_type = ''  # 卡类型：借贷记  必填
        MCNO = ''  # 主体的商户代码  （商户）必填
        MCNM = ''  # 主体的商户名称  （商户）必填
        ACCD = ''  # 收单机构代码  （商户）必填
        fwd_ins_id_cd = ''  # 收单代理机构号
        STCT = ''  # 主体使用的银行卡类型  （持卡人）必填
        card_product = ''  # 卡产品  必填
        card_brand = ''  # 卡品牌
        STCI = ''  # 主体使用的银行卡号码  （持卡人）必填
        IUCD = ''  # 主体开卡机构代码  （持卡人）必填
        rcv_ins_id_cd = ''  # 发卡代理机构号
        tstm = ''  # 交易时间  必填
        tsdr = ''  # 资金收付标志  必填
        TCPP = ''  # 资金用途
        TCTP = ''  # 交易币种  必填
        TCAT = ''  # 交易金额  必填
        TCMN = ''  # 交易对手的商户代码
        TCNM = ''  # 交易对手的商户名称
        CACD = ''  # 交易对方收单机构代码
        c_fwd_ins_id_cd = ''  # 收单代理机构号
        TCCT = ''  # 交易对手使用的银行卡类型
        T_card_product = ''  # 交易对手卡产品  必填
        T_card_brand = ''  # 交易对手卡品牌
        TCCI = ''  # 交易对手使用的银行卡号码
        TCIC = ''  # 交易对手开卡机构代码
        c_rcv_ins_id_cd = ''  # 交易对手发卡代理机构号
        bptc = ''  # 清算组织与成员机构之间的业务交易编码  有就填
        ticd = ''  # 业务标识号  必填
        busi_type = ''  # 业务类型  必填
        trans_type = ''  # 交易类型   必填
        trans_stat = ''  # 交易状态  应填
        tran_advice_st = ''  # 交易通知状态
        acq_merch_city = ''  # 收单商户城市  必填
        acq_merch_state = ''  # 收单商户状态
        TRCD = ''  # 交易发生地  应填
        CBIF = ''  # 境内外标识  应填
        trans_channel = ''  # 交易渠道  应填
        PCTP = ''  # 清算币种  应填
        PCAT = ''  # 清算金额  应填
        crat_u = ''  # 交易金额折合美元  应填
        crat_c = ''  # 交易金额折合人民币  应填
        TSTP = ''  # 交易方式  应填
        mcc = ''  # 商户类型  必填
        pos_entry_cd = ''  # POS机输入方式码  必填
        retriv_ref_num = ''  # 检索参考号
        auth_cd = ''  # 授权码
        resp_cd = ''  # 应答码  必填
        pos_term_id = ''  # POS机终端id  必填
        mer_unit = ''  # 管理机构  必填
        run_dt = ''  # 可疑交易数据生成日期  必填
        data_transfer_dt = ''  # 可疑交易数据传输日期  必填

        all_col = [unit_code, warn_dt, rule_id, rule_type, warn_kd, susp_value, ctif_tp, tran_kd, card_type, MCNO, MCNM, ACCD, fwd_ins_id_cd, STCT, card_product, card_brand, STCI, IUCD, rcv_ins_id_cd, tstm, tsdr, TCPP, TCTP, TCAT, TCMN, TCNM, CACD, c_fwd_ins_id_cd, TCCT, T_card_product, T_card_brand, TCCI, TCIC, c_rcv_ins_id_cd, bptc, ticd, busi_type, trans_type, trans_stat, tran_advice_st, acq_merch_city, acq_merch_state, TRCD, CBIF, trans_channel, PCTP, PCAT, crat_u, crat_c, TSTP, mcc, pos_entry_cd, retriv_ref_num, auth_cd, resp_cd, pos_term_id, mer_unit, run_dt, data_transfer_dt]

        return all_col