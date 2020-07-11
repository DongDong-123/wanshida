# -*- coding: utf-8 -*-
# @Time    : 2020-06-24 22:18
# @Author  : liudongyang
# @FileName: make_data.py
# @Software: PyCharm
# 生成数据
from faker import Faker
from Common import CommonFunction
comm = CommonFunction()
fake = Faker(locale='zh_CN')


class MakeData:
    def __init__(self):
        self.csnm = None
        self.ctnm = None
        self.unit_code = None

    def make_stan_org(self, num):
        """
        机构数据
        :return:
        """
        self.csnm = 'org_1_{}'.format(num)  # 客户号    必填
        custormer_name = comm.org_name()  # 客户名称    必填
        custormer_sname = ''.join([elem[0] for elem in custormer_name.split()])  # 客户简称
        custormer_ename = ''  # 拼音/英文名称
        custormer_sename = ''  # 英文缩写
        busi_name = ''  # doing business as name
        appli_country = comm.chiose_country()  # 申请许可国家    必填
        sub_company = ''  # 子公司
        former_name = ''  # 申请过的合法名称
        cert_tp = comm.cert_type()  # 证件类型    必填
        cert_tp_explain = '证件类型说明'  # 证件类型说明    必填
        cert_num = comm.org_cert_num()  # 证件号码    必填
        cert_validity = comm.make_date()  # 证件有效期    必填
        city = comm.random_city()  # 注册地址-市    应填
        state = comm.chiose_provance(city)  # 注册地址-省    应填
        address = comm.make_address(city)  # 注册地址    必填
        post_code = comm.random_num(6)  # 注册地址-邮编
        tel = comm.make_tel_num()  # 注册地址-联系电话    必填
        fax = ''  # 注册地址-传真
        # ----------无注册地址时，邮寄地址必填------------------
        m_city = comm.random_city()  # 邮寄地址-市
        m_state = comm.chiose_provance(m_city)  # 邮寄地址-省
        m_address = comm.make_address(m_city)  # 邮寄地址
        m_post_code = ''  # 邮寄地址-邮编
        m_tel = comm.make_tel_num()  # 邮寄地址-联系电话
        m_fax = ''  # 邮寄地址-传真
        # -------------------------------------------------------
        pr_mr_ms = comm.random_chenghu()  # 第一联系人称呼
        pr_name = comm.make_name_data()  # 第一联系人姓名    必填
        pr_title = ''  # 第一联系人职务
        pr_phone = comm.make_tel_num()  # 第一联系人电话    必填
        pr_fax = ''  # 第一联系人传真
        pr_email = comm.make_email_data()  # 第一联系人电子邮箱    应填
        pr_address = comm.make_address()  # 第一联系人使用地址
        sec_mr_ms = comm.random_chenghu()  # 第二联系人称呼
        sec_name = comm.make_name_data()  # 第二联系人姓名
        sec_title = ''  # 第二联系人职务
        sec_phone = comm.make_tel_num()  # 第二联系人电话
        sec_fax = ''  # 第二联系人传真
        sec_email = comm.make_email_data()  # 第二联系人电子邮箱
        sec_address = comm.random_code()  # 第二联系人使用地址；1:注册地址 2:邮寄地址
        aml_mr_ms = comm.random_chenghu()  # 反洗钱联系人称呼
        aml_name = comm.make_name_data()  # 反洗钱联系人姓名    必填
        aml_title = ''  # 反洗钱联系人职务    必填
        aml_phone = comm.make_tel_num()  # 反洗钱联系人电话    必填
        aml_fax = ''  # 反洗钱联系人传真
        aml_email = comm.make_email_data()  # 反洗钱联系人电子邮箱    应填
        aml_address = comm.make_address()  # 反洗钱联系人使用地址
        client_tp = comm.cust_tyep()  # 客户类别    必填
        lfa_type = comm.org_type()  # 组织机构类别    应填
        lfa_type_explain = ''  # 组织机构其他类别说明
        found_date = comm.make_date(-20, -1)  # 成立日期    必填
        assets_size = ''  # 资产规模(美元，当年）
        country = comm.chiose_country()  # 注册国家    必填
        other_oper_country = fake.country()  # 其他运营国家  随机国家
        desc_business = fake.paragraph()  # 经营说明  随机一句话
        tin = ''  # TIN
        busi_type = ''  # 业务类型    必填
        industry_type = ''  # 主体的行业类别    必填
        indu_code = ''  # 主体的行业代码原值    应填
        indu_code_nt = ''  # 主体的行业代码原值说明    应填
        legal_p_name = ''  # 主体的法定代表人姓名    必填
        legal_p_ename = ''  # 主体的法定代表人英文姓名
        legal_p_cert_tp = ''  # 主体的法定代表人身份证件类型    必填
        legal_p_cert_explain = ''  # 主体的法定代表人证件类型说明    必填
        legal_p_cert_num = ''  # 主体的法定代表人身份证件号码    必填
        legal_cert_validity = ''  # 主体的法定代表人证件有效期    必填
        crid_country = ''
        registered_capital = comm.random_num(8)  # 注册资本
        registered_capital_currency = ''  # 注册资本金币种
        business_scope = '组织文化艺术交流活动；文艺创作；体育运动项目经营（高危险性体育项目除外）；承办展览展示；婚庆服务；摄影服务；摄像服务；公共关系服务；礼仪服务；模特服务；会议服务；大型活动组织服务；经济信息咨询；婚纱礼服出租；花卉租摆；舞台策划；摄影器材租赁；舞台灯光音响设计；电脑图文设计；电脑动画设计；设计、制作、代理、发布广告。'  # 经营范围    必填
        enps_ecic_sectors = ''  # 企业经济成份
        scale = ''  # 企业人数规模
        establish_busi_date = comm.make_date(-10, -1)  # 建立业务日期    必填
        end_busi_date = ''  # 终止业务日期    （注销的情况下）应填
        self.unit_code = ''  # 成员机构代码
        remark = fake.paragraph()  # 备注  随机一段话
        stat_flag_ori = comm.cust_status()  # 客户状态原值    应填
        stat_flag = comm.cust_status()  # 客户状态    必填
        mer_unit = ''  # 管理机构    必填
        account_manager = ''  # 客户经理
        reals = comm.make_reals_data()  # 客户真实有效性
        complex = comm.make_complex_data()  # 非自然人结构复杂度
        clear = comm.make_clear_data()  # 非自然人股权可辨识度
        create_time = comm.data_time()  # 数据创建时间    必填
        update_time = comm.data_time()  # 数据更新时间    必填
        creator = comm.random_num(5)  # 数据创建人id    必填
        updator = comm.random_num(5)  # 数据更新人id    必填

        # all_col = [self.csnm, ctnm, ctsnm, cten, ctsen, busi_name, appli_country, sub_company, former_name, citp, citp_nt, ctid, ctid_edt, state, city, address, post_code, tel, fax, m_state, m_city, m_address, m_post_code, m_tel, m_fax, pr_mr_ms, pr_name, pr_title, pr_phone, pr_fax, pr_email, pr_address, sec_mr_ms, sec_name, sec_title, sec_phone, sec_fax, sec_email, sec_address, aml_mr_ms, aml_name, aml_title, aml_phone, aml_fax, aml_email, aml_address, client_tp, lfa_type, lfa_type_explain, fud_date, assets_size, country, other_oper_country, desc_business, tin, busi_type, ctvc, indu_code, indu_code_nt, crnm, crit, crit_nt, crid, crid_edt, reg_cptl, reg_cptl_code, remark_ctvc, eecp, scale, rgdt, cls_dt, unit_code, remark, stat_flag_ori, stat_flag, mer_unit, cmgr, act_cd, acc_type1, bank_acc_name, cabm, country_2, statement_type, reals, complex, clear, data_crdt, data_cruser, data_updt, data_upuser]
        all_col = [self.csnm, custormer_name, custormer_sname, custormer_ename, custormer_sename, busi_name, appli_country, sub_company, former_name, cert_tp, cert_tp_explain, cert_num, cert_validity, state, city, address, post_code, tel, fax, m_city, m_state, m_address, m_post_code, m_tel, m_fax, pr_mr_ms, pr_name, pr_title, pr_phone, pr_fax, pr_email, pr_address, sec_mr_ms, sec_name, sec_title, sec_phone, sec_fax, sec_email, sec_address, aml_mr_ms, aml_name, aml_title, aml_phone, aml_fax, aml_email, aml_address, client_tp, lfa_type, lfa_type_explain, found_date, assets_size, country, other_oper_country, desc_business, tin, busi_type, industry_type, indu_code, indu_code_nt, legal_p_name, legal_p_ename, legal_p_cert_tp, legal_p_cert_explain, legal_p_cert_num, legal_cert_validity, crid_country, registered_capital, registered_capital_currency, business_scope, enps_ecic_sectors, scale, establish_busi_date, end_busi_date, self.unit_code, remark, stat_flag_ori, stat_flag, mer_unit, account_manager, reals, complex, clear, create_time, update_time, creator, updator]
        return all_col


    def make_stan_relation(self):
        """
        关系人表
        :return:
        """
        csnm = self.csnm  # 客户号  必填
        custormer_name = self.ctnm  # 客户名称  必填
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
        cert_tp = comm.cert_type()  # 关系人证件类型
        cert_tp_explain = ''  # 关系人证件类型说明
        if rel_cstp == '1':
            cert_num = comm.random_num(18)  # 临时18位数字字符串
            # ctid = comm.person_cert_num()  # 关系人证件号码
        else:
            cert_num = comm.org_cert_num()
        cert_validity = comm.make_date()  # 关系人证件有效期
        rcnt = comm.chiose_country()  # 关系人国籍/国家
        dob = ''  # 关系人出生日期
        cob = fake.country()  # 关系人出生国家
        years_comp = comm.random_num(1)  # 关系人入职年限
        years_indu = comm.random_num(1)  # 关系人从业年限
        rel_prov = ''  # 关系人省
        rel_city = ''  # 关系人市
        rel_area = ''  # 关系人区县
        rear = fake.address()  # 关系人详细地址
        retl = fake.phone_number()  # 关系人联系电话
        rel_phone = fake.phone_number()  # 关系人手机
        rel_fax = comm.random_num(8)  # 关系人传真
        rel_email = fake.ascii_email()  # 关系人电子邮箱
        gov_owned = comm.make_yes_no()  # 关系人是否国有持股
        hold_per = ''  # 持股比例
        hold_amt = ""  # 持股金额
        remark = fake.paragraph()  # 备注
        create_time = comm.data_time()  # 数据创建时间  必填
        creator = comm.random_num(5)  # 数据创建人id  必填
        update_time = comm.data_time()  # 数据更新时间  必填
        updator = comm.random_num(5)  # 数据更新人id  必填

        all_col = [csnm, custormer_name, rel_tp, rel_layer, rel_cstp, fir_name, sec_name, last_name, cert_tp, cert_tp_explain, cert_num, cert_validity, rcnt, dob, cob, years_comp, years_indu, rel_prov, rel_city, rel_area, rear, retl, rel_phone, rel_fax, rel_email, gov_owned, hold_per, hold_amt, remark, create_time, update_time, creator, updator]
        return all_col


    def make_stan_survey_info1(self):
        """"
        """
        ctif_id = self.csnm  # 客户号
        ctnm = self.ctnm  # 客户名称
        info_a_bool = comm.make_yes_no()  # 是否遵守反洗钱或反恐融资法
        laws_name = ''  # 法律法规名称
        info_a_bool2 = comm.make_yes_no()  # 是否有反洗钱或反恐融资的程序或制度
        info_a_bool3 = comm.make_yes_no_unused()  # 监管机构认为该制度是否得当
        supervisor_name = comm.make_name_data(3)  # 监管人员姓名
        inspection_time = comm.make_time()  # 检查时间
        info_a_explain = "".join(fake.paragraphs())  # 制度不完善地方
        info_a_explain2 = ''  # 完善时间及方式
        info_b_bool = comm.make_yes_no()  # 是否有反洗钱专职人员
        info_b_bool2 = comm.make_yes_no()  # 是否有完整的风险评估程序
        info_b_bool3 = comm.make_yes_no()  # 是否有反洗钱相关的员工培训
        info_b_explain = "".join(fake.paragraphs())  # 员工培训制度
        info_c_bool = comm.make_yes_no()  # 是否被禁止在注册地进行银行业务
        info_c_explain = "".join(fake.paragraphs())  # 原因
        info_d_bool = comm.make_yes_no()  # 是否在任何国家、地区都没有实体存在的机构
        info_d_bool2 = comm.make_yes_no()  # 是否禁止与没有任何实体存在的机构建立关系
        info_d_explain = "".join(fake.paragraphs())  # 原因
        payment_card_org = ''  # 监管支付卡活动的机构
        compliance_org = ''  # 监管合规的机构
        chartered_institution = ''  # 特许机构
        info_e_bool = comm.make_yes_no()  # 是否将任何反洗钱或制裁责任外包
        info_e_bool2 = comm.make_yes_no()  # 是否对第三方进行监督
        info_e_bool3 = comm.make_yes_no()  # 监督是否在程序中留痕
        supervision_trace_doc = ''  # 相关监督留痕文件
        info_f_bool = comm.make_yes_no()  # 是否将万事达相关业务进行名单预警
        list_type = comm.make_mingdan_type()  # 名单种类
        other_list_type = ''  # 其他名单种类
        info_f_explain = "".join(fake.paragraphs())  # 原因
        info_g_bool = comm.make_yes_no()  # 是否存在影响申请的诉讼或其他
        info_g_explain = "".join(fake.paragraphs())  # 原因
        info_h_bool = comm.make_yes_no()  # 是否参与允许转账的业务
        info_h_explain = "".join(fake.paragraphs())  # 项目细节描述
        data_crdt = comm.make_trade_time19()  # 数据创建时间
        data_cruser = comm.random_num(8)  # 数据创建人id
        data_updt = comm.make_trade_time19()  # 数据更新时间
        data_upuser = comm.random_num(8)  # 数据更新人id

        all_col = [ctif_id, ctnm, info_a_bool, laws_name, info_a_bool2, info_a_bool3, supervisor_name, inspection_time, info_a_explain, info_a_explain2, info_b_bool, info_b_bool2, info_b_bool3, info_b_explain, info_c_bool, info_c_explain, info_d_bool, info_d_bool2, info_d_explain, payment_card_org , compliance_org, chartered_institution, info_e_bool, info_e_bool2, info_e_bool3, supervision_trace_doc, info_f_bool, list_type, other_list_type, info_f_explain, info_g_bool, info_g_explain, info_h_bool, info_h_explain, data_crdt, data_cruser, data_updt, data_upuser]
        return all_col


    def make_stan_survey_info2(self):
        """"
        """
        ctif_id = self.csnm  # 客户号
        ctnm = self.ctnm  # 客户名称
        info2_a_bool = comm.make_yes_no()  # 监管部门是否可以随时对申请人的档案进行审查或者深入调查
        info2_a_explain = ''  # 最近五项检查的日期及结果
        info2_b_bool = comm.make_yes_no()  # 政府当局能否对申请人处以罚款、暂停或接管其业务以确保其遵守适用的审慎标准
        info2_b_explain = ''  # 原因
        agents_num = ''  # 分销渠道及代理商的数目
        aml_role_explain = ''  # 反洗钱官或反洗钱合规部角色描述
        compliance_name = ''  # 合规部负责人姓名
        aml_workers = ''  # 反洗钱工作人数
        aml_position = ''  # 反洗钱员工职位
        info2_c_bool = comm.make_yes_no()  # 是否有内部审计职能或其他独立第三方定期评估其“反洗钱”政策和程序
        info2_c_bool2 = comm.make_yes_no()  # 是否发现任何与“反洗钱”相关的缺陷
        info2_c_explain = ''  # 请说明已发现的不足之处和解决问题的补救计划
        info2_d_bool = comm.make_yes_no()  # 是否对其客户进行监督
        info2_d_explain = ''  # 描述监督程序
        info2_e_bool = comm.make_yes_no()  # 是否有对其客户信息进行尽职调查的程序
        info2_f_bool = comm.make_yes_no()  # 申请人是否对政治敏感人士有足够的认识和了解
        info2_g_bool = comm.make_yes_no()  # 万事达相关业务是否存在客户信息不完全或受益人信息不完整
        info2_g_explain = ''  # 原因
        info2_h_bool = comm.make_yes_no()  # 是否进行交易监测
        info2_h_explain = ''  # 确认可疑后的行为
        info2_i_bool = comm.make_yes_no()  # 是否可以上报可疑
        info2_i_explain = comm.random_str(10)  # 上报机构
        data_crdt = comm.make_trade_time19()  # 数据创建时间
        data_cruser = comm.random_num(8)  # 数据创建人id
        data_updt = comm.make_trade_time19()  # 数据更新时间
        data_upuser = comm.random_num(8)  # 数据更新人id

        all_col = [ctif_id, ctnm, info2_a_bool, info2_a_explain, info2_b_bool, info2_b_explain, agents_num, aml_role_explain, compliance_name, aml_workers, aml_position, info2_c_bool, info2_c_bool2, info2_c_explain, info2_d_bool, info2_d_explain, info2_e_bool, info2_f_bool, info2_g_bool, info2_g_explain, info2_h_bool, info2_h_explain, info2_i_bool, info2_i_explain, data_crdt, data_cruser, data_updt, data_upuser]
        return all_col


    def make_stan_survey_info3(self):
        """"
        """
        ctif_id = self.csnm  # 客户号
        ctnm = self.ctnm  # 客户名称
        fi_mcard_principal = comm.make_yes_no()  # 万事达卡-总部合作
        fi_mcard_affillate = comm.make_yes_no()  # 万事达卡-分支机构合作
        fi_mcard_association = comm.make_yes_no()  # 万事达卡-代理机构合作
        fi_mcard_issuing = comm.make_yes_no()  # 万事达卡-发卡
        fi_mcard_acquiring_merchants = comm.make_yes_no()  # 万事达卡-商户收单
        fi_mcard_acquiring_atm = comm.make_yes_no()  # 万事达卡-ATM收单
        fi_mcard_acquiring_mcd = comm.make_yes_no()  # 万事达卡-手动付款收单
        fi_mcard_optrpt_msd = comm.make_yes_no()  # 万事达卡-国内发送
        fi_mcard_optrpt_ms = comm.make_yes_no()  # 万事达卡-moneysend
        fi_mcard_optrpt_mscb = comm.make_yes_no()  # 万事达卡-跨境发送
        fi_mcard_optrpt_mpqr = comm.make_yes_no()  # 万事达卡-masterpass QR
        fi_mstro_principal = comm.make_yes_no()  # 万事顺卡-总部合作
        fi_mstro_affillate = comm.make_yes_no()  # 万事顺卡-分支机构合作
        fi_mstro_issuing = comm.make_yes_no()  # 万事顺卡-发卡
        fi_mstro_acquiring_merchants = comm.make_yes_no()  # 万事顺卡-商户收单
        fi_mstro_acquiring_atm = comm.make_yes_no()  # 万事顺卡-ATM收单
        fi_mstro_optrpt_msd = comm.make_yes_no()  # 万事顺卡-国内发送
        fi_mstro_optrpt_ms = comm.make_yes_no()  # 万事顺卡-moneysend
        fi_mstro_optrpt_mscb = comm.make_yes_no()  # 万事顺卡-跨境发送
        fi_mstro_optrpt_mpqr = comm.make_yes_no()  # 万事顺卡-masterpass QR
        fi_cirrus_principal = comm.make_yes_no()  # 顺风卡-总部合作
        fi_cirrus_affillate = comm.make_yes_no()  # 顺风卡-分支机构合作
        fi_cirrus_issuing_atm = comm.make_yes_no()  # 顺风卡-发卡
        fi_cirrus_acquiring_atm = comm.make_yes_no()  # 顺风卡-ATM收单
        fi_cirrus_optp2p_ms = comm.make_yes_no()  # 顺风卡-moneysend
        fi_cirrus_optp2p_mscb = comm.make_yes_no()  # 顺风卡-跨境发送
        fi_cirrus_optp2p_mpqr = comm.make_yes_no()  # 顺风卡-masterpass QR
        cgi_mcard_principal = comm.make_yes_no()  # cgi-万事达卡-总部合作
        cgi_mcard_affillate = comm.make_yes_no()  # cgi-万事达卡-分支机构合作
        cgi_mcard_issuing_credit = comm.make_yes_no()  # cgi-万事达卡-发卡-信用卡
        cgi_mcard_issuing_debit = comm.make_yes_no()  # cgi-万事达卡-发卡-借记卡
        cgi_mcard_issuing_prepaid = comm.make_yes_no()  # cgi-万事达卡-发卡-预付卡
        cgi_mcard_acquiring_atm = comm.make_yes_no()  # cgi-万事达卡-ATM收单
        cgi_mcard_acquiring_mcd = comm.make_yes_no()  # cgi-万事达卡-手动付款收单
        cgi_mcard_acquiring_merchants = comm.make_yes_no()  # cgi-万事达卡-商户收单
        cgi_mcard_acquiring_poi = comm.make_yes_no()  # cgi-万事达卡-POI收单
        cgi_mcard_optrpt_msd = comm.make_yes_no()  # cgi-万事达卡-国内发送
        cgi_mcard_optrpt_ms = comm.make_yes_no()  # cgi-万事达卡-moneysend
        cgi_mcard_optrpt_mscb = comm.make_yes_no()  # cgi-万事达卡-跨境发送
        cgi_mcard_optrpt_mpqr = comm.make_yes_no()  # cgi-万事达卡-masterpass QR
        cgi_mstro_principal = comm.make_yes_no()  # cgi-万事顺卡-总部合作
        cgi_mstro_affillate = comm.make_yes_no()  # cgi-万事顺卡-分支机构合作
        cgi_mstro_issuing_debit = comm.make_yes_no()  # cgi-万事顺卡-发卡-借记卡
        cgi_mstro_issuing_prepaid = comm.make_yes_no()  # cgi-万事顺卡-发卡-预付卡
        cgi_mstro_acquiring_atm = comm.make_yes_no()  # cgi-万事顺卡-ATM收单
        cgi_mstro_acquiring_merchants = comm.make_yes_no()  # cgi-万事顺卡-商户收单
        cgi_mstro_acquiring_poi = comm.make_yes_no()  # cgi-万事顺卡-POI收单
        cgi_mstro_optrpt_msd = comm.make_yes_no()  # cgi-万事顺卡-国内发送
        cgi_mstro_optrpt_ms = comm.make_yes_no()  # cgi-万事顺卡-moneysend
        cgi_mstro_optrpt_mscb = comm.make_yes_no()  # cgi-万事顺卡-跨境发送
        cgi_mstro_optrpt_mpqr = comm.make_yes_no()  # cgi-万事顺卡-masterpass QR
        cgi_cirrus_principal = comm.make_yes_no()  # cgi-顺风卡-总部合作
        cgi_cirrus_affillate = comm.make_yes_no()  # cgi-顺风卡-分支机构合作
        cgi_cirrus__issuing = comm.make_yes_no()  # cgi-顺风卡-发卡
        cgi_cirrus_acquiring_atm = comm.make_yes_no()  # cgi-顺风卡-ATM收单
        cgi_cirrus_optp2p_ms = comm.make_yes_no()  # cgi-顺风卡-moneysend
        cgi_cirrus_optp2p_mscb = comm.make_yes_no()  # cgi-顺风卡-跨境发送
        cgi_cirrus_optp2p_mpqr = comm.make_yes_no()  # cgi-万顺风卡-masterpass QR
        info_a_bool = comm.make_yes_no_unused()  # 如果发行借记卡、信用卡、预付卡，将是否对现金交易进行控制
        info_a_explain = fake.paragraph()  # 原因
        additional_services_transfer = comm.make_yes_no()  # 是否提供额外服务
        acquiring_rePower = comm.make_yes_no()  #
        data_crdt = comm.make_trade_time19()  # 数据创建时间
        data_cruser = comm.random_num(8)  # 数据创建人id
        data_updt = comm.make_trade_time19()  # 数据更新时间
        data_upuser = comm.random_num(8)  # 数据更新人id

        all_col = [ctif_id, ctnm, fi_mcard_principal, fi_mcard_affillate, fi_mcard_association, fi_mcard_issuing, fi_mcard_acquiring_merchants, fi_mcard_acquiring_atm, fi_mcard_acquiring_mcd, fi_mcard_optrpt_msd, fi_mcard_optrpt_ms, fi_mcard_optrpt_mscb, fi_mcard_optrpt_mpqr, fi_mstro_principal, fi_mstro_affillate, fi_mstro_issuing, fi_mstro_acquiring_merchants, fi_mstro_acquiring_atm, fi_mstro_optrpt_msd, fi_mstro_optrpt_ms, fi_mstro_optrpt_mscb, fi_mstro_optrpt_mpqr, fi_cirrus_principal, fi_cirrus_affillate, fi_cirrus_issuing_atm, fi_cirrus_acquiring_atm, fi_cirrus_optp2p_ms, fi_cirrus_optp2p_mscb, fi_cirrus_optp2p_mpqr, cgi_mcard_principal, cgi_mcard_affillate, cgi_mcard_issuing_credit, cgi_mcard_issuing_debit, cgi_mcard_issuing_prepaid, cgi_mcard_acquiring_atm, cgi_mcard_acquiring_mcd, cgi_mcard_acquiring_merchants, cgi_mcard_acquiring_poi, cgi_mcard_optrpt_msd, cgi_mcard_optrpt_ms, cgi_mcard_optrpt_mscb, cgi_mcard_optrpt_mpqr, cgi_mstro_principal, cgi_mstro_affillate, cgi_mstro_issuing_debit, cgi_mstro_issuing_prepaid, cgi_mstro_acquiring_atm, cgi_mstro_acquiring_merchants, cgi_mstro_acquiring_poi, cgi_mstro_optrpt_msd, cgi_mstro_optrpt_ms, cgi_mstro_optrpt_mscb, cgi_mstro_optrpt_mpqr, cgi_cirrus_principal, cgi_cirrus_affillate, cgi_cirrus__issuing, cgi_cirrus_acquiring_atm, cgi_cirrus_optp2p_ms, cgi_cirrus_optp2p_mscb, cgi_cirrus_optp2p_mpqr, info_a_bool, info_a_explain, additional_services_transfer, acquiring_rePower, data_crdt, data_cruser, data_updt, data_upuser]
        return all_col


    def make_stan_ptxn(self, stiftime):
        """"原始业务交易信息表
        """
        msg_id = comm.random_num(20)  # 消息id    必填
        msg_type = ''  # MTI消息类型标识    必填
        inter_tran_type = comm.make_inter_tran_type()  # 联机系统内部交易类型
        uuid = comm.make_ticd_data()  # 交易唯一标识    必填
        trace_id = comm.random_num(12)  # 联机流水号    必填
        tran_group_id = ''  # 交易分组号
        tran_init = comm.make_tran_init()  # 交易发起方：0-联机平台,1-成员行发起,2-手工平台发起    必填
        tran_res = comm.make_tran_res()  # 收单应答标识：0-联机应答,1-成员行应答    必填
        card_bin = comm.random_num(9)  # 卡bin
        card_type = comm.make_card_type()  # 卡类型：借贷记    必填  缺码表
        card_product = '卡产品'  # 卡产品    必填  缺码表
        card_brand = '卡品牌'  # 卡品牌  缺码表
        card_media = comm.make_tsdr_data()  # 01磁条卡,02芯片卡
        token_pan = comm.random_num(32)  # token卡号
        encrypt_pan = fake.sha1()  # 加密后的卡号    必填
        hash_pan = fake.sha256()  # 卡号hash
        digsit = comm.make_digsit(token_pan)  # 前六后四卡号
        crdhldr_tran_type = comm.random_num(2)  # 持卡人交易类型（内部使用）    必填
        crdhldr_acc_tp_from = comm.make_crdhldr_acc_tp_from()  # 持卡人出方账户类型    必填
        crdhldr_acc_tp_to = comm.make_crdhldr_acc_tp_to()  # 持卡人入方账户类型    必填
        tran_amount = comm.make_tcat_data()  # 交易金额    必填
        sett_amount = tran_amount  # 结算金额    必填
        bill_amount = sett_amount  # 账单金额
        tran_datetime = comm.make_trade_time19(stiftime) # 交易时间(YYYY-MM-DD HH:mm:ss)    必填
        crdhldr_bill_fee = ''  # 持卡人账单费用（目前没有用上）
        sett_conv_rate = '7'  # 结算汇率    必填
        bill_conv_rate = sett_conv_rate  # 账单汇率
        sys_trace_audit_nbr = comm.random_num(6)  # 系统跟踪号  8位随机数   必填
        local_tran_datetime = comm.make_trade_time19(stiftime)  # 本地交易时间    必填
        exp_date = comm.make_enable_date()  # 卡有效期
        sett_date = stiftime  # 结算日期    必填
        conv_date = stiftime  # 汇率转换日期
        mcc = comm.random_num_head_0(8)  # 商户类型    必填
        pos_entry_cd = comm.random_num_head_0(8)  # POS机输入方式码    必填
        card_seq_num = comm.random_num_head_0(3)  # 卡序列号
        pos_pin_cptr_cd = comm.random_num_head_0(2)  # PIN获取码
        tran_fee_indi = 'C'  # 交易费借贷标识（暂不收费）
        acq_srchg_amount = '0'  # 交易费金额
        acq_ins_id_cd = comm.random_num(10)  # 收单机构号    必填
        fwd_ins_id_cd = comm.random_num(10)  # 收单代理机构号    必填
        trk2_prsnt_sw = comm.random_code()  # 2磁是否出现  随机选1,2
        retriv_ref_num = comm.random_num(12)  # 检索参考号
        auth_cd = comm.random_word_num_or_str(6)  # 授权码
        resp_cd = comm.random_word_num_or_str(6)  # 应答码    必填
        pos_term_id = comm.random_num(5)  # POS机终端id    必填
        acq_merch_id = comm.random_word_num_or_str(10)  # 收单商户id    必填
        acq_merch_name = '1'  # 收单商户名称    必填
        acq_merch_city = comm.random_city()  # 收单商户城市    必填
        acq_merch_state = '1'  # 收单商户状态    必填
        frmt_resp_data = '1'  # 应答附加信息    必填
        additional_data = '1'  # 附加信息    必填
        funding_payment_tti = ''  # 转出或者转入的交易类型标识    必填
        tran_curr_cd = 'CNY'  # 交易币种    必填
        sett_curr_cd = 'CNY'  # 结算币种    必填
        bill_curr_cd = 'CNY'  # 账单币种    必填
        data_integrated = comm.random_word_num_or_str(214)  # 芯片卡信息    必填
        paym_account = comm.random_num(6)  # 支付账户    必填
        advice_reason_cd = comm.random_num(6)  # 通知码    必填
        advice_reason_dt_cd = comm.random_num(6)  # 通知详细码    必填
        advice_reason_dt_txt = fake.paragraph()  # 通知详细描述    必填
        advice_reason_add_txt = fake.paragraph()  # 通知附加描述    必填
        pos_data = comm.random_num(18)  # POS数据    必填
        pos_crdhldr_present = comm.make_yes_no_unused()  # POS持卡人是否出现    必填
        pos_tran_status = comm.make_yes_no_unused()  # POS交易状态    必填
        inf_data = comm.random_num(10)  # 网络设施数据    必填
        ntw_mng_inf_cd = comm.random_num(6)  # 网络管理信息码    必填
        org_mti = comm.random_num(6)  # 原交易的消息类型标识    必填
        org_stan = comm.random_num(6)  # 原交易的系统跟踪号    必填
        org_tran_datetime = comm.make_trade_time19(stiftime)  # 原交易的交易时间    必填
        org_acq_ins_id_cd = comm.random_num(6)  # 原交易的收单机构号    必填
        org_fwd_ins_id_cd = comm.random_num(6)  # 原交易的收单代理机构号    必填
        org_trace_id = comm.make_ticd_data()  # 原交易的联机流水号    必填
        rcv_ins_id_cd = comm.random_num(10)   # 发卡代理机构号    必填
        iss_mti_cd = comm.make_iss_mti_cd()  # 发给发卡方的交易类型标识    必填
        iss_pcode = comm.make_iss_pcode()  # 发给发卡方的持卡人交易类型    必填
        iss_ins_id_cd = comm.random_str(12)  # 发卡行机构代码    必填
        acq_msg_flag = ''  # 收单行单双标识    必填
        iss_msg_flag = ''  # 发卡行收单标识    必填
        single_dual_flag = ''  # 单双转换信息标识    必填
        tran_buss_st = comm.make_trans_type()  # 交易业务状态    必填
        tran_advice_st = comm.make_tran_advice_st()  # 交易通知状态    必填
        inter_resp_cd = ''  # 内部应答码    必填
        dc_id = ''  # 中心标识    必填
        insert_timestamp = comm.data_time()  # 记录创建时间    必填
        insert_by = ''  # 记录创建人    必填
        last_update_timestamp = comm.data_time()  # 记录最后更新时间    必填
        last_update_by = ''  # 记录最后更新人    必填
        channel_type = comm.make_channel_type()  # 渠道类型    必填
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
        data_transfer_dt = comm.process_time(int(stiftime.replace("-",""))+1)  # 数据传输日期

        all_col = [msg_id, msg_type, inter_tran_type, uuid, trace_id, tran_group_id, tran_init, tran_res, card_bin, card_type, card_product, card_brand, card_media, token_pan, encrypt_pan, hash_pan, digsit, crdhldr_tran_type, crdhldr_acc_tp_from, crdhldr_acc_tp_to, tran_amount, sett_amount, bill_amount, tran_datetime, crdhldr_bill_fee, sett_conv_rate, bill_conv_rate, sys_trace_audit_nbr, local_tran_datetime, exp_date, sett_date, conv_date, mcc, pos_entry_cd, card_seq_num, pos_pin_cptr_cd, tran_fee_indi, acq_srchg_amount, acq_ins_id_cd, fwd_ins_id_cd, trk2_prsnt_sw, retriv_ref_num, auth_cd, resp_cd, pos_term_id, acq_merch_id, acq_merch_name, acq_merch_city, acq_merch_state, frmt_resp_data, additional_data, funding_payment_tti, tran_curr_cd, sett_curr_cd, bill_curr_cd, data_integrated, paym_account, advice_reason_cd, advice_reason_dt_cd, advice_reason_dt_txt, advice_reason_add_txt, pos_data, pos_crdhldr_present, pos_tran_status, inf_data, ntw_mng_inf_cd, org_mti, org_stan, org_tran_datetime, org_acq_ins_id_cd, org_fwd_ins_id_cd, org_trace_id, rcv_ins_id_cd, iss_mti_cd, iss_pcode, iss_ins_id_cd, acq_msg_flag, iss_msg_flag, single_dual_flag, tran_buss_st, tran_advice_st, inter_resp_cd, dc_id, insert_timestamp, insert_by, last_update_timestamp, last_update_by, channel_type, cash_back_amount, cash_back_indicator, mcht_data_srv, tcc, cvv2, pos_cat_level, merch_advic_cd, src_member_id, dest_member_id, group_tran_type, fee_category, fan_ntw_cd, int_rate_id, net_ref_num, bnk_ref_num, acq_ref_num, gcms_prc_num, act_tran_amount, act_sett_amount, act_bill_amount, zero_fill_amount, reserve1, reserve2, reserve3, data_transfer_dt]
        all_data = {
            "msg_id": msg_id,
            "msg_type": msg_type,
            "inter_tran_type": inter_tran_type,
            "uuid": uuid,
            "trace_id": trace_id,
            "tran_group_id": tran_group_id,
            "tran_init": tran_init,
            "tran_res": tran_res,
            "card_bin": card_bin,
            "card_type": card_type,
            "card_product": card_product,
            "card_brand": card_brand,
            "card_media": card_media,
            "token_pan": token_pan,
            "encrypt_pan": encrypt_pan,
            "hash_pan": hash_pan,
            "digsit": digsit,
            "crdhldr_tran_type": crdhldr_tran_type,
            "crdhldr_acc_tp_from": crdhldr_acc_tp_from,
            "crdhldr_acc_tp_to": crdhldr_acc_tp_to,
            "tran_amount": tran_amount,
            "sett_amount": sett_amount,
            "bill_amount": bill_amount,
            "tran_datetime": tran_datetime,
            "crdhldr_bill_fee": crdhldr_bill_fee,
            "sett_conv_rate": sett_conv_rate,
            "bill_conv_rate": bill_conv_rate,
            "sys_trace_audit_nbr": sys_trace_audit_nbr,
            "local_tran_datetime": local_tran_datetime,
            "exp_date": exp_date,
            "sett_date": sett_date,
            "conv_date": conv_date,
            "mcc": mcc,
            "pos_entry_cd": pos_entry_cd,
            "card_seq_num": card_seq_num,
            "pos_pin_cptr_cd": pos_pin_cptr_cd,
            "tran_fee_indi": tran_fee_indi,
            "acq_srchg_amount": acq_srchg_amount,
            "acq_ins_id_cd": acq_ins_id_cd,
            "fwd_ins_id_cd": fwd_ins_id_cd,
            "trk2_prsnt_sw": trk2_prsnt_sw,
            "retriv_ref_num": retriv_ref_num,
            "auth_cd": auth_cd,
            "resp_cd": resp_cd,
            "pos_term_id": pos_term_id,
            "acq_merch_id": acq_merch_id,
            "acq_merch_name": acq_merch_name,
            "acq_merch_city": acq_merch_city,
            "acq_merch_state": acq_merch_state,
            "frmt_resp_data": frmt_resp_data,
            "additional_data": additional_data,
            "funding_payment_tti": funding_payment_tti,
            "tran_curr_cd": tran_curr_cd,
            "sett_curr_cd": sett_curr_cd,
            "bill_curr_cd": bill_curr_cd,
            "data_integrated": data_integrated,
            "paym_account": paym_account,
            "advice_reason_cd": advice_reason_cd,
            "advice_reason_dt_cd": advice_reason_dt_cd,
            "advice_reason_dt_txt": advice_reason_dt_txt,
            "advice_reason_add_txt": advice_reason_add_txt,
            "pos_data": pos_data,
            "pos_crdhldr_present": pos_crdhldr_present,
            "pos_tran_status": pos_tran_status,
            "inf_data": inf_data,
            "ntw_mng_inf_cd": ntw_mng_inf_cd,
            "org_mti": org_mti,
            "org_stan": org_stan,
            "org_tran_datetime": org_tran_datetime,
            "org_acq_ins_id_cd": org_acq_ins_id_cd,
            "org_fwd_ins_id_cd": org_fwd_ins_id_cd,
            "org_trace_id": org_trace_id,
            "rcv_ins_id_cd": rcv_ins_id_cd,
            "iss_mti_cd": iss_mti_cd,
            "iss_pcode": iss_pcode,
            "iss_ins_id_cd": iss_ins_id_cd,
            "acq_msg_flag": acq_msg_flag,
            "iss_msg_flag": iss_msg_flag,
            "single_dual_flag": single_dual_flag,
            "tran_buss_st": tran_buss_st,
            "tran_advice_st": tran_advice_st,
            "inter_resp_cd": inter_resp_cd,
            "dc_id": dc_id,
            "insert_timestamp": insert_timestamp,
            "insert_by": insert_by,
            "last_update_timestamp": last_update_timestamp,
            "last_update_by": last_update_by,
            "channel_type": channel_type,
            "cash_back_amount": cash_back_amount,
            "cash_back_indicator": cash_back_indicator,
            "mcht_data_srv": mcht_data_srv,
            "tcc": tcc,
            "cvv2": cvv2,
            "pos_cat_level": pos_cat_level,
            "merch_advic_cd": merch_advic_cd,
            "src_member_id": src_member_id,
            "dest_member_id": dest_member_id,
            "group_tran_type": group_tran_type,
            "fee_category": fee_category,
            "fan_ntw_cd": fan_ntw_cd,
            "int_rate_id": int_rate_id,
            "net_ref_num": net_ref_num,
            "bnk_ref_num": bnk_ref_num,
            "acq_ref_num": acq_ref_num,
            "gcms_prc_num": gcms_prc_num,
            "act_tran_amount": act_tran_amount,
            "act_sett_amount": act_sett_amount,
            "act_bill_amount": act_bill_amount,
            "zero_fill_amount": zero_fill_amount,
            "reserve1": reserve1,
            "reserve2": reserve2,
            "reserve3": reserve3,
            "data_transfer_dt": data_transfer_dt
        }
        return all_col, all_data


    def make_stan_dtxn(self, ori_ptxn):
        """
        原始差错交易信息表
        :return:
        """
        batclr_sngl_dspt_msg_id = ori_ptxn.get("msg_id")  # 表主键    必填
        dspt_sys_id = ori_ptxn.get("uuid")  # 差错系统唯一标识    必填
        orig_trace_id = ori_ptxn.get("trace_id")  # 原始交易跟踪号    必填
        card_type = ori_ptxn.get("card_type")  # 卡类型    必填
        card_product = ori_ptxn.get("card_product")  # 卡产品    必填
        card_brand = ori_ptxn.get("card_brand")  # 卡品牌    必填
        token_pan = ori_ptxn.get("token_pan")  # token卡号    N
        encrypt_pan = ori_ptxn.get("encrypt_pan")  # 加密卡号    必填
        crdhldr_tran_type = ori_ptxn.get("crdhldr_tran_type")  # 持卡人交易类型    必填
        crdhldr_acc_tp_from = ori_ptxn.get("crdhldr_acc_tp_from")  # 持卡人出方账户类型    N
        crdhldr_acc_tp_to = ori_ptxn.get("crdhldr_acc_tp_to")  # 持卡人入方账户类型    N
        sett_conv_rate = ori_ptxn.get("sett_conv_rate")  # 结算汇率    必填
        dspt_trace_aud_num = ''  # 全零    N
        orig_local_tran_datetime = ori_ptxn.get("local_tran_datetime")  # 原交易本地交易时间    必填
        sett_date = ori_ptxn.get("sett_date")  # 差错交易结算日期    必填
        mcc = ori_ptxn.get("mcc")  # 商户类型    必填
        pos_entry_cd = ori_ptxn.get("pos_entry_cd")  # POS机输入方式码    必填
        retriv_ref_num = ori_ptxn.get("retriv_ref_num")  # 检索参考号    必填
        auth_cd = ori_ptxn.get("auth_cd")  # 授权码    必填
        resp_cd = ori_ptxn.get("resp_cd")  # 应答码    必填
        pos_term_id = ori_ptxn.get("pos_term_id")  # POS机终端id    必填
        tran_curr_cd = ori_ptxn.get("tran_curr_cd")  # 交易币种    必填
        sett_curr_cd = ori_ptxn.get("sett_curr_cd")  # 结算币种    必填
        dspt_advic_rsn_cd = ''  # 差错原因码    必填
        dspt_advic_rsn_dtl_cd = ''  # 差错原因详细码    必填
        org_stan = ori_ptxn.get("org_stan")  # 原交易的系统跟踪号    必填
        channel_type = ''  # 交易渠道    必填
        cash_back_amount = ori_ptxn.get("cash_back_amount")  # 余额信息    N
        orig_tran_type = ori_ptxn.get("inter_tran_type")  # 原交易交易类型    必填
        dspt_tran_type = ori_ptxn.get("inter_tran_type")  # 差错交易交易类型    必填
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
        cash_back_indicator = ori_ptxn.get("cash_back_indicator")  # 余额借贷记标识    N
        mcht_data_srv = ori_ptxn.get("mcht_data_srv")  # 商户数据服务    N
        dspt_ref_num = ''  # 差错系统编号    必填
        insert_timestamp = ori_ptxn.get("insert_timestamp")  # 记录创建时间
        last_update_timestamp = ori_ptxn.get("last_update_timestamp")  # 记录最后更新时间
        reserve1 = ''  # 保留域1
        reserve2 = ''  # 保留域2
        reserve3 = ''  # 保留域3
        version = ''  # 版本号
        case_id = ''  # 立案案件号    C
        msg_rev_ind = ''  # 消息冲正标识    必填
        dspt_tran_dttm = ori_ptxn.get("tran_datetime")  # 差错交易发起时间    必填
        data_transfer_dt = ''  # 数据传输日期

        all_col = [batclr_sngl_dspt_msg_id, dspt_sys_id, orig_trace_id, card_type, card_product, card_brand, token_pan, encrypt_pan, crdhldr_tran_type, crdhldr_acc_tp_from, crdhldr_acc_tp_to, sett_conv_rate, dspt_trace_aud_num, orig_local_tran_datetime, sett_date, mcc, pos_entry_cd, retriv_ref_num, auth_cd, resp_cd, pos_term_id, tran_curr_cd, sett_curr_cd, dspt_advic_rsn_cd, dspt_advic_rsn_dtl_cd, org_stan, channel_type, cash_back_amount, orig_tran_type, dspt_tran_type, send_ica, rcvr_ica, send_rl, rcvr_rl, dspt_tran_amt, dspt_setl_amt, orig_sett_date, db_cr_flag, tran_amt, setl_amt, actl_tran_amt, setl_tran_amt, cash_back_indicator, mcht_data_srv, dspt_ref_num, insert_timestamp, last_update_timestamp, reserve1, reserve2, reserve3, version, case_id, msg_rev_ind, dspt_tran_dttm, data_transfer_dt]
        return all_col


    def make_stan_txn(self, stiftime, ori_ptxn):
        """
        :return:
        """
        id = ori_ptxn.get("msg_id")  # 交易表主键    必填
        tran_kd = comm.make_tran_kd()  # 交易种类    必填
        uuid = ori_ptxn.get("uuid")  # 交易唯一标识    必填
        trace_id = ori_ptxn.get("trace_id")  # 联机流水号    必填
        card_bin = ori_ptxn.get("card_bin")  # 卡bin
        card_type = ori_ptxn.get("card_type")  # 卡类型：借贷记    必填  缺码表
        card_type_pboc = comm.make_STCT_data()  # 报送卡类型
        card_product = ori_ptxn.get("card_product")  # 卡产品    必填  缺码表
        card_brand = ori_ptxn.get("card_brand")  # 卡品牌  缺码表
        token_pan = ori_ptxn.get("token_pan")  # token卡号
        encrypt_pan = ori_ptxn.get("encrypt_pan")  # 加密卡号    必填
        crdhldr_tran_type = ori_ptxn.get("crdhldr_tran_type")  # 持卡人交易类型（内部使用）  缺码表    必填
        crdhldr_acc_tp_from = ori_ptxn.get("crdhldr_acc_tp_from")  # 持卡人出方账户类型  缺码表
        crdhldr_acc_tp_to = ori_ptxn.get("crdhldr_acc_tp_to")  # 持卡人入方账户类型  缺码表
        tran_datetime = ori_ptxn.get("tran_datetime")  # 交易时间    必填
        orig_local_tran_datetime = ori_ptxn.get("local_tran_datetime")  # 原交易本地交易时间    必填
        tsdr = comm.make_tsdr_data()  # 持卡人资金收付标志    必填
        tran_amount = ori_ptxn.get("tran_amount")   # 交易金额    必填
        sett_amount = ori_ptxn.get("sett_amount")  # 结算金额    必填
        tran_curr_cd = ori_ptxn.get("tran_curr_cd")  # 交易币种    必填
        sett_curr_cd = ori_ptxn.get("sett_curr_cd")  # 结算币种    必填
        sett_conv_rate = ori_ptxn.get("sett_conv_rate")  # 结算汇率    必填
        sett_date = ori_ptxn.get("sett_date")  # 结算日期    必填
        crat_u = int(tran_amount) / int(sett_conv_rate)  # 交易金额折合美元    必填
        crat_c = tran_amount  # 交易金额折合人民币    必填
        mcc = ori_ptxn.get("mcc")  # 商户类型    必填  缺码表
        pos_entry_cd = ori_ptxn.get("pos_entry_cd")  # POS机输入方式码    必填  缺码表
        retriv_ref_num = ori_ptxn.get("retriv_ref_num")  # 检索参考号
        auth_cd = ori_ptxn.get("auth_cd")  # 授权码  缺码表
        resp_cd = ori_ptxn.get("resp_cd")  # 应答码    必填   缺码表
        pos_term_id = ori_ptxn.get("pos_term_id")  # POS机终端id    必填
        rcv_ins_id_cd = ori_ptxn.get("rcv_ins_id_cd")  # 发卡代理机构号
        iss_mti_cd = ori_ptxn.get("iss_mti_cd")  # 发给发卡方的交易类型标识  缺码表
        iss_pcode = ori_ptxn.get("iss_pcode")  # 发给发卡方的持卡人交易类型  缺码表
        iss_ins_id_cd = ori_ptxn.get("iss_ins_id_cd")  # 发卡行机构代码    必填
        acq_merch_id = ori_ptxn.get("acq_merch_id")  # 收单商户id    必填
        acq_merch_name = ori_ptxn.get("acq_merch_name")  # 收单商户名称    必填
        acq_merch_city = ori_ptxn.get("acq_merch_city")  # 收单商户城市    必填  缺码表
        acq_merch_state = ori_ptxn.get("acq_merch_state")  # 收单商户状态  缺码表
        acq_ins_id_cd = ori_ptxn.get("acq_ins_id_cd")  # 收单机构号    必填
        fwd_ins_id_cd = ori_ptxn.get("fwd_ins_id_cd")  # 收单代理机构号    必填
        TRCD = comm.make_TRCD_data()  # 交易发生地    必填
        CBIF = comm.make_tsdr_data()  # 境内外标识    必填
        channel_type = ori_ptxn.get("channel_type")  # 交易渠道    必填
        TSTP = comm.make_tstp_data()  # 交易方式    必填
        cash_back_amount = ori_ptxn.get("cash_back_amount")  # 余额信息
        cash_back_indicator = ori_ptxn.get("cash_back_indicator")  # 余额借贷标识  缺码表
        tran_type = ori_ptxn.get("inter_tran_type")  # 交易类型    必填
        if tran_kd == '00':
            dspt_tran_type = ori_ptxn.get("inter_tran_type")  # 差错交易类型
        else:
            dspt_tran_type = ''
        org_stan = ori_ptxn.get("org_stan")  # 原交易的系统跟踪号
        tran_buss_st = ori_ptxn.get("tran_buss_st")  # 交易业务状态    必填
        tran_advice_st = ori_ptxn.get("tran_advice_st")  # 交易通知状态
        mcht_data_srv = ori_ptxn.get("mcht_data_srv")  # 商户数据服务
        additional_data = ori_ptxn.get("additional_data")  # 附加信息
        insert_timestamp = ori_ptxn.get("insert_timestamp")  # 记录创建时间    必填
        insert_by = ori_ptxn.get("insert_by")  # 记录创建人
        last_update_timestamp = ori_ptxn.get("last_update_timestamp")  # 记录最后更新时间
        last_update_by = ori_ptxn.get("last_update_by")  # 记录最后更新人
        mer_unit = '管理机构'  # 管理机构    必填  缺码表
        data_transfer_dt = comm.process_time(int(stiftime.replace("-",""))+1)  # 数据传输日期    必填

        all_col = [id, tran_kd, uuid, trace_id, card_bin, card_type, card_type_pboc, card_product, card_brand, token_pan, encrypt_pan, crdhldr_tran_type, crdhldr_acc_tp_from, crdhldr_acc_tp_to, tran_datetime, orig_local_tran_datetime, tsdr, tran_amount, sett_amount, tran_curr_cd, sett_curr_cd, sett_conv_rate, sett_date, crat_u, crat_c, mcc, pos_entry_cd, retriv_ref_num, auth_cd, resp_cd, pos_term_id, rcv_ins_id_cd, iss_mti_cd, iss_pcode, iss_ins_id_cd, acq_merch_id, acq_merch_name, acq_merch_city, acq_merch_state, acq_ins_id_cd, fwd_ins_id_cd, TRCD, CBIF, channel_type, TSTP, cash_back_amount, cash_back_indicator, tran_type, dspt_tran_type, org_stan, tran_buss_st, tran_advice_st, mcht_data_srv, additional_data, insert_timestamp, insert_by, last_update_timestamp, last_update_by, mer_unit, data_transfer_dt]

        return all_col


    def make_stan_stif(self, stiftime, ori_ptxn):
        """

        :return:
        """
        unit_code = self.unit_code  # 成员机构代码  必填
        warn_dt = ''  # 预警日期  必填
        rule_id = ''  # 预警规则  必填
        rule_type = comm.make_rule_type()  # 预警类型  必填
        warn_kd = comm.make_warn_kd()  # 预警方式
        susp_value = ''  # 可疑分数
        ctif_tp = comm.make_ctif_tp()  # 可疑主体类别  必填
        tran_kd = comm.make_tran_kd()  # 交易种类  必填
        card_type = ''  # 卡类型：借贷记  必填
        mcc = ''  # 商户类型  必填   缺码表

        if ctif_tp == '2':  # 商户
            MCNO = ''  # 主体的商户代码  （商户）必填  缺码表
            MCNM = ''  # 主体的商户名称  （商户）必填
            ACCD = ''  # 收单机构代码  （商户）必填
            STCT = ''  # 主体使用的银行卡类型  （持卡人）必填
            STCI = ''  # 主体使用的银行卡号码  （持卡人）必填
            IUCD = ''  # 主体开卡机构代码  （持卡人）必填
        else:  # 持卡人
            MCNO = ''  # 主体的商户代码  （商户）必填
            MCNM = ''  # 主体的商户名称  （商户）必填
            ACCD = ''  # 收单机构代码  （商户）必填
            STCT = comm.make_STCT_data()  # 主体使用的银行卡类型  （持卡人）必填
            STCI = comm.random_num(18)  # 主体使用的银行卡号码  （持卡人）必填
            IUCD = ''  # 主体开卡机构代码  （持卡人）必填
        fwd_ins_id_cd = ''  # 收单代理机构号
        card_product = ''  # 卡产品  必填  缺码表
        card_brand = ''  # 卡品牌  缺码表
        rcv_ins_id_cd = ''  # 发卡代理机构号
        tstm = '{} {}'.format(stiftime, comm.make_time())  # 交易时间  必填
        tsdr = comm.make_tsdr_data()  # 资金收付标志  必填
        TCPP = ''  # 资金用途
        TCTP = comm.make_tctp_data()  # 交易币种  必填
        TCAT = comm.make_tcat_data()  # 交易金额  必填
        TCMN = ''  # 交易对手的商户代码
        TCNM = ''  # 交易对手的商户名称
        CACD = ''  # 交易对方收单机构代码
        c_fwd_ins_id_cd = ''  # 收单代理机构号
        TCCT = ''  # 交易对手使用的银行卡类型
        T_card_product = ''  # 交易对手卡产品  必填   缺码表
        T_card_brand = ''  # 交易对手卡品牌
        TCCI = ''  # 交易对手使用的银行卡号码
        TCIC = ''  # 交易对手开卡机构代码
        c_rcv_ins_id_cd = ''  # 交易对手发卡代理机构号
        bptc = ''  # 清算组织与成员机构之间的业务交易编码  有就填
        ticd = comm.make_ticd_data()  # 业务标识号  必填
        busi_type = comm.make_busi_type()  # 业务类型  必填
        trans_type = ''  # 交易类型   必填
        trans_stat = comm.make_trans_type()  # 交易状态  应填
        tran_advice_st = comm.make_tran_advice_st()  # 交易通知状态

        acq_merch_city = ''  # 收单商户城市  必填
        acq_merch_state = ''  # 收单商户状态
        TRCD = ''  # 交易发生地  应填
        CBIF = comm.make_tsdr_data()  # 境内外标识  应填  01：境内交易；02：跨境交易
        trans_channel = ''  # 交易渠道  应填
        PCTP = ''  # 清算币种  应填
        PCAT = ''  # 清算金额  应填
        crat_u = ''  # 交易金额折合美元  应填
        crat_c = ''  # 交易金额折合人民币  应填
        TSTP = comm.make_tstp_data()  # 交易方式  应填
        pos_entry_cd = ''  # POS机输入方式码  必填  # 缺码表
        retriv_ref_num = ''  # 检索参考号
        auth_cd = ''  # 授权码
        resp_cd = ''  # 应答码  必填
        pos_term_id = comm.random_num(12)  # POS机终端id  必填
        mer_unit = '管理机构'  # 管理机构  必填   缺码表
        run_dt = stiftime  # 可疑交易数据生成日期  必填
        data_transfer_dt = comm.process_time(int(stiftime.replace("-",""))+1)  # 可疑交易数据传输日期  必填

        all_col = [unit_code, warn_dt, rule_id, rule_type, warn_kd, susp_value, ctif_tp, tran_kd, card_type, MCNO, MCNM, ACCD, fwd_ins_id_cd, STCT, card_product, card_brand, STCI, IUCD, rcv_ins_id_cd, tstm, tsdr, TCPP, TCTP, TCAT, TCMN, TCNM, CACD, c_fwd_ins_id_cd, TCCT, T_card_product, T_card_brand, TCCI, TCIC, c_rcv_ins_id_cd, bptc, ticd, busi_type, trans_type, trans_stat, tran_advice_st, acq_merch_city, acq_merch_state, TRCD, CBIF, trans_channel, PCTP, PCAT, crat_u, crat_c, TSTP, mcc, pos_entry_cd, retriv_ref_num, auth_cd, resp_cd, pos_term_id, mer_unit, run_dt, data_transfer_dt]

        return all_col