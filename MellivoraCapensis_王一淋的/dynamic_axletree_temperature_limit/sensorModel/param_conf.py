#!/usr/bin/env python
# vim: set fileencoding=utf-8
# 功能：参数定义
# author: 20161010 by zgh

# 定义SDR参数
p_sdr = {'A02':'trainsite_valid',    # 车次有效
    'A03':'speed_valid',             # 速度有效
    'A06':'trainsite_id',            # 车次
    'A07':'union_status',            # 联挂状态
    'A09':'speed',                   # 速度
    'A13':'brake_pos',               # 牵引/制动档级
    'A14':'const_speed',             # 恒速：挡位/制动信息
    'A16':'mcr_coach',               # 主控车
    'C01':'vcb_status',              # VCB状态        # 本单元主断状态 1=闭合状态；2=断开状态；3=切除状态；0=通信故障
    'C04':'volt3_value',             # 3次电压
    'C99':'t16_k',                   # K开通状态
    'B17':'emergency_short',         # 紧急短路(空气制动切除M174)
    'B02':'as_press',                # 空簧压力
    'B03':'restop_graph_v',          # 再生制动力图形电压
    'B04':'mr_press',                # 总风压
    'C07':'restop_back',             # 再生制动力反馈
    'B00':'stop_valid',              # 制动信息有效
    'B01':'bc_press',                # BC压
    'C00':'ci_valid',                # CI信息有效
    'C06':'rotor_rate',              # 转子频率数
    'C05':'dc_mm_back',              # 牵引电机电流
    'C08':'spin_flag',               # 空转
    'B14':'glide_flag',              # 滑行
    'B16':'compressor_off',          # 压缩机切除
    'D00':'battery_valid',           # 蓄电池电压有效
    'D01':'battery_v',               # 蓄电池电压
    'C13':'m_off',                   # M车切除
    'C42':'water_in_degree',         # 主变流器冷却水进口温度
    'C12':'vcb_off',                 # VCB切除
    'G00':'aircond_valid',           # 空调信息有效
    'G02':'aircond_work_status',     # 空调运转模式
    'G03':'aircond_half_status',     # 空调负载减半
    'H25':'out_sensor_degree',       # 外温传感器信息温度
    'G05':'aircond_cool_degree',     # 空调控制温度制冷
    'A19':'save_58_tatus',           # 救援58信息
    'B122':'bpsave_device_status',   # BP救援装置
    'H28':'bpsave_valid',            # BP救援有效信号
    'S23':'save_conver_status',      # 救援转换装置电源空开
    'G07':'coach_avg_degree',        # 室内平均温度
    'S19':'mrc_egcs1',               # 主控端EGCS1（司机室保护接地开关）
    'S21':'dirs_signal',             # DIRS电平信号
    'H71':'tbr_signal',              # TBR电平信号
    'A45':'control_order',           # 控制指令（CI）-10
    'B61':'air_compressor_status',   #空压机状态
    'B107':'air_brake_status',       # 空气制动状态
    'A21':'brake_lvl',               # 制动级位
    'B121':'eb_cmd',                 # 紧急制动EB命令/EB制动工况
    'A41':'union_train_no',          # 联挂列号
    'B118':'dc_ctv_off',             # 牵引逆变器切除（动车切除）
    'C67':'dc_trans_degree',         # 牵引变压器温度
    'B119':'axle_slide',             # 车轴滑行
    'K03':'axle_slide_c1',           # 1轴累计滑行次数
    'K04':'axle_slide_c2',           # 2轴累计滑行次数
    'K05':'axle_slide_c3',           # 3轴累计滑行次数
    'K06':'axle_slide_c4',           # 4轴累计滑行次数
    'C35':'mid_dc_volt',             # 中间直流电压
    'C16':'network_volt',             # 网压
    'B06':'emergency_brake',        # 紧急制动
    'B132':'emergency_brake_ebr',   # 紧急制动EBR
    'B131':'emergency_brake_jtr',    # 紧急制动JTR
    'A47':'ci_order',                # 空挡CI指令
    'C09':'pantograph_status',       # 受电弓状态
    'A15':'brake_info',              # 制动信息
    'B08':'alert_brake',              # 警惕制动
    'C257':'dc_low_voltage2',       # 直流低电压2
    'C258':'cool_device_temp',      # 冷却设备过温度
    'C259':'machine_temp_high',     # 机器室内过温度
    'C260':'blower_stop',           # CI/MM鼓风机停止
    'C261':'ovth_malfunction',      # OVTh误动作
    'C262':'main_trans_abnormal',   # 主变压器异常
    'A82':'traction_1_grade',        # 牵引1档  控制指令（CI）-11
    'A17':'direction',                # 方向           00_01_10_11	无方向_前进_后退_故障
    'C43':'water_out_degree',         #主变流器冷却水出口温度
    'C50':'aux_trans_degree',         #辅助变压器温度
    'B07':'em_brake_switch',          #紧急制动拉动开关
    'S48':'park_brake_order'            #停放制动指令
}

# 定义GLIDE和空转参数
p_glide = {
    'K01':'glide_flag',              # 滑行标志
    'K03':'glide1_num',              # 1轴累计滑行次数
    'K04':'glide2_num',              # 2轴累计滑行次数
    'K05':'glide3_num',              # 3轴累计滑行次数
    'K06':'glide4_num',              # 4轴累计滑行次数
    'K00':'spin_flag',               # 空转标志
    'K02':'spin_num',                # 累计空转次数
}

# 故障数据参数
p_fault = {
    '001':'fault_code_001',          # 牵引变流器-控制电源“关”
    '002':'fault_code_002',          # 牵引变流器-传输不良
    '004':'fault_code_004',          # 牵引变流器故障1
    '005':'fault_code_005',          # 牵引变流器故障2
    '084':'fault_code_084',          # 084抱死预告检测
    '113':'fault_code_113',          # 配电柜NFB关
    '134':'fault_code_134',          # 牵引变流器-通风机停止
    '135':'fault_code_135',          # 辅助电源装置故障
    '139':'fault_code_139',          # 牵引变流器-微机故障
    '141':'fault_code_141',          # 牵引变流器-故障
    '143':'fault_code_143',          # 辅助电源装置通风机停止
    '144':'fault_code_144',          # 辅助电源装置ARfN2跳闸
    '146':'fault_code_146',          # 辅助电源装置 ACVN1跳闸
    '147':'fault_code_147',          # 辅助电源装置 ACVN2跳闸
    '148':'fault_code_148',          # 辅助电源装置 ATN 跳闸
    '166':'fault_code_166',          # 辅助电源装置VDTN跳闸
    '201':'fault_code_201',          # 辅助电源装置控制电源“关”
    '204':'fault_code_204',          # 辅助电源装置传输不良
    '132':'fault_code_132',          # 牵引变压器-油流停止
    '008':'fault_code_008',          # 门电源电压低下（GPLV）
    '014':'fault_code_014',          # 电动机过电流 2(MMOC2)
    '015':'fault_code_015',          # 电动机过电流 1(MMOC1)
    '018':'fault_code_018',          # 电动机电流不平衡(PUD)
    '021':'fault_code_021',          # PG 故障(PGD)
    '022':'fault_code_022',          # 控制电源电压低下(CPLV)
    '024':'fault_code_024',          # 0VTh 误动作(0VTGD)
    '030':'fault_code_030',          # 牵引不动作(MFD)
    '050':'fault_code_050',          # 2次侧接点 2(GD2)
    '054':'fault_code_054',          # 直流 100V 异常(P100LV)
    '055':'fault_code_055',          # 2次侧接点 1(GD1)
    '139':'fault_code_139',          # 微机异常WDT
    '012':'fault_code_012',          # 2次过电流 1(ISOC1)
    '006':'fault_code_006',          # 主电路元件异常（IGTFD）
    '019':'fault_code_019',          # 充电不良（CHF）
}

# 参数类：文件字段定义(从HBase导出数据时按此顺序映射)
cols_sdr = {
    'train_type_code':0,          # 车型
    'train_no':1,                 # 列号
    'coach_id':2,                 # 车号
    'time_value':3,               # 时间
    'trainsite_valid':4,          # 车次有效
    'trainsite_id':5,             # 车次
    'union_status':6,             # 联挂有效
    'speed_valid':7,              # 速度有效
    'speed':8,                    # 速度
    'brake_pos':9,                # 牵引制动档位
    'mcr_coach':10,               # 主控车
    'vcb_status':11,              # VCB状态
    'volt3_value':12,             # 3次电压
    't16_k':13,                   # K开通状态
    'emergency_short':14,         # 紧急短路(空气制动切除M174)
    'restop_graph_v':15,          # 再生制动力图形电压
    'mr_press':16,                # 总风压
    'restop_back':17,             # 再生制动力反馈
    'stop_valid':18,              # 制动信息有效
    'bc_press':19,                # BC压
    'ci_valid':20,                # CI信息有效
    'rotor_rate':21,              # 转子频率数
    'dc_mm_back':22,              # 牵引电机电流
    'compressor_off':23,          # 压缩机切除
    'battery_valid':24,           # 蓄电池电压有效
    'battery_v':25,               # 蓄电池电压
    'm_off':26,                   # M车切除
    'as_press':27,                # 空簧压力
    'const_speed':28,             # 恒速：挡位/制动信息
    'glide_flag':29,              # 滑行
    'spin_flag':30,               # 空转
    'water_in_degree':31,         # 主变流器冷却水进口温度
    'vcb_off':32,                 # VCB切除
    'save_58_tatus':33,           # 救援58信息
    'aircond_valid':34,           # 空调信息有效
    'aircond_work_status':35,     # 空调运转模式
    'aircond_half_status':36,     # 空调负载减半
    'out_sensor_degree':37,       # 外温传感器信息温度
    'aircond_cool_degree':38,     # 空调控制温度制冷
    'bpsave_device_status':39,    # BP救援装置
    'bpsave_valid':40,            # BP救援有效信号
    'save_conver_status':41,      # 救援转换装置电源空开
    'coach_avg_degree':42,        # 室内平均温度
    'mrc_egcs1':43,               # 主控端EGCS1（司机室保护接地开关）
    'dirs_signal':44,             # DIRS电平信号
    'tbr_signal':45,              # TBR电平信号
    'control_order':46,           # 控制指令（CI）-10
    'air_compressor_status':47,   #空压机工作状态 0_1_2_3_4_5	信故障_故障_切除_工作_油预热_未工作
    'air_brake_status':48,        # 空气制动状态
    'brake_lvl':49,               # 制动级位
    'eb_cmd':50,                  # 紧急制动EB命令/EB制动工况
    'union_train_no':51,          # 联挂列号
    'dc_ctv_off':52,              # 牵引逆变器切除（动车切除）
    'dc_trans_degree':53,         # 牵引变压器温度
    'axle_slide':54,              # 车轴滑行
    'axle_slide_c1':55,           # 1轴累计滑行次数
    'axle_slide_c2':56,           # 2轴累计滑行次数
    'axle_slide_c3':57,           # 3轴累计滑行次数
    'axle_slide_c4':58,           # 4轴累计滑行次数
    'mid_dc_volt':59,             # 中间直流电压
    'network_volt':60,             # 网压
    'emergency_brake':61,          # 紧急制动       0_1	正常_施加紧急制动
    'emergency_brake_ebr':62,        # 紧急制动EBR   0_1	无效_有效
    'emergency_brake_jtr':63,        # 紧急制动JTR   0_1	无效_有效
    'ci_order':64,                   # 空挡CI指令
    'pantograph_status':65,          # 受电弓状态
    'brake_info':66,                 # 制动信息
    'alert_brake': 67,                # 警惕制动
    'dc_low_voltage2':68,       # 直流低电压2
    'cool_device_temp':69,      # 冷却设备过温度
    'machine_temp_high':70,     # 机器室内过温度
    'blower_stop':71,           # CI/MM鼓风机停止
    'ovth_malfunction':72,      # OVTh误动作
    'main_trans_abnormal':73,   # 主变压器异常
    'traction_1_grade':74,        # 牵引1档  控制指令（CI）-11
    'direction':75,              # 方向            00_01_10_11	无方向_前进_后退_故障
    'water_out_degree':76,        #主变流器冷却水出口温度
    'aux_trans_degree':77,         #辅助变压器温度
    'em_brake_switch':78,          #紧急制动拉动开关
    'park_brake_order':79            #停放制动指令
    
}


# 故障类：文件字段定义(从HBase导出数据时按此顺序映射)
cols_fault = {
    'train_type_code':0,          # 车型
    'train_no':1,                 # 列号
    'coach_id':2,                 # 车号
    'fault_code_001':3,           # 牵引变流器-控制电源“关”
    'fault_code_002':4,           # 牵引变流器-传输不良
    'fault_code_004':5,           # 牵引变流器故障1
    'fault_code_005':6,           # 牵引变流器故障2
    'fault_code_084':7,           # 084抱死预告检测
    'fault_code_113':8,           # 配电柜NFB关
    'fault_code_134':9,           # 牵引变流器-通风机停止
    'fault_code_135':10,          # 辅助电源装置故障
    'fault_code_139':11,          # 牵引变流器-微机故障
    'fault_code_141':12,          # 牵引变流器-故障
    'fault_code_143':13,          # 辅助电源装置通风机停止
    'fault_code_144':14,          # 辅助电源装置ARfN2跳闸
    'fault_code_146':15,          # 辅助电源装置 ACVN1跳闸
    'fault_code_147':16,          # 辅助电源装置 ACVN2跳闸
    'fault_code_148':17,          # 辅助电源装置 ATN 跳闸
    'fault_code_166':18,          # 辅助电源装置VDTN跳闸
    'fault_code_201':19,          # 辅助电源装置控制电源“关”
    'fault_code_204':20,          # 辅助电源装置传输不良
    'fault_code_132':21,          # 牵引变压器-油流停止
    'fault_code_008':22,          # 门电源电压低下（GPLV）
    'fault_code_014':23,          # 电动机过电流 2(MMOC2)
    'fault_code_015':24,          # 电动机过电流 1(MMOC1)
    'fault_code_018':25,          # 电动机电流不平衡(PUD)
    'fault_code_021':26,          # PG 故障(PGD)
    'fault_code_022':27,          # 控制电源电压低下(CPLV)
    'fault_code_024':28,          # 0VTh 误动作(0VTGD)
    'fault_code_030':29,          # 牵引不动作(MFD)
    'fault_code_050':30,          # 2次侧接点 2(GD2)
    'fault_code_054':31,          # 直流 100V 异常(P100LV)
    'fault_code_055':32,          # 2次侧接点 1(GD1)
    'fault_code_139':33,          # 微机异常WDT
    'fault_code_012':34,          # 2次过电流 1(ISOC1)
    'fault_code_006':35,          # 主电路元件异常（IGTFD）
    'fault_code_019':36,          # 充电不良（CHF）
    'time_value':37,              # 时间
}

# 轴温传感器温度数据
p_sensor = {
    # 一路轴温
    'Z03':'1',    # 1位轴箱
    'Z04':'2',    # 2位轴箱
    'Z05':'3',    # 3位轴箱
    'Z06':'4',    # 4位轴箱
    'Z07':'5',    # 5位轴箱
    'Z08':'6',    # 6位轴箱
    'Z09':'7',    # 7位轴箱
    'Z10':'8',    # 8位轴箱
    'Z11':'9',    # 1位小齿轮电机侧
    'Z12':'10',   # 2位小齿轮电机侧
    'Z13':'11',   # 3位小齿轮电机侧
    'Z14':'12',   # 4位小齿轮电机侧
    'Z15':'13',   # 1位小齿轮车轮侧
    'Z16':'14',   # 2位小齿轮车轮侧
    'Z17':'15',   # 3位小齿轮车轮侧
    'Z18':'16',   # 4位小齿轮车轮侧
    'Z19':'17',   # 1位电机定子
    'Z20':'18',   # 1位电机传动端
    'Z21':'19',   # 1位电机非传动端
    'Z22':'20',   # 2位电机定子
    'Z23':'21',   # 2位电机传动端
    'Z24':'22',   # 2位电机非传动端
    'Z25':'23',   # 3位电机定子
    'Z26':'24',   # 3位电机传动端
    'Z27':'25',   # 3位电机非传动端
    'Z28':'26',   # 4位电机定子
    'Z29':'27',   # 4位电机传动端
    'Z30':'28',   # 4位电机非传动端
    'Z31':'29',   # 1位大齿轮电机侧
    'Z32':'30',   # 2位大齿轮电机侧
    'Z33':'31',   # 3位大齿轮电机侧
    'Z34':'32',   # 4位大齿轮电机侧
    'Z35':'33',   # 1位大齿轮车轮侧
    'Z36':'34',   # 2位大齿轮车轮侧
    'Z37':'35',   # 3位大齿轮车轮侧
    'Z38':'36',   # 4位大齿轮车轮侧
     # 2路轴温
    'Z39':'37',    # 1位轴箱
    'Z40':'38',    # 2位轴箱
    'Z41':'39',    # 3位轴箱
    'Z42':'40',    # 4位轴箱
    'Z43':'41',    # 5位轴箱
    'Z44':'42',    # 6位轴箱
    'Z45':'43',    # 7位轴箱
    'Z46':'44',    # 8位轴箱
    'Z47':'45',    # 1位小齿轮电机侧
    'Z48':'46',   # 2位小齿轮电机侧
    'Z49':'47',   # 3位小齿轮电机侧
    'Z50':'48',   # 4位小齿轮电机侧
    'Z51':'49',   # 1位小齿轮车轮侧
    'Z52':'50',   # 2位小齿轮车轮侧
    'Z53':'51',   # 3位小齿轮车轮侧
    'Z54':'52',   # 4位小齿轮车轮侧
    'Z55':'53',   # 1位电机定子
    'Z56':'54',   # 1位电机传动端
    'Z57':'55',   # 1位电机非传动端
    'Z58':'56',   # 2位电机定子
    'Z59':'57',   # 2位电机传动端
    'Z60':'58',   # 2位电机非传动端
    'Z61':'59',   # 3位电机定子
    'Z62':'60',   # 3位电机传动端
    'Z63':'61',   # 3位电机非传动端
    'Z64':'62',   # 4位电机定子
    'Z65':'63',   # 4位电机传动端
    'Z66':'64',   # 4位电机非传动端
    'Z67':'65',   # 1位大齿轮电机侧
    'Z68':'66',   # 2位大齿轮电机侧
    'Z69':'67',   # 3位大齿轮电机侧
    'Z70':'68',   # 4位大齿轮电机侧
    'Z71':'69',   # 1位大齿轮车轮侧
    'Z72':'70',   # 2位大齿轮车轮侧
    'Z73':'71',   # 3位大齿轮车轮侧
    'Z74':'72',   # 4位大齿轮车轮侧
    'A09':'speed',
    'H25':'out_degree',
    'A06':'trainsite_id',
    'A13':'brake_pos',
    'A02':'trainsite_valid'
}

p_air = {'A02':'trainsite_valid',   # 车次有效
    'A03':'speed_valid',             # 速度有效
    'A06':'trainsite_id',            # 车次
    'A09':'speed',                    # 速度
    'A13':'brake_pos',               # 牵引/制动档级
    'G11': 'air_tartemp',            # 目标温度值
    'G12': 'air_dettemp',            # 客室内温度检测值
    'G15': 'air_run_modal',          # 空调运行模式
    'G01': 'air_con_modal',          # 空调控制模式
    'G02': 'air_rev_modal',          # 空调运转模式
    'A10':'air_1',                   # 运营里程
    'G03':'air_2',                   # 减载
    'G18':'air_3',                   # 制冷系统1高压压力值
    'G19':'air_4',                   # 制冷系统2高压压力值
    'G20':'air_5',                   # 制冷系统1低压压力值
    'G21':'air_6',                   # 制冷系统2低压压力值
    'G22':'air_7',                   # 紧急通风逆变器运转
    'G23':'air_8',                   # 1位端通过台电加热运转
    'G24':'air_9',                   # 2位端通过台电加热运转
    'G25':'air_10',                  # 风道电加热运转
    'G26':'air_11',                  # 电加热1故障
    'G27':'air_12',                  # 电加热1运转
    'G28':'air_13',                  # 电加热2故障
    'G29':'air_14',                  # 电加热2运转
    'G30':'air_15',                  # 通风机1故障
    'G31':'air_16',                  # 通风机1运转
    'G32':'air_17',                  # 通风机2故障
    'G33': 'air_18',                 # 通风机2运转
    'G34': 'air_19',                 # 冷凝风机1故障
    'G35': 'air_20',  # 冷凝风机1运转
    'G36': 'air_21',  # 冷凝风机2故障
    'G37': 'air_22',  # 冷凝风机2运转
    'G38': 'air_23',  # 新风A1阀关
    'G39': 'air_24',  # 新风A2阀关
    'G40': 'air_25',  # 新风B1阀关
    'G41': 'air_26',  # 新风B2阀关
    'G42': 'air_27',  # 废排风门关
    'G43': 'air_28',  # 废排压力波阀关
    'G44': 'air_29',  # 废排风机故障
    'G45': 'air_30',  # 废排风机运转
    'G46': 'air_31',  # 司机室压缩机运转
    'G47': 'air_32',  # 司机室冷凝风机运转
    'G48': 'air_33',  # 司机室通风机运转
    'B04': 'air_34',  # 总风压力
    'D04': 'air_35',  # 锂电池组1电量
    'D05': 'air_36',  # 锂电池组2电量
    'D06': 'air_37',  # 充电机1中间电压
    'D07': 'air_38',  # 充电机2中间电压
    'D08': 'air_39',  # 充电机1输出电压
    'D09': 'air_40',  # 充电机2输出电压
    'D14': 'air_41',  # 充电机1输出电流
    'D15': 'air_42',  # 充电机2输出电流
    'D16': 'air_43',  # 充电机1充电电流
    'D17': 'air_44',  # 充电机2充电电流
    'G17':'new_wind_dettemp',        #新风温度检测值
    'G16':'tour_dettemp',            #观光区温度检测值
    'G65':'compressor1_run',         #压缩机1运转
    'G64':'compressor1_fault',       #压缩机1故障
    'G63':'compressor2_run',         #压缩机2运转
    'G62':'compressor2_fault',       #压缩机2故障
    'B21': 'as_press1',               # 空簧压力1
    'B22': 'as_press2',               # 空簧压力2
}

# 车次黑名单(需要排除的以下调试车次) 'G61'次交路已经开通，于20180918去掉
trainsiteBlackList = ['D60', 'D61', 'D62', 'D63', 'D64']

# HBase连接服务端主机名
hbase_thriftserver = '10.73.95.1'


# 定义SDR扩展参数
p_sdrext = {'A02':'trainsite_valid', # 车次有效
    'A03':'speed_valid',             # 速度有效
    'A06':'trainsite_id',            # 车次
    'A07':'union_status',            # 联挂状态
    'A09':'speed',                   # 速度
    'A13':'brake_pos',               # 牵引/制动档级
    'A14':'const_speed',             # 恒速：挡位/制动信息
    'A16':'mcr_coach',               # 主控车
    'C01':'vcb_status',              # VCB状态
    'C04':'volt3_value',             # 3次电压
    'C99':'t16_k',                   # K开通状态
    'B17':'emergency_short',         # 紧急短路(空气制动切除M174)
    'B02':'as_press',                # 空簧压力
    'B03':'restop_graph_v',          # 再生制动力图形电压
    'B04':'mr_press',                # 总风压
    'C07':'restop_back',             # 再生制动力反馈
    'B00':'stop_valid',              # 制动信息有效
    'B01':'bc_press',                # BC压
    'C00':'ci_valid',                # CI信息有效
    'C06':'rotor_rate',              # 转子频率数
    'C05':'dc_mm_back',              # 牵引电机电流
    'C08':'spin_flag',               # 空转
    # 'B119':'glide_flag',              # 滑行                # 同axle_slide 重复，去掉该字段并更改使用该字段的脚本 by lxj 20180622
    'B16':'compressor_off',          # 压缩机切除
    'D00':'battery_valid',           # 蓄电池电压有效
    'D01':'battery_v',               # 蓄电池电压
    'C13':'m_off',                   # M车切除
    'C42':'water_in_degree',         # 主变流器冷却水进口温度
    'C12':'vcb_off',                 # VCB切除
    'A17':'direction',               # 方向
    'A41':'union_train_no',          # 联挂列号
    'C14':'vcb_down',                # VCB闭合
    'B116':'main_compressor_off',    # 主空压机切除指令
    'C39':'assist_volt',             # 辅变母线电压
    'A21':'brake_lvl',               # 制动级位
    'B102':'e_brake',                 # 电制动能力/本车电制动可用
    'B117':'dc_cvt_off',             # 牵引变流器切除
    'B118':'dc_ctv_off',             # 牵引逆变器切除（动车切除）
    'C21':'tcp_keep_model',          # TCU中压保持模式
    'B119':'axle_slide',             # 车轴滑行
    'C58':'tcu_restop_brake',        # TCU反馈再生制动力
    'C57':'bcu_request_brake',       # BCU请求再生制动力
    'C24':'ee1_pull_value',          # 1架电制力实际值/一架电机牵引
    'C25':'ee2_pull_value',          # 2架电制力实际值/二架电机牵引
    'B107':'air_brake_status',       # 空气制动状态
    'K03':'axle_slide_c1',           # 1轴累计滑行次数
    'K04':'axle_slide_c2',           # 2轴累计滑行次数
    'K05':'axle_slide_c3',           # 3轴累计滑行次数
    'K06':'axle_slide_c4',           # 4轴累计滑行次数
    'A39':'train_weight',            # 列车实际质量
    'B120':'coach_weight',           # 本车实际质量
    'B121':'eb_cmd',                 # 紧急制动EB命令/EB制动工况
    'C54':'shelf1_speed',            # 一架电机综合速度
    'C55':'shelf2_speed',            # 二架电机综合速度
    'C32':'dc_trans_elec',           # 牵引变压器原边电流
    'C33':'shelf1_quad4_elec',       # 一架四象限输入电流
    'C34':'shelf2_quad4_elec',       # 一架四象限输入电流
    'C36':'shelf1_engi_elec',        # 一架电机电流
    'C37':'shelf2_engi_elec',        # 二架电机电流
    'C52':'shelf1_engi_power',       # 一架电机实际力
    'C53':'shelf2_engi_power',       # 二架电机实际力
    'C69':'shelf_off',               # 转向架切除（牵引逆变器状态）
    'C61':'power_percent',           # 牵引/电制动力百分比（实际牵引/电制动力百分比）
    'C77':'urgent_pull_relay',       # 紧急牵引指令继电器
    'K02':'pull_idle_count',         # 牵引空转次数
    'C16':'network_volt',            # 网压
    'C35':'mid_dc_volt',             # 中间直流电压
    'C67':'dc_trans_degree',         # 牵引变压器温度
    'C42':'dc_cvt_water_in1_degree', # 牵引变流器冷却水进口1温度
    'C43':'dc_cvt_water_out1_degree',# 牵引变流器冷却水出口1温度
    'C44':'dc_cvt_water_in2_degree', # 牵引变流器冷却水进口2温度
    'C45':'dc_cvt_water_out2_degree', # 牵引变流器冷却水出口2温度
    'G18':'refrigerator_1_high_press',   #制冷系统1高压压力值
    'G19':'refrigerator_2_high_press',   #制冷系统2高压压力值
    'G20':'refrigerator_1_low_press',    #制冷系统1低压压力值
    'G21':'refrigerator_2_low_press',    #制冷系统2低压压力值
    'G17':'new_wind_dettemp',            #E32B新风温度检测值
    'A28':'new_wind_dettemp',            #E32 室外温度
    'G16':'tour_dettemp',                #观光区温度检测值
    'H67':'car_door_1_closed',           #车门1关闭
    'H68':'car_door_2_closed',           #车门2关闭
    'H69':'car_door_3_closed',           #车门3关闭
    'H70':'car_door_4_closed',           #车门4关闭
    'G12':'air_dettemp',                 # 客室内温度检测值
    'G11':'air_tartemp',                 # 目标温度值
    'D08':'charger_out_v1',              # 充电机1输出电压
    'D09':'charger_out_v2',              # 充电机2输出电压
    'D04':'battery_group_qty1',          # 锂电池组1电量
    'D05':'battery_group_qty2',          # 锂电池组2电量
    'D16':'charger_up_v1',               # 充电机1充电电流
    'D17':'charger_up_v2',               # 充电机2充电电流
    'D21':'charger_off1',                # 充电机1切除
    'D22':'charger_off2',                # 充电机2切除
    'B123':'park_brake_order',           # 停放制动施加指令
    'B46':'park_brake_status',           # 停放制动施加
    'B105':'park_brake_press',           # 停放缸压力
    'B61':'air_compressor_status',       # 空压机状态
    'C09':'pantograph_status',           # 受电弓状态
    'C85':'dc_trans_air_open',           # 牵引变压器油泵空开
    'C87':'dc_trans_oil_stop_relay',     # 牵引变压器油流停止继电器
    'P21':'high_off_switch_status',      # 高压隔离开关状态
    'C125':'vcb_off_monitor',            # vcb闭合监视
    'C126':'pantograph_up',              # 受电弓升起
    'P22':'high_off_switch_down',        # 高压隔离开关闭合
    'C38': 'aux_out_voltage',            # 辅变输出电压
    'C40': 'aux_out_current',            # 辅变输出电流
    'C46':'dc_cvt_water_in1_press',      # 牵引变流器冷却水进口1压力
    'C48':'dc_cvt_water_in2_press',      # 牵引变流器冷却水进口2压力
    'G02': 'air_rev_modal',               # 空调运转模式
    'B106':'stop_brake_status',           # 停放制动状态
    'C154': 'phase3_bus_relay1',            # 三相母线有电检测继电器1
    'C155': 'tmotor_fan_contactor1',         # 牵引电机风机1接触器1
    'C156': 'tmotor_fan_contactor2',         # 牵引电机风机1接触器2
    'Q32': 'ATP_power_switch',               # ATP电源控制开关            0_1 无效_有效
    'B157': 'ATP_7_brake',                   # ATP七级制动
    'A62': 'direction_zero',                 # 方向零位
    'A63': 'forward1',                       # 前向1
    'A64': 'forward2',                       # 前向2
    'Q16': 'rescue_switch_mid_pos',          # 救援开关中间位
    'A74': 'rescue_devic_contrl',               # 救援装置控制                0_1 无效_有效
    'Q27': 'rescue_switch_rescued',             # 救援开关被救援位          0_1 无效_有效
    'Q30': 'rescue_switch_rescue',               # 救援开关救援位          0_1 无效_有效
    'C70': 'aux_con_status',		                	# 辅助变流器状态        0_1_2_3_4_5_6无效_工作_非工作_故障_切除_隔离_通信故障"
    'C50': 'aux_trans_degree',                       # 辅助变压器温度
    'C51': 'tconverter_air_degree',                  # 牵引变流器柜体空气温度
    'B34':'bp_press',                                   # BP压力 --列车管压力
    'A71':'back1',                                      # 后向1
    'A72':'back2',                                      # 后向2
    'C158':'short1_contactor',                           # 一架短接接触器
    'C159':'short2_contactor',                           # 二架短接接触器
    'C160':'ac380_bus_contactor',                        # 交流380V母线接触器 0_1 断开 闭合
    'C68':'tract_conver_status',                         # 牵引变流器状态 0_1_2_3_4_5_6  无效_工作_非工作_故障_切除_隔离_通信故障
    'C161':'phase3_bus_relay2',                          # 三相母线有电检测继电器2
    'A74':'bpsave_devicontrl_air',                      # BP救援装置控制空开
    'D23':'charger1_115contactor',                      # 充电机1的115线接触器
    'D25':'charger2_115contactor',                       # 充电机2的115线接触器
    'K08':'tow_slide_count',                      # 牵引滑行次数
    'C62':'set_power_percent',                    # 设定牵引/电制动力百分比
    'Y75':'passing_neutral_section',              # 过分相
    'Q12':'emergency_mode_switch1',              # 紧急模式开关1   0_1  非紧急牵引模式_紧急牵引模式
    'Q13': 'emergency_mode_switch2',             # 紧急模式开关2    0_1  非紧急牵引模式_紧急牵引模式
    'C256': 'Auxiliary_inverter_cut',            # 辅助逆变器切除状态   0_1 未切除_切除
    'B175':'air_brake_isolation'     ,             #空气制动隔离
    'D24':'charger1_103_contactor',               #充电机1的103线接触器
    'D26':'charger2_103_contactor',                #充电机2的103线接触器
    'G65': 'compressor1_run',                       # 压缩机1运转
    'G64': 'compressor1_fault',                     # 压缩机1故障
    'G63': 'compressor2_run',                       # 压缩机2运转
    'G62': 'compressor2_fault',                     # 压缩机2故障
    'C75': 'Charger_1_status',                      # 充电机1状态
    'C76': 'Charger_2_status',                      # 充电机2状态
    'B314':'UB_air_brake_effective',                 # UB空气制动有效  0_1 对应 无效_有效
    'Q25':'brake_isolation_switch',                # 保持制动隔离开关    0_1:无效_有效
    'B315':'keep_brake_app_status',	              # 保持制动施加状态    0_1:未施加_施加
    'Q38':'relieve_keep_brake_switch',	          # 缓解保持制动开关    0_1:无效_有效
    'G60':'change_signal',	                          # 换端信号
    'D106':'charge_battery_group1_tem1',  # 碱性蓄电池组1温度1|充电机1温度1
    'D105':'charge_battery_group1_tem2',  # 碱性蓄电池组1温度2|充电机1温度2
    'D119':'charge_battery_group2_tem1',  # 碱性蓄电池组2温度1|充电机2温度1
    'D118':'charge_battery_group2_tem2'  # 碱性蓄电池组2温度2|充电机2温度2
}


# 参数类：文件字段定义(从HBase导出数据时按此顺序映射)
cols_sdrext = {
    'train_type_code':0,          # 车型
    'train_no':1,                 # 列号
    'coach_id':2,                 # 车号
    'time_value':3,               # 时间
    'trainsite_valid':4,          # 车次有效
    'trainsite_id':5,             # 车次
    'union_status':6,             # 联挂有效
    'speed_valid':7,              # 速度有效
    'speed':8,                    # 速度
    'brake_pos':9,                # 牵引制动档位
    'mcr_coach':10,               # 主控车
    'vcb_status':11,              # VCB状态
    'volt3_value':12,             # 3次电压
    't16_k':13,                   # K开通状态
    'emergency_short':14,         # 紧急短路(空气制动切除M174)
    'restop_graph_v':15,          # 再生制动力图形电压
    'mr_press':16,                # 总风压
    'restop_back':17,             # 再生制动力反馈
    'stop_valid':18,              # 制动信息有效
    'bc_press':19,                # BC压
    'ci_valid':20,                # CI信息有效
    'rotor_rate':21,              # 转子频率数
    'dc_mm_back':22,              # 牵引电机电流
    'compressor_off':23,          # 压缩机切除
    'battery_valid':24,           # 蓄电池电压有效
    'battery_v':25,               # 蓄电池电压
    'm_off':26,                   # M车切除
    'as_press':27,                # 空簧压力
    'const_speed':28,             # 恒速：挡位/制动信息
    'glide_flag':29,              # 滑行
    'spin_flag':30,               # 空转
    'water_in_degree':31,         # 主变流器冷却水进口温度
    'vcb_off':32,                 # VCB切除
    'direction':33,               # 方向
    'union_train_no':34,          # 联挂列号
    'vcb_down':35,                # VCB闭合
    'main_compressor_off':36,     # 主空压机切除指令
    'assist_volt':37,             # 辅变母线电压
    'brake_lvl':38,               # 制动级位
    'e_brake':39,                 # 电制动能力/本车电制动可用
    'dc_cvt_off':40,              # 牵引变流器切除
    'dc_ctv_off':41,              # 牵引逆变器切除
    'tcp_keep_model':42,          # TCU中压保持模式
    'axle_slide':43,              # 车轴滑行
    'tcu_restop_brake':44,        # TCU反馈再生制动力
    'bcu_request_brake':45,       # BCU请求再生制动力
    'ee1_pull_value':46,          # 1架电制力实际值/一架电机牵引
    'ee2_pull_value':47,          # 2架电制力实际值/二架电机牵引
    'air_brake_status':48,        # 空气制动状态
    'axle_slide_c1':49,           # 1轴累计滑行次数
    'axle_slide_c2':50,           # 2轴累计滑行次数
    'axle_slide_c3':51,           # 3轴累计滑行次数
    'axle_slide_c4':52,           # 4轴累计滑行次数
    'train_weight':53,            # 列车实际质量
    'coach_weight':54,            # 本车实际质量
    'eb_cmd':55,                  # 紧急制动EB命令/EB制动工况
    'shelf1_speed':56,            # 一架电机综合速度
    'shelf2_speed':57,            # 二架电机综合速度
    'dc_trans_elec':58,           # 牵引变压器原边电流
    'shelf1_quad4_elec':59,       # 一架四象限输入电流
    'shelf2_quad4_elec':60,       # 二架四象限输入电流
    'shelf1_engi_elec':61,        # 一架电机电流
    'shelf2_engi_elec':62,        # 二架电机电流
    'shelf1_engi_power':63,       # 一架电机实际力
    'shelf2_engi_power':64,       # 二架电机实际力
    'shelf_off':65,               # 转向架切除（牵引逆变器状态）
    'power_percent':66,           # 牵引/电制动力百分比（实际牵引/电制动力百分比）
    'urgent_pull_relay':67,       # 紧急牵引指令继电器
    'pull_idle_count':68,         # 牵引空转次数
    'network_volt':69,            # 网压
    'mid_dc_volt':70,             # 中间直流电压
    'dc_trans_degree':71,         # 牵引变压器温度
    'dc_cvt_water_in1_degree':72, # 牵引变流器冷却水进口1温度
    'dc_cvt_water_out1_degree':73,# 牵引变流器冷却水出口1温度
    'dc_cvt_water_in2_degree':74, # 牵引变流器冷却水进口2温度
    'dc_cvt_water_out2_degree':75,# 牵引变流器冷却水出口2温度
    'refrigerator_1_high_press':76,  # 制冷系统1高压压力值
    'refrigerator_2_high_press':77,  # 制冷系统2高压压力值
    'refrigerator_1_low_press':78,   # 制冷系统1低压压力值
    'refrigerator_2_low_press':79,   # 制冷系统2低压压力值
    'new_wind_dettemp':80,           # 新风温度检测值
    'tour_dettemp':81,               # 观光区温度检测值
    'car_door_1_closed':82,          # 车门1关闭
    'car_door_2_closed':83,          # 车门2关闭
    'car_door_3_closed':84,          # 车门3关闭
    'car_door_4_closed':85,          # 车门4关闭
    'air_dettemp':86,                # 客室内温度检测值
    'air_tartemp':87,                # 目标温度值
    'charger_out_v1':88,             # 充电机1输出电压
    'charger_out_v2':89,             # 充电机2输出电压
    'battery_group_qty1':90,         # 锂电池组1电量
    'battery_group_qty2':91,         # 锂电池组2电量
    'charger_up_v1':92,              # 充电机1充电电流
    'charger_up_v2':93,              # 充电机2充电电流
    'charger_off1':94,               # 充电机1切除
    'charger_off2':95,               # 充电机2切除
    'park_brake_order':96,           # 停放制动施加指令
    'park_brake_status':97,          # 停放制动施加
    'park_brake_press':98,           # 停放缸压力
    'air_compressor_status':99,       # 空压机状态
    'pantograph_status':       100,  # 受电弓状态                      0_1_2_3	通信故障_升弓_降弓_切除
    'dc_trans_air_open':       101,  # 牵引变压器油泵空开              0_1
    'dc_trans_oil_stop_relay': 102,  # 牵引变压器油流停止继电器        0_1
    'high_off_switch_status':  103,  # 高压隔离开关状态  	             0_1_2_3	通信故障_闭合_断开_切除
     'vcb_off_monitor':104,           # vcb闭合监视
    'pantograph_up':105,              #受电弓升起
    'high_off_switch_down':106,        #高压隔离开关闭合
    'aux_out_voltage': 107,             # 辅变输出电压
    'aux_out_current': 108,             # 辅变输出电流
    'dc_cvt_water_in1_press':109,     #牵引变流器冷却水进口1压力
    'dc_cvt_water_in2_press':110,      #牵引变流器冷却水进口2压力
    'air_rev_modal':111,              # 空调运转模式
    'stop_brake_status':112,           # 停放制动状态                 0_1_2_3  无效_施加_缓解_隔离
    'phase3_bus_relay1':113,            # 三相母线有电检测继电器1
    'tmotor_fan_contactor1':114,        # 牵引电机风机1接触器1
    'tmotor_fan_contactor2':115,        # 牵引电机风机1接触器2
    'ATP_power_switch':116,             # ATP电源控制开关
    'ATP_7_brake':117,                      # ATP七级制动
    'direction_zero':118,                # 方向零位
    'forward1':119,                        # 前向1
    'forward2':120,                        # 前向2
    'rescue_switch_mid_pos':121,         # 救援开关中间位
    'rescue_devic_contrl':122,           # 救援装置控制
    'rescue_switch_rescued':123,        # 救援开关被救援位
    'rescue_switch_rescue':124,          # 救援开关救援位
    'aux_con_status':125,                   # 辅助变流器状态    0_1_2_3_4_5_6	无效_工作_非工作_故障_切除_隔离_通信故障
    'aux_trans_degree':126,                  # 辅助变压器温度
    'tconverter_air_degree':127,            # 牵引变流器柜体空气温度
    'bp_press':128,                            # BP压力  --列车管压力
    'back1':129,                                # 后向1
    'back2':130,                                # 后向2
    'short1_contactor':131,                   # 一架短接接触器
    'short2_contactor':132,                    # 二架短接接触器
    'ac380_bus_contactor':133,                # 交流380V母线接触器 0_1 断开 闭合
    'tract_conver_status':134,                 # 牵引变流器状态  0_1_2_3_4_5_6  无效_工作_非工作_故障_切除_隔离_通信故障
    'phase3_bus_relay2':135,                     # 三相母线有电检测继电器2
    'bpsave_devicontrl_air':136,                # BP救援装置控制空开
    'charger1_115contactor':137,                # 充电机1的115线接触器
    'charger2_115contactor':138,                # 充电机2的115线接触器
    'tow_slide_count':139,                 # 牵引滑行次数
    'set_power_percent':140,               # 设定牵引/电制动力百分比
    'passing_neutral_section':141,         # 过分相
    'emergency_mode_switch1':142,          # 紧急模式开关1     0_1  非紧急牵引模式_紧急牵引模式
    'emergency_mode_switch2':143,          # 紧急模式开关2     0_1  非紧急牵引模式_紧急牵引模式
    'Auxiliary_inverter_cut':144,           # 辅助逆变器切除状态  0_1 未切除_切除
    'air_brake_isolation':145,              #空气制动隔离
    'charger1_103_contactor':146,          #充电机1的103线接触器
    'charger2_103_contactor':147,          #充电机1的103线接触器
    'compressor1_run': 148,                 # 压缩机1运转
    'compressor1_fault': 149,               # 压缩机1故障
    'compressor2_run': 150,                 # 压缩机2运转
    'compressor2_fault': 151,               # 压缩机2故障
    'Charger_1_status': 152,                # 充电机1状态            0_1_2_3_4_5_6_7  通信故障_工作_故障_切除_未工作_无效_无效_无效
    'Charger_2_status': 153,                 # 充电机2状态            0_1_2_3_4_5_6_7  通信故障_工作_故障_切除_未工作_无效_无效_无效
    'UB_air_brake_effective':154,           #UB空气制动有效          0_1 无效_有效
    'brake_isolation_switch':155,           # 保持制动隔离开关    0_1:无效_有效
    'keep_brake_app_status':156,            # 保持制动施加状态    0_1:未施加_施加
    'relieve_keep_brake_switch':157,        # 缓解保持制动开关    0_1:无效_有效
    'change_signal':158,                      # 换端信号
    'charge_battery_group1_tem1': 159,  # 碱性蓄电池组1温度1|充电机1温度1
    'charge_battery_group1_tem2': 160,  # 碱性蓄电池组1温度2|充电机1温度2
    'charge_battery_group2_tem1': 161,  # 碱性蓄电池组2温度1|充电机2温度1
    'charge_battery_group2_tem2': 162  # 碱性蓄电池组2温度2|充电机2温度2
}



p_faultExt = {
    'N07':'fault_code',
    'N01':'fault_time',
    'N06':'fault_status',       
    'N08':'coach_id'
}

cols_faultExt={
    'train_type_code': 0,        # 车型
    'train_no': 1,               # 列号
    'coach_id': 2,               # 车号
    'fault_code': 3,             # 故障代码
    'fault_status': 4,           # 故障状态
    'time_value': 5              # 时间
}


p_sensor_mining = {
    # 1路轴温
    'Z03':'1',    # 1位轴箱
    'Z04':'2',    # 2位轴箱
    'Z05':'3',    # 3位轴箱
    'Z06':'4',    # 4位轴箱
    'Z07':'5',    # 5位轴箱
    'Z08':'6',    # 6位轴箱
    'Z09':'7',    # 7位轴箱
    'Z10':'8',    # 8位轴箱
    'Z11':'9',    # 1位小齿轮电机侧
    'Z12':'10',   # 2位小齿轮电机侧
    'Z13':'11',   # 3位小齿轮电机侧
    'Z14':'12',   # 4位小齿轮电机侧
    'Z15':'13',   # 1位小齿轮车轮侧
    'Z16':'14',   # 2位小齿轮车轮侧
    'Z17':'15',   # 3位小齿轮车轮侧
    'Z18':'16',   # 4位小齿轮车轮侧
    'Z19':'17',   # 1位电机定子
    'Z20':'18',   # 1位电机传动端
    'Z21':'19',   # 1位电机非传动端
    'Z22':'20',   # 2位电机定子
    'Z23':'21',   # 2位电机传动端
    'Z24':'22',   # 2位电机非传动端
    'Z25':'23',   # 3位电机定子
    'Z26':'24',   # 3位电机传动端
    'Z27':'25',   # 3位电机非传动端
    'Z28':'26',   # 4位电机定子
    'Z29':'27',   # 4位电机传动端
    'Z30':'28',   # 4位电机非传动端
    'Z31':'29',   # 1位大齿轮电机侧
    'Z32':'30',   # 2位大齿轮电机侧
    'Z33':'31',   # 3位大齿轮电机侧
    'Z34':'32',   # 4位大齿轮电机侧
    'Z35':'33',   # 1位大齿轮车轮侧
    'Z36':'34',   # 2位大齿轮车轮侧
    'Z37':'35',   # 3位大齿轮车轮侧
    'Z38':'36',   # 4位大齿轮车轮侧
    # 2路轴温
    'Z39':'37',    # 1位轴箱
    'Z40':'38',    # 2位轴箱
    'Z41':'39',    # 3位轴箱
    'Z42':'40',    # 4位轴箱
    'Z43':'41',    # 5位轴箱
    'Z44':'42',    # 6位轴箱
    'Z45':'43',    # 7位轴箱
    'Z46':'44',    # 8位轴箱
    'Z47':'45',    # 1位小齿轮电机侧
    'Z48':'46',   # 2位小齿轮电机侧
    'Z49':'47',   # 3位小齿轮电机侧
    'Z50':'48',   # 4位小齿轮电机侧
    'Z51':'49',   # 1位小齿轮车轮侧
    'Z52':'50',   # 2位小齿轮车轮侧
    'Z53':'51',   # 3位小齿轮车轮侧
    'Z54':'52',   # 4位小齿轮车轮侧
    'Z55':'53',   # 1位电机定子
    'Z56':'54',   # 1位电机传动端
    'Z57':'55',   # 1位电机非传动端
    'Z58':'56',   # 2位电机定子
    'Z59':'57',   # 2位电机传动端
    'Z60':'58',   # 2位电机非传动端
    'Z61':'59',   # 3位电机定子
    'Z62':'60',   # 3位电机传动端
    'Z63':'61',   # 3位电机非传动端
    'Z64':'62',   # 4位电机定子
    'Z65':'63',   # 4位电机传动端
    'Z66':'64',   # 4位电机非传动端
    'Z67':'65',   # 1位大齿轮电机侧
    'Z68':'66',   # 2位大齿轮电机侧
    'Z69':'67',   # 3位大齿轮电机侧
    'Z70':'68',   # 4位大齿轮电机侧
    'Z71':'69',   # 1位大齿轮车轮侧
    'Z72':'70',   # 2位大齿轮车轮侧
    'Z73':'71',   # 3位大齿轮车轮侧
    'Z74':'72',   # 4位大齿轮车轮侧
    'A09':'speed',
    'H25':'out_degree',
    'A06':'trainsite_id',
    'A13':'brake_pos',
    'A02':'trainsite_valid',
    'B02':'as_press',   # 空簧压力
    'A11':'plus_minus_speed',   # 加减速度
}

p_E44 = {'U03':'LRdoor_switch',                   # 左右门开关触点断不开
     'U04':'release_Ldoor_switch',                # 释放左门开关触点断不开
     'U05':'apply_parking_switch',                # 施加停放开关触点断不开
     'U06':'relief_parking_switch',               # 缓解停放开关触点断不开
     'U07':'fault_reset_switch',                  # 故障复位开关触点断不开
     'U08':'em_reset_switch',                     # 紧急复位开关触点断不开
     'U09':'broken_VCB_switch',                   # 断VCB开关触点断不开
     'U10':'VCB_switch',                          # 合VCB开关触点断不开
     'U11':'bow_switch',                          # 升弓开关触点断不开
     'U12':'drop_bow_switch',                     # 降弓开关触点断不开
     'U13':'open_Rdoor_switch',                   # 开右门开关触点断不开
     'U14':'close_Rdoor_switch',                  # 关右门开关触点断不开
     'U15':'release_Rdoor_switch',                # 释放右门开关触点断不开
     'U16':'open_Ldoor_switch',                   # 开左门开关触点断不开
     'U17':'asnormal_door11_close',               # 车门1-1关闭时间异常
     'U18':'asnormal_door12_close',               # 车门1-2关闭时间异常
     'U19':'asnormal_door21_close',               # 车门2-1关闭时间异常
     'U20':'asnormal_door22_close',               # 车门2-2关闭时间异常
     'U21':'relief_parking_brake',                # 缓解停放制动继电器动作不一致
     'U22':'relief_hold_brake',                   # 缓解保持制动继电器动作不一致
     'U23':'em_brake_EB',                         # 紧急制动EB安全环路继电器动作不一致
     'U24':'em_brake_UB',                         # 紧急制动UB安全环路继电器动作不一致
     'U25':'TBM_input',                           # TBM投入异常预警
     'U26':'brake_force',                         # 制动力异常预警
     'U27':'air_brake_force',                     # 空气制动力异常预警
     'U28':'ins_dec_train_level',                 # 列车级减速度不足预警
     'U29':'cyc_ATP_brake7',                      # 单车ATP七级制动继电器动作不一致
     'U30':'cyc_brake_command1',                  # 单车制动指令1继电器动作不一致
     'U31':'cyc_brake_command2',                  # 单车制动指令2继电器动作不一致
     'U32':'cyc_brake_command3',                  # 单车制动指令3继电器动作不一致
     'U33':'cyc_control_brake_status',            # 单车司控器制动状态继电器动作不一致
     'U34':'em_traction_command',                 # 紧急牵引指令继电器动作不一致
     'U35':'em_mode_relay',                       # 紧急模式继电器动作不一致
     'U36':'apply_parking_brake',                 # 施加停放制动继电器动作不一致
     'U37':'BJ_battery1_charged_floating_mode',   # 蓄电池1均充转浮充异常报警
     'U38':'YJ_battery1_charged_floating_mode',   # 蓄电池1均充转浮充异常预警
     'U39':'battery1_overcharged',                # 蓄电池1充电超时
     'U40':'battery1_charging_vlotage',           # 蓄电池1充电电压异常
     'U41':'BJ_battery2_charged_floating_mode',   # 蓄电池2均充转浮充异常报警
     'U42':'YJ_battery1_charged_floating_mode',   # 蓄电池2均充转浮充异常预警
     'U43':'battery2_overcharged',                # 蓄电池2充电超时
     'U44':'battery2_charging_vlotage',           # 蓄电池2充电电压异常
     'U45':'tcf_VCB_status',                      # 牵引变流器反馈VCB状态信号不一致
     'U46':'tcf_forward_signal',                  # 牵引变流器反馈前向信号不一致
     'U47':'tcf_backward_signal',                 # 牵引变流器反馈后向信号不一致
     'U48':'tcf_em_traction_mode',                # 牵引变流器反馈紧急牵引模式信号不一致
     'U49':'tcf_em_traction_command',             # 牵引变流器反馈紧急牵引指令信号不一致
     'U50':'tcf_em_braking_UB',                   # 牵引变流器反馈紧急制动UB信号不一致
     'U51':'tcf_disconnect_VCB_command',          # 牵引变流器反馈断VCB指令信号不一致
     'U52':'tcf_broken_rescue_power_status',      # 牵引变流器反馈回送救援发电状态信号不一致
     'U53':'tcm1_current_consist',                # 一架牵引变流器电机电流一致性异常
     'U54':'tcm2_current_consist',                # 二架牵引变流器电机电流一致性异常
     'U55':'tcm1_actual_force_consist',           # 一架牵引电机实际力一致性异常
     'U56':'tcm2_actual_force_consist',           # 二架牵引电机实际力一致性异常
     'U57':'current_trans_high_voltage',          # 电流互感器高压线缆接地
     'U58':'CT2_high_voltage',                    # 本单元CT2监测点后的连接线路高压线缆接地
     'U59':'tt_primary_consist',                  # 牵引变压器原边电流一致性异常预警
     'U60':'tc1_q4_current_consist',              # 一架牵引变流器四象限电流一致性异常
     'U61':'tc2_q4_current_consist',              # 二架牵引变流器四象限电流一致性异常
     'U62':'motor1_force_consist',                # 一架电机设定力一致性异常预警
     'U63':'motor2_force_consist',                # 二架电机设定力一致性异常预警
     'U64':'inter_DC_voltage_consist',            # 中间直流电压一致性异常预警
     'U65':'elec_brake_force_too_lage',           # 电制动力反馈偏差过大预警
     'U66':'max_elec_brake_capacity_too_large',   # 最大电制动能力值偏差过大预警
     'U67':'WSP1_motor_speed_too_large',          # 1轴WSP轴速度与电机速度偏差过大预警
     'U68':'WSP2_motor_speed_too_large',          # 2轴WSP轴速度与电机速度偏差过大预警
     'U69':'WSP3_motor_speed_too_large',          # 3轴WSP轴速度与电机速度偏差过大预警
     'U70':'WSP4_motor_speed_too_large',          # 4轴WSP轴速度与电机速度偏差过大预警
     'U71':'motor1_speed_too_large',              # 1轴电机速度与整车速度偏差过大预警
     'U72':'motor2_speed_too_large',              # 2轴电机速度与整车速度偏差过大预警
     'U73':'motor3_speed_too_large',              # 3轴电机速度与整车速度偏差过大预警
     'U74':'motor4_speed_too_large',              # 4轴电机速度与整车速度偏差过大预警
     'U75':'WPS1_vehicle_speed_too_large',        # 1轴WSP轴速度与整车速度偏差过大预警
     'U76':'WPS2_vehicle_speed_too_large',        # 2轴WSP轴速度与整车速度偏差过大预警
     'U77':'WPS3_vehicle_speed_too_large',        # 3轴WSP轴速度与整车速度偏差过大预警
     'U78':'WPS4_vehicle_speed_too_large',        # 4轴WSP轴速度与整车速度偏差过大预警
     'A09':'speed',                               # 速度
     'U100':'motor_fan2_contactor2_action',           #牵引电机风机2接触器2动作次数
     'U101':'converter_fan2_contactor1_action',       #牵引变流器风机2接触器1动作次数
     'U102':'converter_fan2_contactor2_action',       #牵引变流器风机2接触器2动作次数
     'U103':'transformer_fan1_contactor1_action',     #牵引变压器风机1接触器1动作次数
     'U104':'transformer_fan1_contactor2_action',     #牵引变压器风机1接触器2动作次数
     'U105':'transformer_fan2_contactor1_action',     #牵引变压器风机2接触器1动作次数
     'U106':'transformer_fan2_contactor2_action',     #牵引变压器风机2接触器2动作次数
     'U107':'breaker_action',                         #主断路器动作次数
     'U108':'air_compressor_contactor_action',        #主空压机接触器动作次数
     'U109':'ground_protection_switch_action',        #接地保护开关动作次数
     'U110':'high_voltage_switch_action',             #车高压隔离开关动作次数
     'U111':'ATP_power_switch_action',                #ATP电源空开动作次数
     'U79':'bow_switch_action',                       #升弓开关动作次数
     'U80':'release_Rdoor_switch_action',             #释放右门开关动作次数
     'U81':'open_Rdoor_switch_action',                #开右门开关动作次数
     'U82':'close_Rdoor_switch_action',               #关右门开关动作次数
     'U83':'alert_pedal_switch_action',               #警惕脚踏开关动作次数
     'U84':'alert_manual_switch_action',              #警惕手动开关动作次数
     'U85':'fault_reset_switch_action',               #故障复位开关动作次数
     'U86':'emer_reset_switch_action',                #紧急复位开关动作次数
     'U87':'control_tow_action',                      #司控器牵引位继电器动作次数
     'U88':'control_brake_action',                    #司控器制动位继电器动作次数
     'U89':'control_constant_speed_action',           #司控器恒速位继电器动作次数
     'U90':'brake_command1_relay_action',             #制动控制指令1继电器动作次数
     'U91':'brake_command2_relay_action',             #制动控制指令2继电器动作次数
     'U92':'brake_command3_relay_action',             #制动控制指令3继电器动作次数
     'U93':'control_relay_action',                    #主控端继电器动作次数
     'U94':'tow_relay_action',                        #牵引使能继电器动作次数
     'U95':'5km_speed_relay_action',                  #5km/h速度继电器动作次数
     'U96':'em_brake_EB_relay_action',                #紧急制动EB环路继电器动作次数
     'U97':'tmotor1_contactor1_action',               #牵引电机风机1接触器1动作次数
     'U98':'tmotor1_contactor2_action',               #牵引电机风机1接触器2动作次数
     'U99':'tmotor2_contactor1_action'                #牵引电机风机2接触器1动作次数
}

cols_E44 = {
    'train_type_code':0,                        # 车型
    'train_no':1,                               # 列号
    'coach_id':2,                               # 车号
    'time_value':3,                             # 时间
    'LRdoor_switch':4,                          # 左右门开关触点断不开
    'release_Ldoor_switch':5,                   # 释放左门开关触点断不开
    'apply_parking_switch':6,                   # 施加停放开关触点断不开
    'relief_parking_switch':7,                  # 缓解停放开关触点断不开
    'fault_reset_switch':8,                     # 故障复位开关触点断不开
    'em_reset_switch':9,                        # 紧急复位开关触点断不开
    'broken_VCB_switch':10,                     # 断VCB开关触点断不开
    'VCB_switch':11,                            # 合VCB开关触点断不开
    'bow_switch':12,                            # 升弓开关触点断不开
    'drop_bow_switch':13,                       # 降弓开关触点断不开
    'open_Rdoor_switch':14,                     # 开右门开关触点断不开
    'close_Rdoor_switch':15,                    # 关右门开关触点断不开
    'release_Rdoor_switch':16,                  # 释放右门开关触点断不开
    'open_Ldoor_switch':17,                     # 开左门开关触点断不开
    'asnormal_door11_close':18,                 # 车门1-1关闭时间异常
    'asnormal_door12_close':19,                 # 车门1-2关闭时间异常
    'asnormal_door21_close':20,                 # 车门2-1关闭时间异常
    'asnormal_door22_close':21,                 # 车门2-2关闭时间异常
    'relief_parking_brake':22,                  # 缓解停放制动继电器动作不一致
    'relief_hold_brake':23,                     # 缓解保持制动继电器动作不一致
    'em_brake_EB':24,                           # 紧急制动EB安全环路继电器动作不一致
    'em_brake_UB':25,                           # 紧急制动UB安全环路继电器动作不一致
    'TBM_input':26,                             # TBM投入异常预警
    'brake_force':27,                           # 制动力异常预警
    'air_brake_force':28,                       # 空气制动力异常预警
    'ins_dec_train_level':29,                   # 列车级减速度不足预警
    'cyc_ATP_brake7':30,                        # 单车ATP七级制动继电器动作不一致
    'cyc_brake_command1':31,                    # 单车制动指令1继电器动作不一致
    'cyc_brake_command2':32,                    # 单车制动指令2继电器动作不一致
    'cyc_brake_command3':33,                    # 单车制动指令3继电器动作不一致
    'cyc_control_brake_status':34,              # 单车司控器制动状态继电器动作不一致
    'em_traction_command':35,                   # 紧急牵引指令继电器动作不一致
    'em_mode_relay':36,                         # 紧急模式继电器动作不一致
    'apply_parking_brake':37,                   # 施加停放制动继电器动作不一致
    'BJ_battery1_charged_floating_mode':38,     # 蓄电池1均充转浮充异常报警
    'YJ_battery1_charged_floating_mode':39,     # 蓄电池1均充转浮充异常预警
    'battery1_overcharged':40,                  # 蓄电池1充电超时
    'battery1_charging_vlotage':41,             # 蓄电池1充电电压异常
    'BJ_battery2_charged_floating_mode':42,     # 蓄电池2均充转浮充异常报警
    'YJ_battery2_charged_floating_mode':43,     # 蓄电池2均充转浮充异常预警
    'battery2_overcharged':44,                  # 蓄电池2充电超时
    'battery2_charging_vlotage':45,             # 蓄电池2充电电压异常
    'tcf_VCB_status':46,                        # 牵引变流器反馈VCB状态信号不一致
    'tcf_forward_signal':47,                    # 牵引变流器反馈前向信号不一致
    'tcf_backward_signal':48,                   # 牵引变流器反馈后向信号不一致
    'tcf_em_traction_mode':49,                  # 牵引变流器反馈紧急牵引模式信号不一致
    'tcf_em_traction_command':50,               # 牵引变流器反馈紧急牵引指令信号不一致
    'tcf_em_braking_UB':51,                     # 牵引变流器反馈紧急制动UB信号不一致
    'tcf_disconnect_VCB_command':52,            # 牵引变流器反馈断VCB指令信号不一致
    'tcf_broken_rescue_power_status':53,        # 牵引变流器反馈回送救援发电状态信号不一致
    'tcm1_current_consist':54,                  # 一架牵引变流器电机电流一致性异常
    'tcm2_current_consist':55,                  # 二架牵引变流器电机电流一致性异常
    'tcm1_actual_force_consist':56,             # 一架牵引电机实际力一致性异常
    'tcm2_actual_force_consist':57,             # 二架牵引电机实际力一致性异常
    'current_trans_high_voltage':58,            # 电流互感器高压线缆接地
    'CT2_high_voltage':59,                      # 本单元CT2监测点后的连接线路高压线缆接地
    'tt_primary_consist':60,                    # 牵引变压器原边电流一致性异常预警
    'tc1_q4_current_consist':61,                # 一架牵引变流器四象限电流一致性异常
    'tc2_q4_current_consist':62,                # 二架牵引变流器四象限电流一致性异常
    'motor1_force_consist':63,                  # 一架电机设定力一致性异常预警
    'motor2_force_consist':64,                  # 二架电机设定力一致性异常预警
    'inter_DC_voltage_consist':65,              # 中间直流电压一致性异常预警
    'elec_brake_force_too_lage':66,             # 电制动力反馈偏差过大预警
    'max_elec_brake_capacity_too_large':67,     # 最大电制动能力值偏差过大预警
    'WSP1_motor_speed_too_large':68,            # 1轴WSP轴速度与电机速度偏差过大预警
    'WSP2_motor_speed_too_large':69,            # 2轴WSP轴速度与电机速度偏差过大预警
    'WSP3_motor_speed_too_large':70,            # 3轴WSP轴速度与电机速度偏差过大预警
    'WSP4_motor_speed_too_large':71,            # 4轴WSP轴速度与电机速度偏差过大预警
    'motor1_speed_too_large':72,                # 1轴电机速度与整车速度偏差过大预警
    'motor2_speed_too_large':73,                # 2轴电机速度与整车速度偏差过大预警
    'motor3_speed_too_large':74,                # 3轴电机速度与整车速度偏差过大预警
    'motor4_speed_too_large':75,                # 4轴电机速度与整车速度偏差过大预警
    'WPS1_vehicle_speed_too_large':76,          # 1轴WSP轴速度与整车速度偏差过大预警
    'WPS2_vehicle_speed_too_large':77,          # 2轴WSP轴速度与整车速度偏差过大预警
    'WPS3_vehicle_speed_too_large':78,          # 3轴WSP轴速度与整车速度偏差过大预警
    'WPS4_vehicle_speed_too_large':79,          # 4轴WSP轴速度与整车速度偏差过大预警
    'speed':80,                                 # 速度
    'motor_fan2_contactor2_action': 81,         # 牵引电机风机2接触器2动作次数
    'converter_fan2_contactor1_action': 82,     # 牵引变流器风机2接触器1动作次数
    'converter_fan2_contactor2_action': 83,     # 牵引变流器风机2接触器2动作次数
    'transformer_fan1_contactor1_action': 84,   # 牵引变压器风机1接触器1动作次数
    'transformer_fan1_contactor2_action': 85,   # 牵引变压器风机1接触器2动作次数
    'transformer_fan2_contactor1_action': 86,   # 牵引变压器风机2接触器1动作次数
    'transformer_fan2_contactor2_action': 87,   # 牵引变压器风机2接触器2动作次数
    'breaker_action': 88,                       # 主断路器动作次数
    'air_compressor_contactor_action': 89,      # 主空压机接触器动作次数
    'ground_protection_switch_action': 90,      # 接地保护开关动作次数
    'high_voltage_switch_action': 91,           # 车高压隔离开关动作次数
    'ATP_power_switch_action': 92,              # ATP电源空开动作次数
    'bow_switch_action': 93,                    # 升弓开关动作次数
    'release_Rdoor_switch_action': 94,          # 释放右门开关动作次数
    'open_Rdoor_switch_action': 95,             # 开右门开关动作次数
    'close_Rdoor_switch_action': 96,            # 关右门开关动作次数
    'alert_pedal_switch_action': 97,            # 警惕脚踏开关动作次数
    'alert_manual_switch_action': 98,           # 警惕手动开关动作次数
    'fault_reset_switch_action': 99,            # 故障复位开关动作次数
    'emer_reset_switch_action': 100,            # 紧急复位开关动作次数
    'control_tow_action': 101,                  # 司控器牵引位继电器动作次数
    'control_brake_action': 102,                # 司控器制动位继电器动作次数
    'control_constant_speed_action': 103,       # 司控器恒速位继电器动作次数
    'brake_command1_relay_action': 104,         # 制动控制指令1继电器动作次数
    'brake_command2_relay_action': 105,         # 制动控制指令2继电器动作次数
    'brake_command3_relay_action': 106,         # 制动控制指令3继电器动作次数
    'control_relay_action': 107,                # 主控端继电器动作次数
    'tow_relay_action': 108,                    # 牵引使能继电器动作次数
    '5km_speed_relay_action': 109,              # 5km/h速度继电器动作次数
    'em_brake_EB_relay_action': 110,            # 紧急制动EB环路继电器动作次数
    'tmotor1_contactor1_action': 111,           # 牵引电机风机1接触器1动作次数
    'tmotor1_contactor2_action': 112,           # 牵引电机风机1接触器2动作次数
    'tmotor2_contactor1_action': 113            # 牵引电机风机2接触器1动作次数
}

# 16车长编标准动车组车型列表
long_group_sdrext =['E44']

trans_p_sdr={
 'train_type_code':'train_type_code',
 'train_no':'train_no',
 'coach_id':'coach_id',
 'time_value':'time_value',
 'aircond_cool_degree': 'G05',
 'dc_ctv_off': 'B118',
 'bc_press': 'B01',
 'coach_avg_degree': 'G07',
 'blower_stop': 'C260',
 'mid_dc_volt': 'C35',
 'air_brake_status': 'B107',
 'trainsite_valid': 'A02',
 'emergency_brake_ebr': 'B132',
 'ci_order': 'A47',
 'brake_lvl': 'A21',
 'vcb_status': 'C01',
 'emergency_brake_jtr': 'B131',
 'union_status': 'A07',
 'compressor_off': 'B16',
 'emergency_brake': 'B06',
 'mr_press': 'B04',
 'aircond_half_status': 'G03',
 'restop_graph_v': 'B03',
 'cool_device_temp': 'C258',
 'm_off': 'C13',
 'speed': 'A09',
 'const_speed': 'A14',
 'battery_valid': 'D00',
 'restop_back': 'C07',
 'air_compressor_status': 'B61',
 'pantograph_status': 'C09',
 'brake_pos': 'A13',
 'spin_flag': 'C08',
 'eb_cmd': 'B121',
 'mrc_egcs1': 'S19',
 'mcr_coach': 'A16',
 'direction': 'A17',
 'emergency_short': 'B17',
 'main_trans_abnormal': 'C262',
 'out_sensor_degree': 'H25',
 'aircond_valid': 'G00',
 'trainsite_id': 'A06',
 't16_k': 'C99',
 'water_out_degree': 'C43',
 'aux_trans_degree': 'C50',
 'volt3_value': 'C04',
 'aircond_work_status': 'G02',
 'vcb_off': 'C12',
 'park_brake_order': 'S48',
 'speed_valid': 'A03',
 'save_conver_status': 'S23',
 'dc_low_voltage2': 'C257',
 'bpsave_valid': 'H28',
 'ci_valid': 'C00',
 'dc_trans_degree': 'C67',
 'union_train_no': 'A41',
 'save_58_tatus': 'A19',
 'machine_temp_high': 'C259',
 'rotor_rate': 'C06',
 'em_brake_switch': 'B07',
 'water_in_degree': 'C42',
 'as_press': 'B02',
 'axle_slide': 'B119',
 'axle_slide_c3': 'K05',
 'axle_slide_c2': 'K04',
 'axle_slide_c1': 'K03',
 'axle_slide_c4': 'K06',
 'traction_1_grade': 'A82',
 'tbr_signal': 'H71',
 'bpsave_device_status': 'B122',
 'dirs_signal': 'S21',
 'glide_flag': 'B14',
 'control_order': 'A45',
 'dc_mm_back': 'C05',
 'stop_valid': 'B00',
 'brake_info': 'A15',
 'network_volt': 'C16',
 'battery_v': 'D01',
 'alert_brake': 'B08',
 'ovth_malfunction': 'C261'
}

trans_cols_sdr={0: 'train_type_code',
 1: 'train_no',
 2: 'coach_id',
 3: 'time_value',
 4: 'trainsite_valid',
 5: 'trainsite_id',
 6: 'union_status',
 7: 'speed_valid',
 8: 'speed',
 9: 'brake_pos',
 10: 'mcr_coach',
 11: 'vcb_status',
 12: 'volt3_value',
 13: 't16_k',
 14: 'emergency_short',
 15: 'restop_graph_v',
 16: 'mr_press',
 17: 'restop_back',
 18: 'stop_valid',
 19: 'bc_press',
 20: 'ci_valid',
 21: 'rotor_rate',
 22: 'dc_mm_back',
 23: 'compressor_off',
 24: 'battery_valid',
 25: 'battery_v',
 26: 'm_off',
 27: 'as_press',
 28: 'const_speed',
 29: 'glide_flag',
 30: 'spin_flag',
 31: 'water_in_degree',
 32: 'vcb_off',
 33: 'save_58_tatus',
 34: 'aircond_valid',
 35: 'aircond_work_status',
 36: 'aircond_half_status',
 37: 'out_sensor_degree',
 38: 'aircond_cool_degree',
 39: 'bpsave_device_status',
 40: 'bpsave_valid',
 41: 'save_conver_status',
 42: 'coach_avg_degree',
 43: 'mrc_egcs1',
 44: 'dirs_signal',
 45: 'tbr_signal',
 46: 'control_order',
 47: 'air_compressor_status',
 48: 'air_brake_status',
 49: 'brake_lvl',
 50: 'eb_cmd',
 51: 'union_train_no',
 52: 'dc_ctv_off',
 53: 'dc_trans_degree',
 54: 'axle_slide',
 55: 'axle_slide_c1',
 56: 'axle_slide_c2',
 57: 'axle_slide_c3',
 58: 'axle_slide_c4',
 59: 'mid_dc_volt',
 60: 'network_volt',
 61: 'emergency_brake',
 62: 'emergency_brake_ebr',
 63: 'emergency_brake_jtr',
 64: 'ci_order',
 65: 'pantograph_status',
 66: 'brake_info',
 67: 'alert_brake',
 68: 'dc_low_voltage2',
 69: 'cool_device_temp',
 70: 'machine_temp_high',
 71: 'blower_stop',
 72: 'ovth_malfunction',
 73: 'main_trans_abnormal',
 74: 'traction_1_grade',
 75: 'direction',
 76: 'water_out_degree',
 77: 'aux_trans_degree',
 78: 'em_brake_switch',
 79: 'park_brake_order'
}


trans_p_sdrext={
 'train_type_code': 'train_type_code',
 'train_no': 'train_no',
 'coach_id': 'coach_id',
 'time_value': 'time_value',
 'pantograph_up': 'C126',
 'bc_press': 'B01',
 'eb_cmd': 'B121',
 'charger1_103_contactor': 'D24',
 'brake_isolation_switch': 'Q25',
 'brake_lvl': 'A21',
 'bpsave_devicontrl_air': 'A74',
 'rescue_devic_contrl':'A74',
 'compressor_off': 'B16',
 'shelf2_engi_elec': 'C37',
 'mr_press': 'B04',
 'charger2_103_contactor': 'D26',
 'vcb_down': 'C14',
 'dc_mm_back': 'C05',
 'm_off': 'C13',
 'speed': 'A09',
 'car_door_3_closed': 'H69',
 'air_tartemp': 'G11',
 'brake_pos': 'A13',
 'mcr_coach': 'A16',
 'dc_cvt_water_in2_press': 'C48',
 'compressor1_fault': 'G64',
 'car_door_4_closed': 'H70',
 'charger_out_v2': 'D09',
 'change_signal': 'G60',
 'volt3_value': 'C04',
 'vcb_off': 'C12',
 'park_brake_order': 'B123',
 'power_percent': 'C61',
 'refrigerator_1_low_press': 'G20',
 'high_off_switch_down': 'P22',
 'Auxiliary_inverter_cut': 'C256',
 'speed_valid': 'A03',
 'aux_out_current': 'C40',
 'charger1_115contactor': 'D23',
 'dc_cvt_water_out2_degree': 'C45',
 'charger2_115contactor': 'D25',
 'dc_cvt_water_in1_press': 'C46',
 'tcu_restop_brake': 'C58',
 'rescue_switch_mid_pos': 'Q16',
 'dc_ctv_off': 'B118',
 'keep_brake_app_status': 'B315',
 'ee1_pull_value': 'C24',
 'refrigerator_2_high_press': 'G19',
 'vcb_status': 'C01',
 'bcu_request_brake': 'C57',
 'air_compressor_status': 'B61',
 'rescue_switch_rescue': 'Q30',
 'assist_volt': 'C39',
 'air_brake_isolation': 'B175',
 'short1_contactor': 'C158',
 'dc_cvt_water_in1_degree': 'C42',
 'water_in_degree':'C42',
 'passing_neutral_section': 'Y75',
 'pull_idle_count': 'K02',
 'shelf1_quad4_elec': 'C33',
 'emergency_mode_switch1': 'Q12',
 'emergency_mode_switch2': 'Q13',
 'refrigerator_1_high_press': 'G18',
 'relieve_keep_brake_switch': 'Q38',
 'bp_press': 'B34',
 'rotor_rate': 'C06',
 'shelf_off': 'C69',
 'stop_brake_status': 'B106',
 'axle_slide_c3': 'K05',
 'axle_slide_c2': 'K04',
 'axle_slide_c1': 'K03',
 'axle_slide_c4': 'K06',
 'ac380_bus_contactor': 'C160',
 'main_compressor_off': 'B116',
 'shelf1_speed': 'C54',
 'ee2_pull_value': 'C25',
 'tract_conver_status': 'C68',
 'short2_contactor': 'C159',
 'mid_dc_volt': 'C35',
 'urgent_pull_relay': 'C77',
 'ATP_7_brake': 'B157',
 'charger_out_v1': 'D08',
 'dc_cvt_water_in2_degree': 'C44',
 'shelf2_engi_power': 'C53',
 'new_wind_dettemp': 'A28',
 'restop_graph_v': 'B03',
 'restop_back': 'C07',
 'pantograph_status': 'C09',
 'spin_flag': 'C08',
 'high_off_switch_status': 'P21',
 'tmotor_fan_contactor1': 'C155',
 'tmotor_fan_contactor2': 'C156',
 'set_power_percent': 'C62',
 'aux_trans_degree': 'C50',
 'direction': 'A17',
 'compressor2_fault': 'G62',
 'forward1': 'A63',
 'forward2': 'A64',
 't16_k': 'C99',
 'dc_trans_degree': 'C67',
 'union_train_no': 'A41',
 'car_door_2_closed': 'H68',
 'battery_group_qty1': 'D04',
 'battery_group_qty2': 'D05',
 'trainsite_valid': 'A02',
 'axle_slide': 'B119',
 'glide_flag':'B119',
 'shelf1_engi_power': 'C52',
 'charger_up_v2': 'D17',
 'dc_trans_oil_stop_relay': 'C87',
 'charger_up_v1': 'D16',
 'compressor1_run': 'G65',
 'aux_con_status': 'C70',
 'phase3_bus_relay2': 'C161',
 'union_status': 'A07',
 'ATP_power_switch': 'Q32',
 'phase3_bus_relay1': 'C154',
 'vcb_off_monitor': 'C125',
 'network_volt': 'C16',
 'battery_v': 'D01',
 'train_weight': 'A39',
 'car_door_1_closed': 'H67',
 'Charger_1_status': 'C75',
 'refrigerator_2_low_press': 'G21',
 'shelf1_engi_elec': 'C36',
 'air_brake_status': 'B107',
 'dc_cvt_off': 'B117',
 'tour_dettemp': 'G16',
 'charger_off1': 'D21',
 'charger_off2': 'D22',
 'const_speed': 'A14',
 'air_dettemp': 'G12',
 'dc_cvt_water_out1_degree': 'C43',
 'tow_slide_count': 'K08',
 'dc_trans_elec': 'C32',
 'compressor2_run': 'G63',
 'emergency_short': 'B17',
 'trainsite_id': 'A06',
 'tcp_keep_model': 'C21',
 'shelf2_quad4_elec': 'C34',
 'as_press': 'B02',
 'battery_valid': 'D00',
 'e_brake': 'B102',
 'shelf2_speed': 'C55',
 'ci_valid': 'C00',
 'Charger_2_status': 'C76',
 'dc_trans_air_open': 'C85',
 'rescue_switch_rescued': 'Q27',
 'park_brake_status': 'B46',
 'park_brake_press': 'B105',
 'back2': 'A72',
 'coach_weight': 'B120',
 'back1': 'A71',
 'stop_valid': 'B00',
 'aux_out_voltage': 'C38',
 'direction_zero': 'A62',
 'air_rev_modal': 'G02',
 'UB_air_brake_effective': 'B314',
 'tconverter_air_degree': 'C51',
 'charge_battery_group1_tem1':'D106',
 'charge_battery_group1_tem2':'D105',
 'charge_battery_group2_tem1':'D119',
 'charge_battery_group2_tem2':'D118'
}


trans_cols_sdrext={
 0: 'train_type_code',
 1: 'train_no',
 2: 'coach_id',
 3: 'time_value',
 4: 'trainsite_valid',
 5: 'trainsite_id',
 6: 'union_status',
 7: 'speed_valid',
 8: 'speed',
 9: 'brake_pos',
 10: 'mcr_coach',
 11: 'vcb_status',
 12: 'volt3_value',
 13: 't16_k',
 14: 'emergency_short',
 15: 'restop_graph_v',
 16: 'mr_press',
 17: 'restop_back',
 18: 'stop_valid',
 19: 'bc_press',
 20: 'ci_valid',
 21: 'rotor_rate',
 22: 'dc_mm_back',
 23: 'compressor_off',
 24: 'battery_valid',
 25: 'battery_v',
 26: 'm_off',
 27: 'as_press',
 28: 'const_speed',
 29: 'glide_flag',
 30: 'spin_flag',
 31: 'water_in_degree',
 32: 'vcb_off',
 33: 'direction',
 34: 'union_train_no',
 35: 'vcb_down',
 36: 'main_compressor_off',
 37: 'assist_volt',
 38: 'brake_lvl',
 39: 'e_brake',
 40: 'dc_cvt_off',
 41: 'dc_ctv_off',
 42: 'tcp_keep_model',
 43: 'axle_slide',
 44: 'tcu_restop_brake',
 45: 'bcu_request_brake',
 46: 'ee1_pull_value',
 47: 'ee2_pull_value',
 48: 'air_brake_status',
 49: 'axle_slide_c1',
 50: 'axle_slide_c2',
 51: 'axle_slide_c3',
 52: 'axle_slide_c4',
 53: 'train_weight',
 54: 'coach_weight',
 55: 'eb_cmd',
 56: 'shelf1_speed',
 57: 'shelf2_speed',
 58: 'dc_trans_elec',
 59: 'shelf1_quad4_elec',
 60: 'shelf2_quad4_elec',
 61: 'shelf1_engi_elec',
 62: 'shelf2_engi_elec',
 63: 'shelf1_engi_power',
 64: 'shelf2_engi_power',
 65: 'shelf_off',
 66: 'power_percent',
 67: 'urgent_pull_relay',
 68: 'pull_idle_count',
 69: 'network_volt',
 70: 'mid_dc_volt',
 71: 'dc_trans_degree',
 72: 'dc_cvt_water_in1_degree',
 73: 'dc_cvt_water_out1_degree',
 74: 'dc_cvt_water_in2_degree',
 75: 'dc_cvt_water_out2_degree',
 76: 'refrigerator_1_high_press',
 77: 'refrigerator_2_high_press',
 78: 'refrigerator_1_low_press',
 79: 'refrigerator_2_low_press',
 80: 'new_wind_dettemp',
 81: 'tour_dettemp',
 82: 'car_door_1_closed',
 83: 'car_door_2_closed',
 84: 'car_door_3_closed',
 85: 'car_door_4_closed',
 86: 'air_dettemp',
 87: 'air_tartemp',
 88: 'charger_out_v1',
 89: 'charger_out_v2',
 90: 'battery_group_qty1',
 91: 'battery_group_qty2',
 92: 'charger_up_v1',
 93: 'charger_up_v2',
 94: 'charger_off1',
 95: 'charger_off2',
 96: 'park_brake_order',
 97: 'park_brake_status',
 98: 'park_brake_press',
 99: 'air_compressor_status',
 100: 'pantograph_status',
 101: 'dc_trans_air_open',
 102: 'dc_trans_oil_stop_relay',
 103: 'high_off_switch_status',
 104: 'vcb_off_monitor',
 105: 'pantograph_up',
 106: 'high_off_switch_down',
 107: 'aux_out_voltage',
 108: 'aux_out_current',
 109: 'dc_cvt_water_in1_press',
 110: 'dc_cvt_water_in2_press',
 111: 'air_rev_modal',
 112: 'stop_brake_status',
 113: 'phase3_bus_relay1',
 114: 'tmotor_fan_contactor1',
 115: 'tmotor_fan_contactor2',
 116: 'ATP_power_switch',
 117: 'ATP_7_brake',
 118: 'direction_zero',
 119: 'forward1',
 120: 'forward2',
 121: 'rescue_switch_mid_pos',
 122: 'rescue_devic_contrl',
 123: 'rescue_switch_rescued',
 124: 'rescue_switch_rescue',
 125: 'aux_con_status',
 126: 'aux_trans_degree',
 127: 'tconverter_air_degree',
 128: 'bp_press',
 129: 'back1',
 130: 'back2',
 131: 'short1_contactor',
 132: 'short2_contactor',
 133: 'ac380_bus_contactor',
 134: 'tract_conver_status',
 135: 'phase3_bus_relay2',
 136: 'bpsave_devicontrl_air',
 137: 'charger1_115contactor',
 138: 'charger2_115contactor',
 139: 'tow_slide_count',
 140: 'set_power_percent',
 141: 'passing_neutral_section',
 142: 'emergency_mode_switch1',
 143: 'emergency_mode_switch2',
 144: 'Auxiliary_inverter_cut',
 145: 'air_brake_isolation',
 146: 'charger1_103_contactor',
 147: 'charger2_103_contactor',
 148: 'compressor1_run',
 149: 'compressor1_fault',
 150: 'compressor2_run',
 151: 'compressor2_fault',
 152: 'Charger_1_status',
 153: 'Charger_2_status',
 154: 'UB_air_brake_effective',
 155: 'brake_isolation_switch',
 156: 'keep_brake_app_status',
 157: 'relieve_keep_brake_switch',
 158: 'change_signal',
 159: 'charge_battery_group1_tem1',
 160: 'charge_battery_group1_tem2',
 161: 'charge_battery_group2_tem1',
 162: 'charge_battery_group2_tem2'
}
