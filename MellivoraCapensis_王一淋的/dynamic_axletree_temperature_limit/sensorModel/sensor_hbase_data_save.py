# -*- coding: utf-8 -*-
import sys, datetime, time, happybase
import param_conf
import traceback
from getMysqlData import MySQLData

reload(sys)
sys.setdefaultencoding('utf8')

hb_host = param_conf.hbase_thriftserver

sensorTypeList = [['1', '1位轴端', 'L'], ['2', '2位轴端', 'R'], ['1', '3位轴端', 'L'], ['2', '4位轴端', 'R'], ['1', '5位轴端', 'L'],
                  ['2', '6位轴端', 'R'], ['1', '7位轴端', 'L'], ['2', '8位轴端', 'R'], ['3', '1轴小齿轮箱电机侧', 'R'],
                  ['3', '2轴小齿轮箱电机侧', 'R'], ['3', '3轴小齿轮箱电机侧', 'R'], ['3', '4轴小齿轮箱电机侧', 'R'], ['4', '1轴小齿轮箱车轮侧', 'L'],
                  ['4', '2轴小齿轮箱车轮侧', 'L'], ['4', '3轴小齿轮箱车轮侧', 'L'], ['4', '4轴小齿轮箱车轮侧', 'L'], ['7', '1轴电机定子', 'L'],
                  ['8', '1轴电机传动端', 'L'], ['9', '1轴电机非传动端', 'R'], ['7', '2轴电机定子', 'L'], ['8', '2轴电机传动端', 'L'],
                  ['9', '2轴电机非传动端', 'R'], ['7', '3轴电机定子', 'L'], ['8', '3轴电机传动端', 'L'], ['9', '3轴电机非传动端', 'R'],
                  ['7', '4轴电机定子', 'L'], ['8', '4轴电机传动端', 'L'], ['9', '4轴电机非传动端', 'R'], ['5', '1轴大齿轮箱电机侧', 'R'],
                  ['5', '2轴大齿轮箱电机侧', 'R'], ['5', '3轴大齿轮箱电机侧', 'R'], ['5', '4轴大齿轮箱电机侧', 'R'], ['6', '1轴大齿轮箱车轮侧', 'L'],
                  ['6', '2轴大齿轮箱车轮侧', 'L'], ['6', '3轴大齿轮箱车轮侧', 'L'], ['6', '4轴大齿轮箱车轮侧', 'L']]
sensorStatusList = {'2': ['报警', 'A1'], '3': ['预警', 'A0']}

tableRs = 'dm_train_zw_warn_rs'
tableLogTest = 'dm_train_zw_warn_rs_log'
tableLog = 'dm_train_zw_warn_rs_log_test'

columnFamily = 'f_data'


# 声明传感器编码
sensorCodeList = 'None'
try:  # 连接MySQL数据库获取数据
    db = MySQLData()
    sensorCodeList = db.querySensorPos()
except Exception, e:
    print '%s 从MySQL数据库获取数据出现异常：%s' % (datetime.datetime.now(), e)


class SaveData:

    def __init__(self):
        try:  # 连接HBase数据库
            self.connection = happybase.Connection(hb_host, autoconnect=False)  # 建立连接
            self.connection.open()  # 打开连接
            self.hTableRs = self.connection.table(tableRs)  # 读取表数据
            self.hTablelog = self.connection.table(tableLog)  # 读取表数据
        except Exception as e:
            print '%s HBase连接异常, 请确认输入正确的Thrift Server主机名或IP:  %s' % (datetime.datetime.now(), e)

    """功能说明：保存数据至文件或数据库"""
    def saveToFile(self, fileName, d):
        """功能：将数据写入文件
           参数：fileName-写入文件路径和文件名
                 d-内容(列表格式)
        """
        # 定义文件名
        dataFileName = fileName + '.put'
        contents = '\t'.join([str(item) for item in d])

        # 写入文件
        f = file(dataFileName, "a+")
        f.write(contents + '\n')
        f.close()


    """将数据导出至HBase"""

    def onTrainWarning(self, d):
        """处理车载预警/报警"""
        global sensorTypeList, sensorStatusList, table, tableLog

        # 初始化数据
        trainTypeCode, trainNo, coachId, fTime, speed = d[36], d[37], d[38], d[39], d[40]
        timeValue = fTime.replace('-', '').replace(':', '').replace(' ', '')

        for i in xrange(0, 36):
            sensorStatus = d[i]
            if sensorStatus in ('2', '3'):
                sensorType, sensorKind, sensorName, sensorSide = (str(i + 1)).zfill(2), sensorTypeList[i][0], \
                                                                 sensorTypeList[i][1], sensorTypeList[i][2]  # 获取传感器位置
                sensorStatusCd, sensorStatusType = sensorStatusList[sensorStatus][0], sensorStatusList[sensorStatus][1]
                glId = trainTypeCode + '_' + coachId + '_' + sensorType
                sensorCode = sensorCodeList[glId]

                for x in xrange(1, 3):
                    flag = 0
                    if x == 1 and int(speed) > 0:  # 如果速度大于0，则生成table信息
                        tableName = table
                        rowKey = trainNo
                        flag = 1

                    if x == 2:  # 则生成tableLog信息
                        tableName = tableLog
                        rowKey = timeValue + '_' + trainNo
                        flag = 1

                    if flag == 1:
                        # 输出信息(基础信息)
                        basicTrainType = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAINTYPE","' + trainTypeCode + '"'
                        basicCreateTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':CREATE_TIME","' + fTime + '"'
                        basicOpTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':OP_TIME","' + timeValue + '001"'
                        basicSpeedState = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':SPEED_STATE","1"'
                        basicTrainId = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAIN_ID","' + trainNo + '"'
                        basicTrainSpeed = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAIN_SPEED","' + speed + '"'
                        basicWarnTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':WARN_TIME","' + timeValue + '"'
                        basicDealStatus = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':ZW_DEAL_STATUS","1"'

                        # 输出信息(定制信息)
                        customizedType = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + 'YJ_TYPE_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + sensorStatusType + '"'
                        customizedSpeed = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + 'YJ_SPEED_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + speed + '"'
                        customizedStatus = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + 'YJ_STATUS_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","1"'
                        customizedParamValue = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + 'YJ_PARAMVALUE_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","车载' + sensorStatusCd + '"'
                        customizedTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + 'YJ_TIME_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + timeValue + '"'
                        customizedName = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + 'YJ_NAME_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","车载' + sensorStatusCd + ':' + sensorName + ':出现' + sensorStatusCd + '"'

                        print basicTrainType
                        print basicCreateTime
                        print basicOpTime
                        print basicSpeedState
                        print basicTrainId
                        print basicTrainSpeed
                        print basicWarnTime
                        print basicDealStatus
                        print customizedType
                        print customizedSpeed
                        print customizedStatus
                        print customizedParamValue
                        print customizedTime
                        print customizedName

    def onGroundWarning(self, d):
        """处理地面预判/预警/报警"""
        global sensorTypeList, columnFamily

        # 系统当前时间  UNIX时间戳日期
        timeNow = datetime.datetime.now()
        ftime = timeNow.strftime("%Y%m%d%H%M%S")
        timeStamp = str(time.mktime(time.strptime(ftime, "%Y%m%d%H%M%S")))

        # 初始化数据
        trainTypeCode, trainNo, coachId, speed, trainsiteId, sensorKind, sensorName = d[0], d[1], d[2], d[3], d[4], d[5], d[6]
        sensorType, sensorValue, sameSideValue, preJudgeDesc, preWarningDesc = d[7], d[8], d[9], d[10], d[11]
        warningDesc, modelType, fTime = d[12], d[13], d[14]
        timeValue = fTime.replace('-', '').replace(':', '').replace(' ', '')

        sensorSide = sensorTypeList[int(sensorType) - 1][2]  # 传感器侧面(R/L)

        glId = trainTypeCode + '_' + coachId + '_' + sensorType
        glId = glId.replace('\xef\xbb\xbf', '')
        sensorCode = sensorCodeList.get(glId)  # 传感器位置编码
        sensorCode = trainTypeCode + '000000' if str(sensorCode) == 'None' else sensorCode

        for x in xrange(1, 2):
            if x == 1:
                columnFamily = 'f1'
                tableName = 'test_jsj'
                rowKey = timeValue + '_' + trainNo

            # 输出信息(基础信息)
            # basicTrainType = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAINTYPE","' + trainTypeCode + '"'
            # basicCreateTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':CREATE_TIME","' + fTime + '"'
            # basicOpTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':OP_TIME","' + timeValue + '001"'
            # basicSpeedState = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':SPEED_STATE","1"'
            # basicTrainId = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAIN_ID","' + trainNo + '"'
            # basicTrainSpeed = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAIN_SPEED","' + speed + '"'
            # basicWarnTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':WARN_TIME","' + timeValue + '"'
            # basicDealStatus = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':ZW_DEAL_STATUS","1"'

            basicTrainType = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAINTYPE","' + trainTypeCode + '",' + timeStamp
            basicCreateTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':CREATE_TIME","' + fTime + '",' + timeStamp
            basicOpTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':OP_TIME","' + timeValue + '001",' + timeStamp
            basicSpeedState = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':SPEED_STATE","1",' + timeStamp
            basicTrainId = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAIN_ID","' + trainNo + '",' + timeStamp
            basicTrainSpeed = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':TRAIN_SPEED","' + speed + '",' + timeStamp
            basicWarnTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':WARN_TIME","' + timeValue + '",' + timeStamp
            basicDealStatus = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':ZW_DEAL_STATUS","1",' + timeStamp

            # 定义事件编码
            eventTypeId, eventTypeName = '', ''
            # 定义传输信息
            sensorStatusType, warningCode, warningType, description = '', '', '', ''
            # 输出信息(定制信息)
            if modelType == '融合报警':  # 融合报警
                if sensorKind in ('7', '8', '9'):
                    eventTypeId, eventTypeName = 'm006', '牵引电机融合报警'
                else:
                    eventTypeId, eventTypeName = 'm007', '齿轮箱、轴端融合报警'
                sensorStatusType, warningCode, warningType, description = 'B1', 'BJ', '融合报警', warningDesc
            elif modelType == '地面PHM轴温模型':   # 标动模型
                if sensorKind in('7','8','9'):
                    if preJudgeDesc.split(':')[0] == 'YP':
                        eventTypeId,eventTypeName = 'm018','标动牵引电机轴温地面模型'
                        sensorStatusType,warningCode,warningType,description = 'K0','YP', '地面轴温预判',preJudgeDesc.split(':')[1]
                    elif preWarningDesc.split(':')[0] == 'YJ':
                          eventTypeIdeventTypeName = 'm017','标动牵引电机轴温地面模型'
                          sensorStatusType,warningCode,warningType,description = 'K3','YJ', '地面轴温预警',preWarningDesc.split(':')[1]
                    elif warningDesc.split(':')[0] == 'BJ':
                        eventTypeId,eventTypeName = 'm004','牵引电机温度报警'
                        sensorStatusType,warningCode,warningType,description = 'K1','BJ', '地面轴温报警',warningDesc.split(':')[1]
                elif sensorKind in('1','2','3','4','5','6'):
                    if preJudgeDesc.split(':')[0] == 'YP':
                        eventTypeId,eventTypeName='m020','标动齿轮箱、轴端轴温地面模型'
                        sensorStatusType,warningCode,warningType,description = 'K0','YP', '地面轴温预判',preJudgeDesc.split(':')[1]
                    elif preWarningDesc.split(':')[0] == 'YJ':
                        eventTypeId,eventTypeName = 'm019','标动齿轮箱、轴端轴温地面模型'
                        sensorStatusType,warningCode,warningType,description = 'K3','YJ', '地面轴温预警',preWarningDesc.split(':')[1]
                    elif warningDesc.split(':')[0] == 'BJ':
                        eventTypeId,eventTypeName = 'm005','齿轮箱、轴箱温度报警'
                        sensorStatusType,warningCode,warningType,description = 'K1','BJ', '地面轴温报警',warningDesc.split(':')[1]
                    sameSideFlag = 0
            elif modelType in ('滤波模型_业务部门', '滤波模型_林森'):  # 地面模型
                if sensorKind in ('7', '8', '9'):
                    eventTypeId, eventTypeName = 'm002', '牵引电机温度预判'
                    sensorStatusType, warningCode, warningType, description = 'K0', 'YP', '地面轴温预判', \
                                                                              preJudgeDesc.split(':')[1]
                elif sensorKind in ('1', '2', '3', '4', '5', '6'):
                    eventTypeId, eventTypeName = 'm003', '齿轮箱、轴箱温度预判'
                    sensorStatusType, warningCode, warningType, description = 'K0', 'YP', '地面轴温预判', \
                                                                              preJudgeDesc.split(':')[1]

            elif modelType in ('滤波模型_业务部门2'):  # 温度阈值规则2
                modelType = '滤波模型_业务部门'
                if sensorKind in ('1', '2', '3', '4', '5', '6'):
                    eventTypeId, eventTypeName = 'm023', '齿轮箱、轴箱温度预判'
                    sensorStatusType, warningCode, warningType, description = 'K0', 'YP', '地面轴温预判', \
                                                                              preJudgeDesc.split(':')[1]

            sameSideFlag = 1  # 如果预报警规则是温升速率, 则不显示同侧温度

            if '温升速率' in preJudgeDesc:  # 如果预警规则是温升速率, 则不显示同侧温度
                sameSideFlag = 0

            customizedTrainsiteId = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_TRAINNO_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + trainsiteId + '",' + timeStamp  # 车次
            customizedType = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_TYPE_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + sensorStatusType + '",' + timeStamp
            customizedSpeed = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_SPEED_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + speed + '",' + timeStamp
            customizedStatus = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_STATUS_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","1",' + timeStamp
            customizedParamValue = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_PARAMVALUE_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + sensorValue + '",' + timeStamp
            customizedTime = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_TIME_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","' + timeValue + '"'
            customizedName = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_NAME_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '"," ' + eventTypeName + '[' + eventTypeId + ']' + ':' + sensorName + '",' + timeStamp
            customizedCount = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_COUNT_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","1",' + timeStamp

            if sameSideFlag == 1:
                modelView = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_MODELVIEW_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","[' + modelType + ']' + warningType + ':' + sensorName + ':' + description + ':同侧温度:' + sameSideValue + '",' + timeStamp
            else:
                modelView = 'put "' + tableName + '","' + rowKey + '","' + columnFamily + ':' + sensorStatusType + warningCode + '_MODELVIEW_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode + '","[' + modelType + ']' + warningType + ':' + sensorName + ':' + description + '",' + timeStamp

            print basicTrainType
            print basicCreateTime
            print basicOpTime
            print basicSpeedState
            print basicTrainId
            print basicTrainSpeed
            print basicWarnTime
            print basicDealStatus
            print customizedTrainsiteId
            print customizedType
            print customizedSpeed
            print customizedStatus
            print customizedParamValue
            print customizedTime
            print customizedName
            print customizedCount
            print modelView

    def updateData(self, d):
        '''功能: 更新数据至HBase数据库
           参数：dType为中文事件类型
                 dataSet为报警数据集
                 warningType: YP-预判, YJ-预警, BJ-报警
        '''
        d=[str(x) for x in d]
        # 初始化数据
        trainTypeCode, trainNo, coachId, speed, trainsiteId, sensorKind, sensorName = d[0], d[1], d[2], d[3], d[4], d[
            5], d[6]
        sensorType, sensorValue, sameSideValue, preJudgeDesc, preWarningDesc = d[7], d[8], d[9], d[10], d[11]
        warningDesc, modelType, fTime = d[12], d[13], d[14]

        timeValue = fTime.replace('-', '').replace(':', '').replace(' ', '')
        sensorSide = sensorTypeList[int(sensorType) - 1][2]  # 传感器侧面(R/L)

        glId = trainTypeCode + '_' + coachId + '_' + sensorType
        glId = glId.replace('\xef\xbb\xbf', '')
        sensorCode = sensorCodeList.get(glId)  # 传感器位置编码
        sensorCode = trainTypeCode + '-000000' if str(sensorCode) == 'None' else sensorCode

        # 定义事件编码
        eventTypeId, eventTypeName = '', ''
        # 定义传输信息
        sensorStatusType, warningCode, warningType, description = None, None, '', ''
        # 输出信息(定制信息)
        if modelType == '融合报警':  # 融合报警
            if sensorKind in ('7', '8', '9'):
                eventTypeId, eventTypeName = 'm006', '牵引电机融合报警'
            else:
                eventTypeId, eventTypeName = 'm007', '齿轮箱、轴端融合报警'
            sensorStatusType, warningCode, warningType, description = 'B1', 'BJ', '融合报警', warningDesc
        elif modelType == '地面PHM轴温模型':  # 标动模型
            if sensorKind in ('7', '8', '9'):
                eventTypeId, eventTypeName = 'm018', '标动牵引电机轴温地面模型'
                sensorStatusType, warningCode, warningType, description = 'K0', 'YP', '地面轴温预判', \
                                                                          preJudgeDesc.split(':')[1]
            elif sensorKind in ('1', '2', '3', '4', '5', '6'):
                eventTypeId, eventTypeName = 'm020', '标动齿轮箱、轴端轴温地面模型'
                sensorStatusType, warningCode, warningType, description = 'K0', 'YP', '地面轴温预判', \
                                                                          preJudgeDesc.split(':')[1]
        elif modelType in  ('阈值和温差预判','温升预判_网开滤波','温升预判_电开滤波'):
            sensorStatusType, warningCode, warningType, description = 'K0', 'YP', '地面轴温预判', \
                                                                      preJudgeDesc.split(':')[1]
            if sensorKind in ('7', '8', '9'):
                eventTypeId, eventTypeName = 'm002', '牵引电机温度预判'
            else:
                eventTypeId, eventTypeName = 'm003', '齿轮箱、轴箱温度预判'


        sameSideFlag = 1  # 如果预报警规则是温升速率, 则不显示同侧温度

        if '温升速率' in preJudgeDesc:  # 如果预警规则是温升速率, 则不显示同侧温度
            sameSideFlag = 0

        if sameSideFlag == 1:
            ViewValue = '[' + modelType + ']' + warningType + ':' + sensorName + ':' + description + ':同侧温度:' + sameSideValue
        else:
            ViewValue = '[' + modelType + ']' + warningType + ':' + sensorName + ':' + description

        if sensorStatusType is not None and warningCode is not None:
            basicTrainType = columnFamily + ':TRAINTYPE","' + trainTypeCode  # 车型
            basicCreateTime = columnFamily + ':CREATE_TIME'      # 时间
            basicOpTime = columnFamily + ':OP_TIME'
            basicSpeedState = columnFamily + ':SPEED_STATE'
            basicTrainId = columnFamily + ':TRAIN_ID'
            basicTrainSpeed = columnFamily + ':TRAIN_SPEED'
            basicWarnTime = columnFamily + ':WARN_TIME'
            basicDealStatus = columnFamily + ':ZW_DEAL_STATUS'
            customizedTrainsiteId = columnFamily + ':' + sensorStatusType + warningCode + '_TRAINNO_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedType = columnFamily + ':' + sensorStatusType + warningCode + '_TYPE_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedSpeed = columnFamily + ':' + sensorStatusType + warningCode + '_SPEED_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedStatus =  columnFamily + ':' + sensorStatusType + warningCode + '_STATUS_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedParamValue = columnFamily + ':' + sensorStatusType + warningCode + '_PARAMVALUE_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedTime = columnFamily + ':' + sensorStatusType + warningCode + '_TIME_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedName = columnFamily + ':' + sensorStatusType + warningCode + '_NAME_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            customizedCount = columnFamily + ':' + sensorStatusType + warningCode + '_COUNT_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode
            modelView = columnFamily + ':' + sensorStatusType + warningCode + '_MODELVIEW_' + coachId + '_' + sensorKind + '_' + sensorSide + '_' + sensorCode

            sqlStr = {
                basicTrainType: trainTypeCode,
                basicCreateTime: fTime,
                basicOpTime: timeValue + '001',
                basicSpeedState: '1',
                basicTrainId: trainNo,
                basicTrainSpeed: speed,
                basicWarnTime: timeValue,
                basicDealStatus: '1',
                customizedTrainsiteId: trainsiteId,
                customizedType: sensorStatusType,
                customizedSpeed:speed,
                customizedStatus:'1',
                customizedParamValue:sensorValue,
                customizedTime:timeValue,
                customizedName:eventTypeName + '[' + eventTypeId + ']' + ':' + sensorName,
                customizedCount:'1',
                modelView:ViewValue
            }

            try:
                # 插入在线表
                rowKeyRs = trainNo
                self.hTableRs.put(rowKeyRs, sqlStr)

                # 插入日志表
                rowKeyLog = timeValue + '_' + trainNo
                self.hTablelog.put(rowKeyLog, sqlStr)

                # 插入测试表
                #table = connection.table('test_gy')
                #rowKey = timeValue + '_' + trainNo
                #table.put(rowKey, sqlStr)

                # print sqlStr
                # print '%s' % ('\t'.join([str(item) for item in d]))
            except Exception as e:
                print '%s 数据库插入数据时出现异常:  %s' % (datetime.datetime.now(), e)

        else:
            print 'ERROR: sensorStatusType 和 warningCode为None!!'
            print 'modelType:',str(modelType),'sensorKind:',str(sensorKind)
            sys.exit(1)

    def connectionClose(self):
        try:
            if self.connection:
                self.connection.close()
        except Exception as e:
            print '%s hbase连接关闭异常: %s' % (datetime.datetime.now(), e)

if __name__ == '__main__':
    try:
        if len(sys.argv) == 1:
            # 调用计算
            th = SaveData()
            print th,'\n',th.connection,'\n',th.hTableRs,'\n',th.hTablelog
            th=SaveData()
            print th

        #     for line in sys.stdin:
        #         d = line.strip().split('\x01')
        #         doType = d[-1]
        #         if doType == 'onTrainWarning':  # 车载预警/报警
        #             th.onTrainWarning(d)
        #         elif doType == 'onGroundWarning':  # 地面预警/报警
        #             th.updateData(d)
        # # 程序调试
        # if len(sys.argv) == 2 and sys.argv[1].lower() == 'debug':
        #     th = TransformToHBase()
        #     for line in sys.stdin:
        #         d = line.strip().split('\x01')
        #         doType = d[-1]
        #         if doType == 'onGroundWarning':  # 地面预警/报警
        #             th.updateData(d)
    except Exception as e:
        exec_info = traceback.format_exc()
        print exec_info
