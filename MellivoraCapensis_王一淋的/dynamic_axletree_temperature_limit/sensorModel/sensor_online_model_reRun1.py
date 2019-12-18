#!/usr/bin/env python
# vim: set fileencoding=utf-8
# 功能：轴温模型计算
# author: 201810 by lzb

import sys, os, time, datetime, subprocess
from sensor_hbase_data_save import SaveData
from getMysqlData import MySQLData
import traceback
from kazoo.client import KazooClient
import hdfs
import param_conf

# model_type_code m1=融合模型,m2=地面模型, m3=滤波模型，m4=阈值模型标准动车车型
trainBlackList = ['0000']
trainsiteIdBlackList = param_conf.trainsiteBlackList

autoNum = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
sensorTypeList = [['1', '1位轴端'], ['2', '2位轴端'], ['1', '3位轴端'], ['2', '4位轴端'],
                  ['1', '5位轴端'], ['2', '6位轴端'], ['1', '7位轴端'], ['2', '8位轴端'],
                  ['3', '1轴小齿轮箱电机侧'], ['3', '2轴小齿轮箱电机侧'], ['3', '3轴小齿轮箱电机侧'], ['3', '4轴小齿轮箱电机侧'],
                  ['4', '1轴小齿轮箱车轮侧'], ['4', '2轴小齿轮箱车轮侧'], ['4', '3轴小齿轮箱车轮侧'], ['4', '4轴小齿轮箱车轮侧'],
                  ['7', '1轴电机定子'], ['8', '1轴电机传动端'], ['9', '1轴电机非传动端'], ['7', '2轴电机定子'],
                  ['8', '2轴电机传动端'], ['9', '2轴电机非传动端'], ['7', '3轴电机定子'], ['8', '3轴电机传动端'],
                  ['9', '3轴电机非传动端'], ['7', '4轴电机定子'], ['8', '4轴电机传动端'], ['9', '4轴电机非传动端'],
                  ['5', '1轴大齿轮箱电机侧'], ['5', '2轴大齿轮箱电机侧'], ['5', '3轴大齿轮箱电机侧'], ['5', '4轴大齿轮箱电机侧'],
                  ['6', '1轴大齿轮箱车轮侧'], ['6', '2轴大齿轮箱车轮侧'], ['6', '3轴大齿轮箱车轮侧'], ['6', '4轴大齿轮箱车轮侧']]
speedList = {}
speedList1 = {}
CompPreInfo = {}  # 记录前一信息
dataList = []
sId = ''

# 轴箱控制策略
sensorA = [['E15', 'E16', 'E21', 'E27', 'E30', 'E31', 'E35', 'E11', 'E12'],
           ['E19', 'E26', 'E28', 'E36', 'E37', 'E39', 'E41', 'E46', 'E6A', 'E6B', 'E6A3', 'E6A4', 'E6F', 'E6F4',
            'GSYE11', 'GSYE12', 'GSYE15', 'GSYE16', 'GSYE17', 'GSYE18', 'GSYE19', 'GSYE21']]

# 齿轮箱控制策略
sensorB = [
    ['E15', 'E16', 'E21', 'E27', 'E30', 'E31', 'E35', 'E11', 'E12', 'E19', 'E26', 'E28', 'E36', 'E37', 'E39', 'E41',
     'E46'],
    ['E6A', 'E6B', 'E6A3', 'E6A4', 'E6F', 'E6F4', 'GSYE11', 'GSYE12', 'GSYE15', 'GSYE16', 'GSYE17', 'GSYE18', 'GSYE19',
     'GSYE21']]

# 电机传动端控制策略
sensorC = [
    ['E15', 'E16', 'E21', 'E27', 'E30', 'E31', 'E35', 'E11', 'E12', 'E19', 'E26', 'E28', 'E36', 'E37', 'E39', 'E41',
     'E46', 'E6A', 'E6B', 'E6A3', 'E6A4', 'E6F', 'E6F4', 'GSYE11', 'GSYE12', 'GSYE15', 'GSYE16', 'GSYE17', 'GSYE18',
     'GSYE19', 'GSYE21']]

# 电机定子控制策略
sensorD = [['E15', 'E16', 'E21', 'E27', 'E30', 'E31', 'E35', 'E19', 'E26', 'E28', 'E36', 'E37', 'E39', 'E41', 'E46'],
           ['E11', 'E12', 'E6A', 'E6B', 'E6A3', 'E6A4', 'E6F', 'E6F4', 'GSYE11', 'GSYE12', 'GSYE15', 'GSYE16', 'GSYE17',
            'GSYE18', 'GSYE19',
            'GSYE21']]

# 电机非传动端控制策略
sensorE = [
    ['E15', 'E16', 'E21', 'E27', 'E30', 'E31', 'E35', 'E11', 'E12', 'E19', 'E26', 'E28', 'E36', 'E37', 'E39', 'E41',
     'E46', 'E6A', 'E6B', 'E6A3', 'E6A4', 'E6F', 'E6F4', 'GSYE11', 'GSYE12', 'GSYE15', 'GSYE16', 'GSYE17', 'GSYE18',
     'GSYE19', 'GSYE21']]

dataSet1 = dict()
dataSet2 = dict()
dataSet3 = dict()


class onlineCompModel:
    """融合模型计算"""

    # 参数设置
    modelParam = {  # 参数1: 传感器类别, speedParam: 速率, diffParam: 温差, downLimit: 温度下限, timeLongParam: 时长
        "1": {"speedParam": 1, "diffParam": 30, "downLimit": 80, "timeLongParam": 10},  # 轴端参数
        "2": {"speedParam": 1, "diffParam": 30, "downLimit": 80, "timeLongParam": 10},  # 轴端参数
        "3": {"speedParam": 1, "diffParam": 25, "downLimit": 30, "timeLongParam": 10},  # 小齿轮
        "4": {"speedParam": 1, "diffParam": 25, "downLimit": 30, "timeLongParam": 10},  # 小齿轮
        "5": {"speedParam": 1, "diffParam": 25, "downLimit": 30, "timeLongParam": 10},  # 大齿轮
        "6": {"speedParam": 1, "diffParam": 25, "downLimit": 30, "timeLongParam": 10},  # 大齿轮
        "7": {"speedParam": 1, "diffParam": 32, "downLimit": 60, "timeLongParam": 15},  # 电机定子
        "8": {"speedParam": 1, "diffParam": 32, "downLimit": 30, "timeLongParam": 10},  # 传动端
        "9": {"speedParam": 1, "diffParam": 11, "downLimit": 30, "timeLongParam": 10},  # 非传动端
    }

    modelParam1 = {  # 参数1: 传感器类别, speedParam: 速率, diffParam: 温差, downLimit: 温度下限, timeLongParam: 时长
        "1": {"speedParam": 5, "diffParam": 30, "downLimit": 80, "timeLongParam": 3},  # 轴端参数
        "2": {"speedParam": 5, "diffParam": 30, "downLimit": 80, "timeLongParam": 3},  # 轴端参数
        "3": {"speedParam": 5, "diffParam": 25, "downLimit": 30, "timeLongParam": 3},  # 小齿轮
        "4": {"speedParam": 5, "diffParam": 25, "downLimit": 30, "timeLongParam": 3},  # 小齿轮
        "5": {"speedParam": 5, "diffParam": 25, "downLimit": 30, "timeLongParam": 3},  # 大齿轮
        "6": {"speedParam": 5, "diffParam": 25, "downLimit": 30, "timeLongParam": 3},  # 大齿轮
        "7": {"speedParam": 5, "diffParam": 25, "downLimit": 60, "timeLongParam": 3},  # 电机定子
        "8": {"speedParam": 5, "diffParam": 32, "downLimit": 30, "timeLongParam": 3},  # 传动端
        "9": {"speedParam": 5, "diffParam": 8, "downLimit": 30, "timeLongParam": 3},  # 非传动端
    }

    model_type_code = 'm1'
    warmDict1 = {}
    warmDict2 = {}

    # 定义时间参数(单位: 分钟)
    diffTimeParam = 2

    def calSensorDiff(self, calVal, valList):
        # 计算同侧轴温温差
        l = []

        # 获取列表
        for i in xrange(0, len(valList)):
            if valList[i] != 0:
                l.append(valList[i])

        # 计算平均值
        if len(l) > 0:
            meanVal = sum(l) / len(l)

            # 计算同侧轴温温差
            diffVal = calVal - meanVal
        else:
            diffVal = 0
            meanVal = 0

            # 返回温差值和均值
        return diffVal, meanVal

    def calSensorSpeed(self, info):
        global speedList

        if len(info) > 0:
            sensorType = info.keys()[0]  # 传感器类型
            seq = info[sensorType][0]  # 当前计算值序列
            sensorValue = info[sensorType][1]  # 当前温度值

            # 判断值是否已经在字典表里存在
            if seq > -1:
                existsKey = [item for item in speedList if item == sensorType]
            else:
                existsKey = []

            # 将变量初始化为0
            speedVal = 0  # 非突升速率值
            timeLong = 0  # 时长

            # 如果存在, 则计算速率
            if len(existsKey) > 0 and speedList[sensorType][0] != -1:
                timeLong = seq - speedList[sensorType][0]  # 时长(单位: 分钟)
                if timeLong > 0:
                    speedVal = round((sensorValue - speedList[sensorType][1]) / timeLong)  # 速率(温度/min)
                    # print sensorValue, speedList[sensorType][1], timeLong
                else:
                    speedVal = 0
                    # print '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (sensorType, seq, speedList[sensorType][0], sensorValue, speedList[sensorType][1], speedVal, timeLong)

            # 剔除连续速率小于1的数据
            # if speedVal < 1:
            #     timeLong = 0
            #     speedVal = 0
            #     existsKey = []
            # autoNum[int(sensorType.split('_')[1])-1] = 0

            # 如果字典列表不存在相应项, 则添加
            if len(existsKey) == 0 or (len(existsKey) > 0 and speedList[sensorType][0] == -1):
                speedList[sensorType] = [seq, sensorValue]

        # 计算速率
        return timeLong, speedVal

    def calSensorSpeed1(self, info):
        global speedList1
        global autoNum

        if len(info) > 0:
            sensorType = info.keys()[0]  # 传感器类型
            seq = info[sensorType][0]  # 当前计算值序列
            sensorValue = info[sensorType][1]  # 当前温度值

            # 判断值是否已经在字典表里存在
            if seq > -1:
                existsKey = [item for item in speedList1 if item == sensorType]
            else:
                existsKey = []
            # 将变量初始化为0
            speedVal = 0  # 非突升速率值
            timeLong = 0  # 时长

            # 如果存在, 则计算速率
            if len(existsKey) > 0 and speedList1[sensorType][0] != -1:
                timeLong = seq - speedList1[sensorType][0]  # 时长(单位: 分钟)
                if timeLong > 0:
                    speedVal = round((sensorValue - speedList1[sensorType][1]) / timeLong)  # 速率(温度/min)
                    # print sensorValue, speedList1[sensorType][1], timeLong
                else:
                    speedVal = 0
                    # print '%s\t%s\t%s\t%s\t%s\t%s\t%s' % (sensorType, seq, speedList[sensorType][0], sensorValue, speedList[sensorType][1], speedVal, timeLong)

            # 剔除连续速率小于5的数据
            # if speedVal < 5:
            #     timeLong = 0
            #     speedVal = 0
            #     existsKey = []
            # autoNum[int(sensorType.split('_')[1])-1] = 0

            # 如果字典列表不存在相应项, 则添加
            if len(existsKey) == 0 or (len(existsKey) > 0 and speedList1[sensorType][0] == -1):
                speedList1[sensorType] = [seq, sensorValue]

        # 计算速率
        return timeLong, speedVal

    def calTimeFlag(self, diffTime, oldVal, newVal, seq, downLimit, diffVal, diffParam):
        global autoNum

        # 计算温度连续上升标志
        flag = -1
        # if newVal >= oldVal and (newVal >= downLimit and diffVal >= diffParam):
        # speedVal = (newVal - oldVal)/diffTime
        # if newVal >= oldVal and (newVal >= downLimit) and speedVal > 1:
        if newVal >= oldVal and (newVal >= downLimit):
            autoNum[seq] = autoNum[seq] + diffTime  # 增加时长
            flag = autoNum[seq]  # 获取当前时长
        else:
            autoNum[seq] = 0

        return flag

    def calDiffTime(self, newTime, oldTime):
        # 计算时间差
        diffTime = 999999999999
        try:
            diffTime = int((newTime.split(" ")[1].split(":")[0])) * 60 + int(
                (newTime.split(" ")[1].split(":")[1])) - int((oldTime.split(" ")[1].split(":")[0])) * 60 - int(
                (oldTime.split(" ")[1].split(":")[1]))
            # if len(newTime.split(" ")[1].split(":")) == 2:
            #     diffTime = int((newTime.split(" ")[1].split(":")[0]))*60 + int((newTime.split(" ")[1].split(":")[1])) - int((oldTime.split(" ")[1].split(":")[0]))*60 - int((oldTime.split(" ")[1].split(":")[1]))
            # elif len(newTime.split(" ")[1].split(":")) == 3:
            #     diffTime = int((newTime.split(" ")[1].split(":")[0]))*60*60 + int((newTime.split(" ")[1].split(":")[1]))*60 + int((newTime.split(" ")[1].split(":")[2])) - int((oldTime.split(" ")[1].split(":")[0]))*60*60 - int((oldTime.split(" ")[1].split(":")[1]))*60 - int((oldTime.split(" ")[1].split(":")[2]))

        finally:
            return diffTime

    def addFirstInfo(self, dataId, data, fTime):
        # 添加第一条信息至变量
        global CompPreInfo
        CompPreInfo.clear()
        sensorList = []
        for ii in xrange(0, 36):
            sensorList.append([int(data[ii]), fTime])  # 将36个传感器的温度值加载至变量
        CompPreInfo[dataId] = sensorList
        # print CompPreInfo

    def getMinData(self, data):
        # 清洗数据, 处理为每分钟数据
        global dataList, sId, db

        ###trainMroStatus = db.queryTrainMroStatus()

        dList = []
        d = data
        # d[39] = (data[39])[0:16]    # 修改时间

        # 初始化数据列表
        if len(dataList) == 0:
            dataList.append(d)

        # 获取时间值
        t1 = dataList[0][39]
        t2 = d[39]

        # 计算时间差(单位：分钟)
        diffTime = int(t2.split(' ')[1].split(':')[0]) * 60 + int(t2.split(' ')[1].split(':')[1]) - int(
            t1.split(' ')[1].split(':')[0]) * 60 - int(t1.split(' ')[1].split(':')[1])

        # 获取基本信息
        trainTypeCode, trainNo, coachId, fTime = data[36], data[37], data[38], data[39]
        dataId = trainTypeCode + '_' + trainNo + '_' + coachId  # 确定数据记录ID

        # 20170515根据融合模型趋势跟踪需求增加输出信息
        self.out_degree, self.trainsite_id, self.brake_pos = d[41], d[42], d[43]
        ## 20181127 过滤调试车次 与 高级修
        if self.trainsite_id not in trainsiteIdBlackList:
            if trainNo in trainMroStatus:
                if trainMroStatus[trainNo][2] not in ('三级修', '四级修', '五级修'):
                    # 如果跨分钟数据, 则开始调用计算
                    if diffTime >= self.diffTimeParam and sId == dataId:
                        for kk in xrange(0, 36):
                            l = []

                            # 处理每列数据
                            for jj in xrange(0, len(dataList)):
                                if dataList[jj][kk] != '999' and dataList[jj][kk] != 'NULL':
                                    l.append(int(dataList[jj][kk]))

                            # 去除每分钟内的最高值与最低值(处理波动数据)
                            if len(l) > 0:
                                l.sort()
                                if max(l) - sum(l) / len(l) >= 5:  # 去除最大值
                                    l = l[:-1]

                                if abs(min(l) - sum(l) / len(l)) >= 5:  # 去除最小值
                                    l = l[1:]

                                meanVal = sum(l) / len(l)
                            else:
                                meanVal = 999  # 取均值

                            dList.append(meanVal)

                        # 获取基础数据信息
                        for hh in xrange(36, 41):
                            dList.append(dataList[-1][hh])

                        # 重新记录当前数据
                        dataList = []
                        dataList.append(d)

                        # 调用计算方法开始计算
                        outputTime = data[39]
                        self.calMain(dList, outputTime)
                    else:
                        if sId != dataId or diffTime < 0:
                            sId = dataId
                            dataList = []
                        dataList.append(d)  # 如果当前数据在每分钟内, 则添加数据至该变量

    def calMain(self, data, outputTime):
        global sensorTypeList

        # # 获取基本数据信息
        trainTypeCode, trainNo, coachId, fTime, trainSpeed = data[36], data[37], data[38], data[39], data[40]

        if len(coachId) < 2:
            coachId = '0' + coachId

        dataId = trainTypeCode + '_' + trainNo + '_' + coachId  # 确定数据记录ID

        # 判断车号类型
        if trainTypeCode in ('E02', 'E05', 'E09', 'E11', 'E27') and coachId in ('01', '08'):
            coachType = 'T'
        elif trainTypeCode in ('E01', 'E01A', 'E19', 'E22', 'E25', 'E26', 'E28') and coachId in (
                '01', '04', '05', '08'):
            coachType = 'T'
        elif trainTypeCode in ('E03', 'E06', 'E36', 'E37', 'E41') and coachId in (
                '01', '04', '05', '08', '09', '12', '13', '16'):
            coachType = 'T'
        elif trainTypeCode in ('E12', 'E35') and coachId in ('01', '16'):
            coachType = 'T'
        else:
            coachType = 'M'

        # 判断值是否已经在字典表里存在
        existsKey = [item for item in CompPreInfo if item == dataId]

        if len(existsKey) > 0:

            # 如果是拖车, 则只处理轴端
            num = 10 if coachType == 'M' else 3

            # 定义函数获取同侧温度列表数据
            ssv = lambda sensorGroup, sensorType: [int(data[ii - 1]) for ii in sensorGroup if
                                                   sensorType != ii and data[ii - 1] != '999']
            ssvAll = lambda sensorGroup, sensorType: [int(data[ii - 1]) for ii in sensorGroup if data[ii - 1] != '999']

            for i in xrange(1, num):
                if i == 1:
                    sensorGroup = [1, 3, 5, 7]  # 轴端

                if i == 2:
                    sensorGroup = [2, 4, 6, 8]  # 轴端

                if i == 3:
                    sensorGroup = [9, 10, 11, 12]  # 小齿轮

                if i == 4:
                    sensorGroup = [13, 14, 15, 16]  # 小齿轮

                # 模型参数设置
                if i == 5:
                    sensorGroup = [29, 30, 31, 32]  # 大齿轮

                if i == 6:
                    sensorGroup = [33, 34, 35, 36]  # 大齿轮

                if i == 7:
                    sensorGroup = [17, 20, 23, 26]  # 电机定子

                if i == 8:
                    sensorGroup = [18, 21, 24, 27]  # 传动端

                if i == 9:
                    sensorGroup = [19, 22, 25, 28]  # 非传动端

                for x in xrange(0, 4):
                    # 初始化同侧温度列表值
                    sensorValList = []

                    # 传感器类型
                    sensorType = sensorGroup[x]
                    sensorKind, sensorKindName = sensorTypeList[sensorType - 1][0], sensorTypeList[sensorType - 1][1]

                    # 模型参数设置
                    speedParam = self.modelParam[sensorKind]["speedParam"]  # 速率参数设置
                    diffParam = self.modelParam[sensorKind]["diffParam"]  # 温差副度参数设置
                    downLimit = self.modelParam[sensorKind]["downLimit"]  # 温度下限设置
                    timeLongParam = self.modelParam[sensorKind]["timeLongParam"]  # 时长

                    # 轴温突变参数
                    speedParam1 = self.modelParam1[sensorKind]["speedParam"]  # 速率参数设置
                    diffParam1 = self.modelParam1[sensorKind]["diffParam"]  # 温差副度参数设置
                    downLimit1 = self.modelParam1[sensorKind]["downLimit"]  # 温度下限设置
                    timeLongParam1 = self.modelParam1[sensorKind]["timeLongParam"]  # 时长

                    # 当前温度值
                    newSensorVal = int(data[sensorType - 1])

                    # 获取同侧温度列表
                    # for j in xrange(0,len(sensorGroup)):
                    # if int(data[sensorGroup[j]-1]) != 999:   #温度值不为0
                    #     sensorValList.append(int(data[sensorGroup[j]-1]))
                    sensorValList, sensorListOutput = ssv(sensorGroup, sensorType), ssvAll(sensorGroup, sensorType)

                    # print sensorValList, sensorListOutput

                    diffVal, meanVal = self.calSensorDiff(newSensorVal, [max(sensorValList)])
                    # print diffVal, meanVal
                    # if i == 8 and sensorType == 24:
                    #     print fTime, newSensorVal, sensorValList, diffVal
                    # 获取上个时间温度值和时间
                    oldSensorVal, oldTime = CompPreInfo[dataId][sensorType - 1][0], CompPreInfo[dataId][sensorType - 1][
                        1]
                    # print CompPreInfo
                    # 如果获取的值为999(原始数据为空), 则更新数据
                    if oldSensorVal == 999 or newSensorVal == 999:
                        CompPreInfo[dataId][sensorType - 1][0], CompPreInfo[dataId][sensorType - 1][
                            1] = newSensorVal, fTime
                    else:
                        # 计算时间差
                        diffTime = self.calDiffTime(fTime, oldTime)

                        # 计算时间连续性标志
                        flag = self.calTimeFlag(diffTime, oldSensorVal, newSensorVal, sensorType - 1, downLimit,
                                                diffVal, diffParam)

                        # 计算速率
                        info = {'S_' + str(sensorType): [flag, newSensorVal]}
                        timeLong, speedVal = self.calSensorSpeed(info)
                        timeLong1, speedVal1 = self.calSensorSpeed1(info)

                        # 时间标志不连续或者当前温度大于上一温度值, 则更新数据
                        if flag == -1 or newSensorVal > oldSensorVal:
                            CompPreInfo[dataId][sensorType - 1][0], CompPreInfo[dataId][sensorType - 1][
                                1] = newSensorVal, fTime
                        # print timeLong, speedVal
                        # >>>>>>>>>>>start 模型计算>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                        # 根据模型参数进行计算(温度>=下限值, 速率>=速率参数值, 温差>=温差参数值, 持续时长>=时长参数)
                        # if i == 8 and sensorType == 24:
                        #     print fTime, newSensorVal, diffVal, timeLong, speedVal
                        if newSensorVal >= downLimit and diffVal >= diffParam and timeLong >= timeLongParam and speedVal >= speedParam:
                            # 输出结果(车型，列号, 车号, 传感器类别, 传感器类别名称, 传感器类型, 轴温温度值, 前一分钟温度值, 同侧温度值, 同侧均值, 温差, 速率, 模型类型, 告警信息, 时间)
                            warningDesc = '温升速率>=' + str(speedParam) + ';同侧温差>=' + str(diffParam) + ';时长>=' + str(
                                timeLongParam) + '分钟'
                            sameSideVal, modelType, preJudgeDesc, preWarningDesc = ','.join(
                                [str(item) for item in sensorListOutput]), '融合报警', '', ''

                            warn_Key = trainNo + '_' + coachId + '_' + self.model_type_code + '_' + str(sensorType)
                            flag1 = 0

                            # 如果以前没有报过警 则进行报警输出
                            if outputTime not in self.warmDict1.keys():
                                self.warmDict1[outputTime] = {warn_Key: ''}
                                flag1 = 1
                            elif warn_Key not in self.warmDict1[outputTime].keys():
                                self.warmDict1[outputTime][warn_Key] = ''
                                flag1 = 1
                            else:
                                flag1 = 0

                            if flag1 == 1:
                                # print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                                #     trainTypeCode, trainNo, coachId, trainSpeed, self.out_degree, self.trainsite_id,
                                #     self.brake_pos, sensorKind, sensorKindName, sensorType, newSensorVal, oldSensorVal,
                                #     sameSideVal, meanVal, diffVal, speedVal, timeLong, preJudgeDesc, preWarningDesc,
                                #     warningDesc, modelType, outputTime)

                                tmpList = [trainTypeCode, trainNo, coachId, trainSpeed, self.out_degree,
                                           self.trainsite_id,
                                           self.brake_pos, sensorKind, '融合模型:' + sensorKindName, sensorType,
                                           newSensorVal,
                                           oldSensorVal,
                                           sameSideVal, meanVal, diffVal, speedVal, timeLong, preJudgeDesc,
                                           preWarningDesc,
                                           warningDesc, modelType, outputTime]

                                # sd.saveToFile(, tmpList)
                                kazooModel.saveDataToHive(tmpList)

                                ## 事件中心
                                wType = 'sensorComp1'
                                keyId = trainTypeCode + '_' + trainNo + '_' + coachId
                                keyms = keyId + '_' + wType + '_' + sensorKind + '_' + str(sensorType)
                                toDay = outputTime[0:4] + outputTime[5:7] + outputTime[8:10]
                                dt_today = datetime.datetime.strptime(toDay, '%Y%m%d')
                                dayList5 = [(dt_today - datetime.timedelta(days=i)).strftime('%Y%m%d') for i in
                                            range(5)]
                                dayList4 = [(dt_today - datetime.timedelta(days=i)).strftime('%Y%m%d') for i in
                                            range(4)]
                                condition = (trainTypeCode, trainNo,
                                             coachId, '', '', '', wType,
                                             '', outputTime, toDay, sensorKind + '_' + str(sensorType))
                                db.updateOnlineMonitor(condition)  # 将数据插入Mysql数据库表中
                                mysqldata = db.getmysqldata('sensorComp1')
                                eventFlag = 0
                                if keyms in mysqldata:
                                    # 电机传动端 连续3天 或 5天出现3次
                                    if sensorKind == '8':
                                        if dayList5[1] in mysqldata[keyms] and dayList5[2] in mysqldata[keyms]:
                                            warningDesc = '[连续3天满足温差预判条件]' + warningDesc
                                            eventFlag = 1
                                        else:
                                            tmp = [day for day in dayList5 if
                                                   day in mysqldata[keyms]]
                                            if len(tmp) >= 3:
                                                warningDesc = '[5天内3次满足温差预判条件]' + warningDesc
                                                eventFlag = 1
                                    else:  # 其他位置 连续2天 或 4天出现2次
                                        if dayList4[1] in mysqldata[keyms]:
                                            warningDesc = '[连续2天满足温差预判条件]' + warningDesc
                                            eventFlag = 1
                                        else:
                                            tmp = [day for day in dayList4 if
                                                   day in mysqldata[keyms]]
                                            if len(tmp) >= 2:
                                                warningDesc = '[4天内2次满足温差预判条件]' + warningDesc
                                                eventFlag = 1
                                # 满足进事件中心
                                if eventFlag == 1:
                                    tmpList2 = [trainTypeCode, trainNo, coachId, trainSpeed, self.trainsite_id,
                                                sensorKind,
                                                '融合模型:' + sensorKindName, sensorType, newSensorVal, sameSideVal,
                                                preJudgeDesc,
                                                preWarningDesc, warningDesc, modelType, outputTime]

                                    # sd.updateData(tmpList2)

                                # 控制 self.warmDict1的内存
                                if len(self.warmDict1.keys()) > 200:
                                    timeList = sorted(self.warmDict1.keys())
                                    for i in range(len(timeList) - 20):
                                        self.warmDict1.pop(timeList[i])

                        # 轴温突变
                        # {"speedParam": 4, "diffParam": 10, "downLimit": 30, "timeLongParam": 5}
                        if newSensorVal >= downLimit1 and diffVal >= diffParam1 and timeLong1 >= timeLongParam1 and speedVal1 >= speedParam1:
                            # 输出结果(列号, 车号, 传感器类别, 传感器类别名称, 传感器类型, 轴温值, 前一分钟温度值, 同侧温度平均值, 温差值, 速率, 标志, 时间)
                            warningDesc = '温升速率>=5;同侧温差>=' + str(diffParam) + ';时长>=3分钟'
                            sameSideVal, modelType, preJudgeDesc, preWarningDesc = ','.join(
                                [str(item) for item in sensorListOutput]), '融合报警', '', ''

                            warn_Key = trainNo + '_' + coachId + '_' + self.model_type_code + '_' + str(sensorType)

                            flag2 = 0

                            # 如果以前没有报过警 则进行报警输出
                            if outputTime not in self.warmDict2.keys():
                                self.warmDict2[outputTime] = {warn_Key: ''}
                                flag2 = 1
                            elif warn_Key not in self.warmDict2[outputTime].keys():
                                self.warmDict2[outputTime][warn_Key] = ''
                                flag2 = 1
                            else:
                                flag2 = 0

                            if flag2 == 1:
                                # print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
                                #     trainTypeCode, trainNo, coachId, trainSpeed, self.out_degree, self.trainsite_id,
                                #     self.brake_pos, sensorKind, sensorKindName, sensorType, newSensorVal, oldSensorVal,
                                #     sameSideVal, meanVal, diffVal, speedVal1, timeLong1, preJudgeDesc, preWarningDesc,
                                #     warningDesc, modelType, outputTime)

                                tmpList = [trainTypeCode, trainNo, coachId, trainSpeed, self.out_degree,
                                           self.trainsite_id,
                                           self.brake_pos, sensorKind, '融合模型:' + sensorKindName, sensorType,
                                           newSensorVal,
                                           oldSensorVal,
                                           sameSideVal, meanVal, diffVal, speedVal, timeLong, preJudgeDesc,
                                           preWarningDesc,
                                           warningDesc, modelType, outputTime]

                                # sd.saveToFile(, tmpList)
                                kazooModel.saveDataToHive(tmpList)

                                ## 事件中心
                                wType = 'sensorComp2'
                                keyId = trainTypeCode + '_' + trainNo + '_' + coachId
                                keyms = keyId + '_' + wType + '_' + sensorKind + '_' + str(sensorType)
                                toDay = outputTime[0:4] + outputTime[5:7] + outputTime[8:10]
                                dt_today = datetime.datetime.strptime(toDay, '%Y%m%d')
                                dayList5 = [(dt_today - datetime.timedelta(days=i)).strftime('%Y%m%d') for i in
                                            range(5)]
                                dayList4 = [(dt_today - datetime.timedelta(days=i)).strftime('%Y%m%d') for i in
                                            range(4)]
                                # 连接MySQL查询数据
                                condition = (trainTypeCode, trainNo,
                                             coachId, '', '', '', wType,
                                             '', outputTime, toDay, sensorKind + '_' + str(sensorType))
                                db.updateOnlineMonitor(condition)  # 将数据插入Mysql数据库表中
                                mysqldata = db.getmysqldata('sensorComp2')
                                eventFlag = 0
                                if keyms in mysqldata:
                                    # 电机传动端 连续3天 或 5天出现3次
                                    if sensorKind == '8':
                                        if dayList5[1] in mysqldata[keyms] and dayList5[2] in mysqldata[keyms]:
                                            warningDesc = '[连续3天满足温差预判条件]' + warningDesc
                                            eventFlag = 1
                                        else:
                                            tmp = [day for day in dayList5 if
                                                   day in mysqldata[keyms]]
                                            if len(tmp) >= 3:
                                                warningDesc = '[5天内3次满足温差预判条件]' + warningDesc
                                                eventFlag = 1
                                    else:  # 其他位置 连续2天 或 4天出现2次
                                        if dayList4[1] in mysqldata[keyms]:
                                            warningDesc = '[连续2天满足温差预判条件]' + warningDesc
                                            eventFlag = 1
                                        else:
                                            tmp = [day for day in dayList4 if
                                                   day in mysqldata[keyms]]
                                            if len(tmp) >= 2:
                                                warningDesc = '[4天内2次满足温差预判条件]' + warningDesc
                                                eventFlag = 1
                                # 满足进事件中心
                                if eventFlag == 1:
                                    tmpList2 = [trainTypeCode, trainNo, coachId, trainSpeed, self.trainsite_id,
                                                sensorKind,
                                                '融合模型:' + sensorKindName, sensorType, newSensorVal, sameSideVal,
                                                preJudgeDesc,
                                                preWarningDesc, warningDesc, modelType, outputTime]

                                    # sd.updateData(tmpList2)

                                # 控制 self.warmDict2的内存
                                if len(self.warmDict2.keys()) > 200:
                                    timeList = sorted(self.warmDict2.keys())
                                    for i in range(len(timeList) - 20):
                                        self.warmDict2.pop(timeList[i])
                            # >>>>>>>>>>>end 模型计算>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

        else:
            # 添加第一条信息至变量preInfo
            self.addFirstInfo(dataId, data, fTime)


class onlineThresholdModel:
    """阈值模型计算"""
    # 传感器位置

    sensorTypeList = [[['1', '1位轴端'], ['1', '3位轴端'], ['1', '5位轴端'], ['1', '7位轴端']],
                      [['2', '2位轴端'], ['2', '4位轴端'], ['2', '6位轴端'], ['2', '8位轴端']],
                      [['3', '1轴小齿轮箱电机侧'], ['3', '2轴小齿轮箱电机侧'], ['3', '3轴小齿轮箱电机侧'], ['3', '4轴小齿轮箱电机侧']],
                      [['4', '1轴小齿轮箱车轮侧'], ['4', '2轴小齿轮箱车轮侧'], ['4', '3轴小齿轮箱车轮侧'], ['4', '4轴小齿轮箱车轮侧']],
                      [['7', '1轴电机定子'], ['7', '2轴电机定子'], ['7', '3轴电机定子'], ['7', '4轴电机定子']],
                      [['8', '1轴电机传动端'], ['8', '2轴电机传动端'], ['8', '3轴电机传动端'], ['8', '4轴电机传动端']],
                      [['9', '1轴电机非传动端'], ['9', '2轴电机非传动端'], ['9', '3轴电机非传动端'], ['9', '4轴电机非传动端']],
                      [['5', '1轴大齿轮箱电机侧'], ['5', '2轴大齿轮箱电机侧'], ['5', '3轴大齿轮箱电机侧'], ['5', '4轴大齿轮箱电机侧']],
                      [['6', '1轴大齿轮箱车轮侧'], ['6', '2轴大齿轮箱车轮侧'], ['6', '3轴大齿轮箱车轮侧'], ['6', '4轴大齿轮箱车轮侧']]
                      ]
    model_type_code = 'm4'
    warmDict = {}

    def transData(self, d):
        """函数功能：数据清洗和转换"""

        global dataSet1
        # 增加数据标志
        addDataFlag = 0
        weight = 0
        num = 0

        # 获取基本信息
        trainTypeCode, trainNo, coachId, fTime, trainSpeed = d[36], d[37], d[38], d[39], d[40]
        dataId = trainTypeCode + '_' + trainNo + '_' + coachId  # 确定数据记录ID

        # 20170515根据融合模型趋势跟踪需求增加输出信息
        self.out_degree, self.trainsite_id, self.brake_pos = d[41], d[42], d[43]

        if len(coachId) < 2:
            coachId = '0' + coachId

        dataId = trainTypeCode + '_' + trainNo + '_' + coachId  # 确定数据记录ID

        # 获取轴温数据
        # sv = [int(item) for item in d[4:40]]
        sv = []
        for item in d[0:36]:
            if item.split('-')[-1].isdigit():
                v = int(item)
            else:
                v = 999
            sv.append(v)

        # 同侧轴温列表
        sensorValueList = [[sv[0], sv[2], sv[4], sv[6]],  # 轴箱1
                           [sv[1], sv[3], sv[5], sv[7]],  # 轴箱2
                           [sv[8], sv[9], sv[10], sv[11]],  # 小齿轮箱电机侧
                           [sv[12], sv[13], sv[14], sv[15]],  # 小齿轮箱车轮侧
                           [sv[16], sv[19], sv[22], sv[25]],  # 电机定子
                           [sv[17], sv[20], sv[23], sv[26]],  # 电机传动端
                           [sv[18], sv[21], sv[24], sv[27]],  # 电机非传动端
                           [sv[28], sv[29], sv[30], sv[31]],  # 大齿轮箱电机侧
                           [sv[32], sv[33], sv[34], sv[35]]  # 大齿轮箱车轮侧
                           ]

        coachType = 'M'
        # 如果是拖车, 则只处理轴端
        num = 9 if coachType == 'M' else 2

        self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.fTime = trainTypeCode, trainNo, coachId, trainSpeed, fTime

        #########################################################
        # 准备平滑数据
        if len(dataSet1) > 0:
            # 判断值是否已经在字典表里存在
            existsKey = [item for item in dataSet1 if item == dataId]
            if len(existsKey) > 0:
                # 获取时间值
                t1 = dataSet1[dataId][-1][0]
                t2 = fTime

                # 计算时间差(单位：秒)
                diffTime = int(t2.split(' ')[1].split(':')[0]) * 60 * 60 + int(
                    t2.split(' ')[1].split(':')[1]) * 60 + int(t2.split(' ')[1].split(':')[2]) - int(
                    t1.split(' ')[1].split(':')[0]) * 60 * 60 - int(t1.split(' ')[1].split(':')[1]) * 60 - int(
                    t1.split(' ')[1].split(':')[2])

                # 如果数据时间间隔超过40s，则重新添加数据
                if diffTime < 0:
                    addDataFlag = 1
                elif diffTime >= 40:
                    addDataFlag = 1
                    if len(dataSet1[dataId]) == 2:  # 如果数据集中已经有两条数据，则只计算这两条数据
                        weight = 2  # 速率计算系数2
                        p1 = dataSet1[dataId][0]  # 获取第1包数据
                        p2 = dataSet1[dataId][1]  # 获取第2包数据
                    elif len(dataSet1[dataId]) == 3:
                        weight = 2  # 速率计算系数2
                        p1 = dataSet1[dataId][1]  # 获取第2包数据
                        p2 = dataSet1[dataId][2]  # 获取第3包数据

                elif diffTime >= 20 and diffTime < 40:
                    # 如果数据包少于3条，则继续记录数据
                    if len(dataSet1[dataId]) < 3:
                        dataSet1[dataId].append([fTime, sensorValueList])
                    else:
                        # 将新数据添加至变量中
                        dataSet1[dataId] = dataSet1[dataId][1:]
                        dataSet1[dataId].append([fTime, sensorValueList])

                    if len(dataSet1[dataId]) == 3:
                        weight = 1  # 速率计算系数1
                        p1 = dataSet1[dataId][0]  # 获取第1包数据
                        p2 = dataSet1[dataId][2]  # 获取第3包数据
            else:
                addDataFlag = 1
        else:
            addDataFlag = 1

        if addDataFlag == 1:
            dataSet1.clear()
            dataSet1[dataId] = [[fTime, sensorValueList]]

        #########################################################
        for i in xrange(0, num):
            if 999 not in sensorValueList[i]:
                self.fTime = fTime

                # 调用温差计算函数
                self.calDiff(i, sensorValueList[i])

            # 计算温升斜率
            if weight > 0:
                calFlag = 1  # 判断温度值是否有999的值，如果没有，则开始计算
                if 999 in p1[1][i] or 999 in p2[1][i]:
                    calFlag = 0
                if calFlag == 1:
                    if i in (0, 1, 2, 3, 7, 8):  # 齿轮箱、轴箱30秒平滑
                        # 计算第一个位置温升斜率
                        s1 = p2[1][i][0]
                        s11 = p1[1][i][0]
                        s12 = p2[1][i][0]
                        sp1 = (s12 - s11) * weight

                        # 计算第二个位置温升斜率
                        s2 = p2[1][i][1]
                        s21 = p1[1][i][1]
                        s22 = p2[1][i][1]
                        sp2 = (s22 - s21) * weight

                        # 计算第三个位置温升斜率
                        s3 = p2[1][i][2]
                        s31 = p1[1][i][2]
                        s32 = p2[1][i][2]
                        sp3 = (s32 - s31) * weight

                        # 计算第四个位置温升斜率
                        s4 = p2[1][i][3]
                        s41 = p1[1][i][3]
                        s42 = p2[1][i][3]
                        sp4 = (s42 - s41) * weight

                        # 获取报警时间
                        self.fTime = p2[0]
                    elif i in (4, 5, 6):  # 牵引电机温升斜率计算
                        # 计算第一个位置温升斜率
                        s1 = p2[1][i][0]
                        s11 = p1[1][i][0]
                        s12 = p2[1][i][0]
                        sp1 = (s12 - s11) * weight

                        # 计算第二个位置温升斜率
                        s2 = p2[1][i][1]
                        s21 = p1[1][i][1]
                        s22 = p2[1][i][1]
                        sp2 = (s22 - s21) * weight

                        # 计算第三个位置温升斜率
                        s3 = p2[1][i][2]
                        s31 = p1[1][i][2]
                        s32 = p2[1][i][2]
                        sp3 = (s32 - s31) * weight

                        # 计算第四个位置温升斜率
                        s4 = p2[1][i][3]
                        s41 = p1[1][i][3]
                        s42 = p2[1][i][3]
                        sp4 = (s42 - s41) * weight

                        # 获取报警时间
                        self.fTime = p2[0]

                    # 调用规则函数进行判断
                    # 有顶棚值
                    self.calSpeed(i, [s1, s2, s3, s4], [sp1, sp2, sp3, sp4])

    def calSpeed(self, seq, sensorValueList, speedList):
        """函数功能：计算温升速率"""
        modelType = '地面PHM轴温模型'
        # xxx = [x for x in speedList if x >8]
        # if xxx:
        #     print '1',seq, speedList

        for i in xrange(0, 4):
            warningType = ''  # 告警类型
            warningDesc = ''  # 告警信息
            warningFlag = 0
            sensorKind = self.sensorTypeList[seq][i][0]
            sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

            # 齿轮箱
            if seq in (2, 3, 7, 8):
                # 预判
                preJudgeSpeed = [8, 10]

                # 预判
                if speedList[i] >= preJudgeSpeed[0] and speedList[i] < preJudgeSpeed[1]:
                    warningType = 'YP'
                    warningDesc = '温升速率大于等于8℃/min小于10℃/min'
                    warningFlag = 1

            # 牵引电机温升速率计算
            elif seq in (4, 5, 6):
                # 预判
                if seq == 4:
                    preJudgeSpeed = 13  # 电机定子, 电机传动端
                    if speedList[i] >= preJudgeSpeed:
                        warningType = 'YP'
                        warningDesc = '温升速率>' + str(preJudgeSpeed) + '℃/min'
                        warningFlag = 1

                elif seq == 5:
                    preJudgeSpeed = 13  # 电机定子, 电机传动端
                    if speedList[i] >= preJudgeSpeed:
                        warningType = 'YP'
                        warningDesc = '温升速率>' + str(preJudgeSpeed) + '℃/min'
                        warningFlag = 1

                elif seq == 6:
                    preJudgeSpeed = 8  # 电机非传动端
                    if speedList[i] >= preJudgeSpeed:
                        warningType = 'YP'
                        warningDesc = '温升速率>' + str(preJudgeSpeed) + '℃/min'
                        warningFlag = 1

            if warningFlag == 1:
                # 输出结果
                self.output(
                    [sensorKind, '轴温温升:' + sensorKindName, sensorValueList, sensorValueList[i], 0, speedList[i],
                     warningType,
                     warningDesc, modelType])

    def calDiff(self, seq, sensorValueList):
        """函数功能：计算温差"""
        modelType = '地面PHM轴温模型'

        # 计算各个位置的温差值
        diff1 = sensorValueList[0] - sum(sensorValueList[1:4]) / 3
        diff2 = sensorValueList[1] - (sensorValueList[0] + sum(sensorValueList[2:4])) / 3
        diff3 = sensorValueList[2] - (sum(sensorValueList[0:2]) + sensorValueList[3]) / 3
        diff4 = sensorValueList[3] - sum(sensorValueList[0:3]) / 3
        diffList = [diff1, diff2, diff3, diff4]

        # xxx=[x for x in diffList if x > 30]
        # if xxx:
        #     print '2', seq, diffList

        warningType = ''
        warningDesc = ''

        #########################################
        for i in xrange(0, 4):
            sensorKind = self.sensorTypeList[seq][i][0]
            sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置
            warningFlag = 0

            # 轴箱计算
            if seq in (0, 1):
                # 轴箱参数

                # 温度阀值
                preJudgeVal = 100
                # 预判
                if sensorValueList[i] >= preJudgeVal:
                    warningType = 'YP'
                    warningDesc = '温度大于等于' + str(preJudgeVal) + '℃'
                    warningFlag = 1
                    self.output(
                        [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                         warningType,
                         warningDesc, modelType])

                # 预判
                preJudgeDiff = [40, 50]  # 温差阀值
                if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                    warningType = 'YP'
                    warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                    warningFlag = 1
                    self.output(
                        [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                         warningType,
                         warningDesc, modelType])


            # 齿轮箱计算
            elif seq in (2, 3, 7, 8):
                # 温度阀值

                # 预判
                preJudgeVal = 115  # 预判温度阀值
                if sensorValueList[i] >= preJudgeVal:
                    warningType = 'YP'
                    warningDesc = '温度高于等于' + str(preJudgeVal) + '℃'
                    warningFlag = 1
                    self.output(
                        [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                         warningType,
                         warningDesc, modelType])

                # 预判
                preJudgeDiff = [30, 40]  # 温差阀值
                if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                    warningType = 'YP'
                    warningDesc = '高于同车同侧点其它轴的温度平均值' + str(preJudgeDiff[0]) + '℃至' + str(preJudgeDiff[1]) + '℃'
                    warningFlag = 1
                    self.output(
                        [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                         warningType,
                         warningDesc, modelType])

            # 牵引电机计算
            elif seq in (4, 5, 6):
                if seq == 4:
                    preJudgeVal = 175  # 阀值
                    preJudgeDiff = 35  # 温差
                    # 预判
                    if sensorValueList[i] >= preJudgeVal:
                        warningType = 'YP'
                        warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                        warningFlag = 1
                        self.output(
                            [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                             warningType,
                             warningDesc, modelType])

                    if diffList[i] >= preJudgeDiff:
                        warningType = 'YP'
                        warningDesc = '高于等于同一辆车同测点温度平均值' + str(preJudgeDiff) + '℃(去除预判轴温)'
                        warningFlag = 1
                        self.output(
                            [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                             warningType,
                             warningDesc, modelType])

                elif seq == 5:
                    preJudgeVal = 115  # 电机传动端
                    preJudgeDiff = 35  # 电机传动端温差
                    # 预判
                    if sensorValueList[i] >= preJudgeVal:
                        warningType = 'YP'
                        warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                        warningFlag = 1
                        self.output(
                            [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                             warningType,
                             warningDesc, modelType])

                    if diffList[i] >= preJudgeDiff:
                        warningType = 'YP'
                        warningDesc = '高于等于同一辆车同测点温度平均值' + str(preJudgeDiff) + '℃(去除预判轴温)'
                        warningFlag = 1
                        self.output(
                            [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                             warningType,
                             warningDesc, modelType])
                elif seq == 6:
                    preJudgeVal = 115  # 电机非传动端
                    preJudgeDiff = 15  # 电机非传动端温差
                    # 预判
                    if sensorValueList[i] >= preJudgeVal:
                        warningType = 'YP'
                        warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                        warningFlag = 1
                        self.output(
                            [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                             warningType,
                             warningDesc, modelType])

                    if diffList[i] >= preJudgeDiff:
                        warningType = 'YP'
                        warningDesc = '高于等于同一辆车同测点温度平均值' + str(preJudgeDiff) + '℃(去除预判轴温)'
                        warningFlag = 1
                        self.output(
                            [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0,
                             warningType,
                             warningDesc, modelType])

            # if warningFlag == 1:
            #     # 输出结果
            #     self.output(
            #         [sensorKind, sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0, warningType,
            #          warningDesc, modelType])

    def output(self, outputList):
        """函数功能：输出结果信息"""
        sensorTypeL = {'1位轴端': '1', '2位轴端': '2', '3位轴端': '3', '4位轴端': '4',
                       '5位轴端': '5', '6位轴端': '6', '7位轴端': '7', '8位轴端': '8',
                       '1轴小齿轮箱电机侧': '9', '2轴小齿轮箱电机侧': '10', '3轴小齿轮箱电机侧': '11', '4轴小齿轮箱电机侧': '12',
                       '1轴小齿轮箱车轮侧': '13', '2轴小齿轮箱车轮侧': '14', '3轴小齿轮箱车轮侧': '15', '4轴小齿轮箱车轮侧': '16',
                       '1轴电机定子': '17', '1轴电机传动端': '18', '1轴电机非传动端': '19',
                       '2轴电机定子': '20', '2轴电机传动端': '21', '2轴电机非传动端': '22',
                       '3轴电机定子': '23', '3轴电机传动端': '24', '3轴电机非传动端': '25',
                       '4轴电机定子': '26', '4轴电机传动端': '27', '4轴电机非传动端': '28',
                       '1轴大齿轮箱电机侧': '29', '2轴大齿轮箱电机侧': '30', '3轴大齿轮箱电机侧': '31', '4轴大齿轮箱电机侧': '32',
                       '1轴大齿轮箱车轮侧': '33', '2轴大齿轮箱车轮侧': '34', '3轴大齿轮箱车轮侧': '35', '4轴大齿轮箱车轮侧': '36'}

        # 获取参数信息
        outputTime = self.fTime
        sensorKind = outputList[0]
        sensorKindName = outputList[1]
        sensorValueList = outputList[2]
        sensorValue = outputList[3]
        diffValue = outputList[4]
        speedValue = outputList[5]
        warningType = outputList[6]
        warningDesc = outputList[7]
        modelType = outputList[8]
        sensorType = sensorTypeL[sensorKindName.split(':')[-1]]
        sameSideVal = ','.join([str(item) for item in sensorValueList])

        prejudgeDesc = ''
        preWarnDesc = ''
        warnDesc = ''

        warn_Key = self.trainNo + '_' + self.coachId + '_' + warningType + '_' + self.model_type_code + '_' + sensorType

        # 如果以前没有报过警 则进行报警输出
        flag = 0

        # 如果以前没有报过警 则进行报警输出
        if outputTime not in self.warmDict.keys():
            self.warmDict[outputTime] = {warn_Key: ''}
            flag = 1
        elif warn_Key not in self.warmDict[outputTime].keys():
            self.warmDict[outputTime][warn_Key] = ''
            flag = 1
        else:
            flag = 0

        if flag == 1:
            if warningType == 'YP':
                prejudgeDesc = warningType + ':' + warningDesc
            elif warningType == 'YJ':
                preWarnDesc = warningType + ':' + warningDesc
            elif warningType == 'BJ':
                warnDesc = warningType + ':' + warningDesc

            # print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
            #     self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.out_degree, self.trainsite_id,
            #     self.brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '', diffValue,
            #     speedValue,
            #     '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime)

            tmpList = [self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.out_degree,
                       self.trainsite_id,
                       self.brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '',
                       diffValue,
                       speedValue,
                       '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime]

            # sd.saveToFile(, tmpList)
            kazooModel.saveDataToHive(tmpList)

            tmpList2 = [self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.trainsite_id, sensorKind,
                        sensorKindName, sensorType, sensorValue, sameSideVal, prejudgeDesc, preWarnDesc, warnDesc,
                        modelType, outputTime]

            # sd.updateData(tmpList2)

            # 控制 self.warmDict1的内存
            if len(self.warmDict.keys()) > 200:
                timeList = sorted(self.warmDict.keys())
                for i in range(len(timeList) - 20):
                    self.warmDict.pop(timeList[i])


class onTrainModel:
    # 传感器位置
    sensorTypeList = [[['1', '1位轴端'], ['1', '3位轴端'], ['1', '5位轴端'], ['1', '7位轴端']],
                      [['2', '2位轴端'], ['2', '4位轴端'], ['2', '6位轴端'], ['2', '8位轴端']],
                      [['3', '1轴小齿轮箱电机侧'], ['3', '2轴小齿轮箱电机侧'], ['3', '3轴小齿轮箱电机侧'], ['3', '4轴小齿轮箱电机侧']],
                      [['4', '1轴小齿轮箱车轮侧'], ['4', '2轴小齿轮箱车轮侧'], ['4', '3轴小齿轮箱车轮侧'], ['4', '4轴小齿轮箱车轮侧']],
                      [['7', '1轴电机定子'], ['7', '2轴电机定子'], ['7', '3轴电机定子'], ['7', '4轴电机定子']],
                      [['8', '1轴电机传动端'], ['8', '2轴电机传动端'], ['8', '3轴电机传动端'], ['8', '4轴电机传动端']],
                      [['9', '1轴电机非传动端'], ['9', '2轴电机非传动端'], ['9', '3轴电机非传动端'], ['9', '4轴电机非传动端']],
                      [['5', '1轴大齿轮箱电机侧'], ['5', '2轴大齿轮箱电机侧'], ['5', '3轴大齿轮箱电机侧'], ['5', '4轴大齿轮箱电机侧']],
                      [['6', '1轴大齿轮箱车轮侧'], ['6', '2轴大齿轮箱车轮侧'], ['6', '3轴大齿轮箱车轮侧'], ['6', '4轴大齿轮箱车轮侧']]
                      ]

    model_type_code = 'm3'
    warmDict = {}

    def doData(self, d):
        """函数功能：数据清洗和转换"""
        # 增加数据标志
        addDataFlag = 0

        global dataSet2

        # 获取基础数据信息
        trainTypeCode, trainNo, coachId, fTime, trainSpeed = d[36], d[37], d[38], d[39], d[40]

        out_degree, trainsite_id, brake_pos = d[41], d[42], d[43]

        if len(coachId) < 2:
            coachId = '0' + coachId

        dataId = trainTypeCode + '_' + trainNo + '_' + coachId  # 确定数据记录ID

        # 获取轴温数据
        # sv = [int(item) for item in d[4:40]]
        sv = []
        for item in d[0:36]:
            if item.split('-')[-1].isdigit():
                v = int(item)
            else:
                v = 999
            sv.append(v)

        # 同侧轴温列表
        # sensorValueList = [sv[0:4], sv[4:8], sv[8:12], sv[12:16], sv[16:20], sv[20:24], sv[24:28], sv[28:32], sv[32:36]]
        sensorValueList = [[sv[0], sv[2], sv[4], sv[6]],  # 轴箱1
                           [sv[1], sv[3], sv[5], sv[7]],  # 轴箱2
                           [sv[8], sv[9], sv[10], sv[11]],  # 小齿轮箱电机侧
                           [sv[12], sv[13], sv[14], sv[15]],  # 小齿轮箱车轮侧
                           [sv[16], sv[19], sv[22], sv[25]],  # 电机定子
                           [sv[17], sv[20], sv[23], sv[26]],  # 电机传动端
                           [sv[18], sv[21], sv[24], sv[27]],  # 电机非传动端
                           [sv[28], sv[29], sv[30], sv[31]],  # 大齿轮箱电机侧
                           [sv[32], sv[33], sv[34], sv[35]]  # 大齿轮箱车轮侧
                           ]

        coachType = 'M'
        if coachType == 'M':
            num = 9
        elif coachType == 'T':  # 如果是拖车, 则只处理轴端
            num = 2

        self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.fTime = trainTypeCode, trainNo, coachId, trainSpeed, fTime

        self.out_degree, self.trainsite_id, self.brake_pos = d[41], d[42], d[43]

        otherInfoList = [trainTypeCode, trainNo, coachId, trainSpeed, out_degree, trainsite_id, brake_pos, fTime]

        #########################################################
        # 准备平滑数据
        if len(dataSet2) > 0:
            # 判断值是否已经在字典表里存在
            existsKey = [item for item in dataSet2 if item == dataId]
            if len(existsKey) > 0:
                # 获取时间值
                t1 = dataSet2[dataId][-1][0]
                t2 = fTime

                # 计算时间差(单位：秒)
                diffTime = int(t2.split(' ')[1].split(':')[0]) * 60 * 60 + int(
                    t2.split(' ')[1].split(':')[1]) * 60 + int(t2.split(' ')[1].split(':')[2]) - int(
                    t1.split(' ')[1].split(':')[0]) * 60 * 60 - int(t1.split(' ')[1].split(':')[1]) * 60 - int(
                    t1.split(' ')[1].split(':')[2])

                # 如果数据时间间隔超过1分钟，则重新添加数据
                if diffTime >= 60 or diffTime < 0:
                    addDataFlag = 1
                elif diffTime > 20 and diffTime < 40:
                    # 如果数据包少于6条，则继续记录数据
                    if len(dataSet2[dataId]) < 6:
                        dataSet2[dataId].append([fTime, sensorValueList, otherInfoList])
                    else:
                        # 将新数据添加至变量中
                        dataSet2[dataId] = dataSet2[dataId][1:]
                        dataSet2[dataId].append([fTime, sensorValueList, otherInfoList])
            else:
                addDataFlag = 1
        else:
            addDataFlag = 1

        if addDataFlag == 1:
            dataSet2.clear()
            dataSet2[dataId] = [[fTime, sensorValueList, otherInfoList]]

        flag = 0
        flag3 = 0

        # 获取数据包
        if addDataFlag == 0:
            if len(dataSet2[dataId]) >= 4:
                p11 = dataSet2[dataId][-4]
                p22 = dataSet2[dataId][-3]
                p33 = dataSet2[dataId][-2]
                p44 = dataSet2[dataId][-1]
                flag3 = 1

            if len(dataSet2[dataId]) >= 6:
                # 获取6个数据包
                p1 = dataSet2[dataId][0]
                p2 = dataSet2[dataId][1]
                p3 = dataSet2[dataId][2]
                p4 = dataSet2[dataId][3]
                p5 = dataSet2[dataId][4]
                p6 = dataSet2[dataId][5]

                flag = 1

        #########################################################
        for i in xrange(0, num):
            if 999 not in sensorValueList[i]:
                self.fTime = fTime
                # 调用温差计算函数(有顶棚值)  地面模型已判断
                self.calDiffNoLimit(i, sensorValueList[i])

                # 调用温差计算函数(有顶棚值)  规则2 20180524 王伟确认下线 滤波模型_业务部门2
                # self.calTopValue(i, sensorValueList[i])

            if flag3 == 1:
                calFlag3 = 1  # 判断温度值是否有999的值，如果没有，则开始计算
                if 999 in p11[1][i] or 999 in p22[1][i] or 999 in p33[1][i] or 999 in p44[1][i]:
                    calFlag3 = 0

                if calFlag3 == 1:
                    # 计算温升斜率
                    v11 = p22[1][i][0] - p11[1][i][0]  # 第一段温升斜率
                    v12 = p33[1][i][0] - p22[1][i][0]  # 第二段温升斜率
                    v13 = p44[1][i][0] - p33[1][i][0]  # 第三段温升斜率

                    v21 = p22[1][i][1] - p11[1][i][1]  # 第一段温升斜率
                    v22 = p33[1][i][1] - p22[1][i][1]  # 第二段温升斜率
                    v23 = p44[1][i][1] - p33[1][i][1]  # 第三段温升斜率

                    v31 = p22[1][i][2] - p11[1][i][2]  # 第一段温升斜率
                    v32 = p33[1][i][2] - p22[1][i][2]  # 第二段温升斜率
                    v33 = p44[1][i][2] - p33[1][i][2]  # 第三段温升斜率

                    v41 = p22[1][i][3] - p11[1][i][3]  # 第一段温升斜率
                    v42 = p33[1][i][3] - p22[1][i][3]  # 第二段温升斜率
                    v43 = p44[1][i][3] - p33[1][i][3]  # 第三段温升斜率

                    ########################################################
                    # 业务部门提供的滤波规则
                    l1 = [v11, v12, v13]
                    l1.sort()
                    sp11 = 2 * l1[1]
                    ## 得到中间速率对应的轴温,整车信息和同侧四根轴的数据
                    if l1[1] == v13:
                        fp11 = p44[2]  # 整车数据
                        ssv1 = p44[1][i][0]  # 轴温
                        sv1 = [p44[1][i][0], p44[1][i][1], p44[1][i][2], p44[1][i][3]]  # 四根轴
                    elif l1[1] == v12:
                        fp11 = p33[2]
                        ssv1 = p33[1][i][0]
                        sv1 = [p33[1][i][0], p33[1][i][1], p33[1][i][2], p33[1][i][3]]
                    elif l1[1] == v11:
                        fp11 = p22[2]
                        ssv1 = p22[1][i][0]
                        sv1 = [p22[1][i][0], p22[1][i][1], p22[1][i][2], p22[1][i][3]]

                    l2 = [v21, v22, v23]
                    l2.sort()
                    sp22 = 2 * l2[1]
                    if l2[1] == v23:
                        fp22 = p44[2]
                        ssv2 = p44[1][i][1]
                        sv2 = [p44[1][i][0], p44[1][i][1], p44[1][i][2], p44[1][i][3]]
                    elif l2[1] == v22:
                        fp22 = p33[2]
                        ssv2 = p33[1][i][1]
                        sv2 = [p33[1][i][0], p33[1][i][1], p33[1][i][2], p33[1][i][3]]
                    elif l2[1] == v21:
                        fp22 = p22[2]
                        ssv2 = p22[1][i][1]
                        sv2 = [p22[1][i][0], p22[1][i][1], p22[1][i][2], p22[1][i][3]]

                    l3 = [v31, v32, v33]
                    l3.sort()
                    sp33 = 2 * l3[1]
                    if l3[1] == v33:
                        fp33 = p44[2]
                        ssv3 = p44[1][i][2]
                        sv3 = [p44[1][i][0], p44[1][i][1], p44[1][i][2], p44[1][i][3]]
                    elif l3[1] == v32:
                        fp33 = p33[2]
                        ssv3 = p33[1][i][2]
                        sv3 = [p33[1][i][0], p33[1][i][1], p33[1][i][2], p33[1][i][3]]
                    elif l3[1] == v31:
                        fp33 = p22[2]
                        ssv3 = p22[1][i][2]
                        sv3 = [p22[1][i][0], p22[1][i][1], p22[1][i][2], p22[1][i][3]]

                    l4 = [v41, v42, v43]
                    l4.sort()
                    sp44 = 2 * l4[1]
                    if l4[1] == v43:
                        fp44 = p44[2]
                        ssv4 = p44[1][i][3]
                        sv4 = [p44[1][i][0], p44[1][i][1], p44[1][i][2], p44[1][i][3]]
                    elif l4[1] == v42:
                        fp44 = p33[2]
                        ssv4 = p33[1][i][3]
                        sv4 = [p33[1][i][0], p33[1][i][1], p33[1][i][2], p33[1][i][3]]
                    elif l4[1] == v41:
                        fp44 = p22[2]
                        ssv4 = p22[1][i][3]
                        sv4 = [p22[1][i][0], p22[1][i][1], p22[1][i][2], p22[1][i][3]]

                    # 获取报警时间
                    self.fTime = dataSet2[dataId][2][0]

                    # 调用规则函数进行判断
                    # 已添加顶棚值
                    self.calSpeedNoLimit(i, [ssv1, ssv2, ssv3, ssv4], [sp11, sp22, sp33, sp44],
                                         [fp11, fp22, fp33, fp44], [sv1, sv2, sv3, sv4])

            #####################################################
            if flag == 1:
                calFlag = 1  # 判断温度值是否有999的值，如果没有，则开始计算
                if 999 in p1[1][i] or 999 in p2[1][i] or 999 in p3[1][i] or 999 in p4[1][i] or 999 in p5[1][i] or 999 in \
                        p6[1][i]:
                    calFlag = 0

                if calFlag == 1:
                    # 计算温升斜率
                    v11 = 2 * (p2[1][i][0] - p1[1][i][0])  # 第一段温升斜率
                    v12 = 2 * (p3[1][i][0] - p2[1][i][0])  # 第二段温升斜率
                    v13 = 2 * (p4[1][i][0] - p3[1][i][0])  # 第三段温升斜率
                    v14 = 2 * (p5[1][i][0] - p4[1][i][0])  # 第四段温升斜率
                    v15 = 2 * (p6[1][i][0] - p5[1][i][0])  # 第五段温升斜率

                    v21 = 2 * (p2[1][i][1] - p1[1][i][1])  # 第一段温升斜率
                    v22 = 2 * (p3[1][i][1] - p2[1][i][1])  # 第二段温升斜率
                    v23 = 2 * (p4[1][i][1] - p3[1][i][1])  # 第三段温升斜率
                    v24 = 2 * (p5[1][i][1] - p4[1][i][1])  # 第四段温升斜率
                    v25 = 2 * (p6[1][i][1] - p5[1][i][1])  # 第五段温升斜率

                    v31 = 2 * (p2[1][i][2] - p1[1][i][2])  # 第一段温升斜率
                    v32 = 2 * (p3[1][i][2] - p2[1][i][2])  # 第二段温升斜率
                    v33 = 2 * (p4[1][i][2] - p3[1][i][2])  # 第三段温升斜率
                    v34 = 2 * (p5[1][i][2] - p4[1][i][2])  # 第四段温升斜率
                    v35 = 2 * (p6[1][i][2] - p5[1][i][2])  # 第五段温升斜率

                    v41 = 2 * (p2[1][i][3] - p1[1][i][3])  # 第一段温升斜率
                    v42 = 2 * (p3[1][i][3] - p2[1][i][3])  # 第二段温升斜率
                    v43 = 2 * (p4[1][i][3] - p3[1][i][3])  # 第三段温升斜率
                    v44 = 2 * (p5[1][i][3] - p4[1][i][3])  # 第四段温升斜率
                    v45 = 2 * (p6[1][i][3] - p5[1][i][3])  # 第五段温升斜率

                    ########################################################
                    # 林森提供的规则
                    spL1 = [v11, v12, v13, v14, v15]
                    spL2 = [v21, v22, v23, v24, v25]
                    spL3 = [v31, v32, v33, v34, v35]
                    spL4 = [v41, v42, v43, v44, v45]

                    fp1 = p4[2]
                    fp2 = p4[2]
                    fp3 = p4[2]
                    fp4 = p4[2]

                    # 已添加顶棚值
                    self.calSpeedNoLimit_v3(i, [p4[1][i][0], p4[1][i][1], p4[1][i][2], p4[1][i][3]],
                                            [spL1, spL2, spL3, spL4], [fp1, fp2, fp3, fp4])

    # 已添加顶棚值
    def calSpeedNoLimit(self, seq, sensorValueList, speedList, infoList, sensor4List):
        """函数功能：计算温升速率
        :param seq: string, 轴类编号。
        :param sensorValueList: list,每个轴位的四轴温升速率对应时刻的自身轴温。
        :param speedList: list,每个轴位的四轴温升列表。
        :param infoList: 二维list, 每个轴位的四轴温升速率对应时刻的整车信息
        :param sensor4List: 二维list, 每个轴位的四轴温升速率对应的同侧四轴温度数据
        """
        #########################################
        # 轴箱、齿轮箱温升斜率计算
        if seq in (0, 1, 2, 3, 7, 8):
            modelType = '温升预判_网开滤波'
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置名称

                # 轴箱 无温升预判
                # 齿轮箱
                if seq in (2, 3, 7, 8):
                    if self.trainTypeCode in sensorB[0]:
                        # 预判
                        preJudgeSpeed = 8
                        if speedList[i] >= preJudgeSpeed:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min'

                    elif self.trainTypeCode in sensorB[1]:
                        # 预判
                        preJudgeSpeed = 10
                        if speedList[i] >= preJudgeSpeed:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min'

                if len(warningDesc) > 0 and warningType == 'YP':
                    # 输出结果
                    self.output2(
                        # [sensorKindName, sensorValueList[i], 0, speedList[i], warningType, warningDesc, modelType])
                        [sensorKind, '轴温温升:' + sensorKindName, sensor4List[i], sensorValueList[i], 0, speedList[i],
                         warningType,
                         warningDesc, modelType], infoList[i])

        #########################################
        # 牵引电机温升斜率计算
        if seq in (4, 5, 6):
            modelType = '温升预判_网开滤波'
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置名称

                if seq == 5:
                    if self.trainTypeCode in sensorC[0]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 60
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                elif seq == 4:
                    if self.trainTypeCode in sensorD[0]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 90
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'
                    elif self.trainTypeCode in sensorD[1]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 90
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                elif seq == 6:
                    if self.trainTypeCode in sensorE[0]:
                        # 预判
                        preJudgeSpeed = 8
                        preJudgeValue = 40
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                if len(warningDesc) > 0 and warningType == 'YP':
                    # 输出结果
                    self.output2(
                        # [sensorKindName, sensorValueList[i], 0, speedList[i], warningType, warningDesc, modelType])
                        [sensorKind, '轴温温升:' + sensorKindName, sensor4List[i], sensorValueList[i], 0, speedList[i],
                         warningType,
                         warningDesc, modelType], infoList[i])

    def calSpeedNoLimit_v3(self, seq, sensorValueList, speedList, infoList):
        """函数功能：计算温升速率"""
        #########################################
        # 轴箱、齿轮箱温升斜率计算
        if seq in (0, 1, 2, 3, 7, 8):
            modelType = '温升预判_电开滤波'
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

                # 轴箱 无温升预判
                # 齿轮箱
                if seq in (2, 3, 7, 8):
                    if self.trainTypeCode in sensorB[0]:
                        # 预判
                        preJudgeSpeed = 8
                        if speedList[i][2] >= preJudgeSpeed:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按滤波平滑计算）'

                            if speedList[i][0] < 0 and abs(speedList[i][0]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][1] < 0 and abs(speedList[i][1]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][3] < 0 and abs(speedList[i][3]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][4] < 0 and abs(speedList[i][4]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''

                    elif self.trainTypeCode in sensorB[1]:
                        # 预判
                        preJudgeSpeed = 10
                        if speedList[i][2] >= preJudgeSpeed:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min'

                            if speedList[i][0] < 0 and abs(speedList[i][0]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][1] < 0 and abs(speedList[i][1]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][3] < 0 and abs(speedList[i][3]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][4] < 0 and abs(speedList[i][4]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''

                if len(warningDesc) > 0 and warningType == 'YP':
                    # 输出结果
                    self.output2(
                        # [sensorKindName, sensorValueList[i], 0, speedList[i][2], warningType, warningDesc, modelType])
                        [sensorKind, '轴温温升:' + sensorKindName, sensorValueList, sensorValueList[i], 0, speedList[i][2],
                         warningType, warningDesc, modelType], infoList[i])

        #########################################
        # 牵引电机温升斜率计算
        if seq in (4, 5, 6):
            modelType = '温升预判_电开滤波'
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

                if seq == 5:
                    if self.trainTypeCode in sensorC[0]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 60
                        if speedList[i][2] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                            if speedList[i][0] < 0 and abs(speedList[i][0]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][1] < 0 and abs(speedList[i][1]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][3] < 0 and abs(speedList[i][3]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][4] < 0 and abs(speedList[i][4]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''

                elif seq == 4:
                    if self.trainTypeCode in sensorD[0]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 90
                        if speedList[i][2] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                            if speedList[i][0] < 0 and abs(speedList[i][0]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][1] < 0 and abs(speedList[i][1]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][3] < 0 and abs(speedList[i][3]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][4] < 0 and abs(speedList[i][4]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''

                    elif self.trainTypeCode in sensorD[1]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 90
                        if speedList[i][2] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                            if speedList[i][0] < 0 and abs(speedList[i][0]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][1] < 0 and abs(speedList[i][1]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][3] < 0 and abs(speedList[i][3]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][4] < 0 and abs(speedList[i][4]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''

                elif seq == 6:
                    if self.trainTypeCode in sensorE[0]:
                        # 预判
                        preJudgeSpeed = 8
                        preJudgeValue = 40
                        if speedList[i][2] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min,同时温度值≥' + str(preJudgeValue) + '℃'

                            if speedList[i][0] < 0 and abs(speedList[i][0]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][1] < 0 and abs(speedList[i][1]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][3] < 0 and abs(speedList[i][3]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''
                            elif speedList[i][4] < 0 and abs(speedList[i][4]) > (preJudgeSpeed / 2):
                                warningType = ''
                                warningDesc = ''

                if len(warningDesc) > 0 and warningType == 'YP':
                    # 输出结果
                    self.output2(
                        # [sensorKindName, sensorValueList[i], 0, speedList[i][2], warningType, warningDesc, modelType])
                        [sensorKind, '轴温温升:' + sensorKindName, sensorValueList, sensorValueList[i], 0, speedList[i][2],
                         warningType, warningDesc, modelType], infoList[i])

    def calDiffNoLimit(self, seq, sensorValueList):
        """函数功能：计算温差"""
        # 轴箱、齿轮箱温差计算
        modelType = '阈值和温差预判'
        diff1 = sensorValueList[0] - sum(sensorValueList[1:4]) / 3
        diff2 = sensorValueList[1] - (sensorValueList[0] + sum(sensorValueList[2:4])) / 3
        diff3 = sensorValueList[2] - (sum(sensorValueList[0:2]) + sensorValueList[3]) / 3
        diff4 = sensorValueList[3] - sum(sensorValueList[0:3]) / 3
        diffList = [diff1, diff2, diff3, diff4]

        warningType = ''
        warningDesc = ''

        #########################################
        if seq in (0, 1, 2, 3, 7, 8):
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

                # 轴箱
                if seq in (0, 1):
                    if self.trainTypeCode in sensorA[0]:
                        preJudgeVal = 100  # 预判温度阀值
                        preJudgeDiff = [40, 50]  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                    elif self.trainTypeCode in sensorA[1]:
                        preJudgeVal = 90  # 预判温度阀值
                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                # 齿轮箱
                elif seq in (2, 3, 7, 8):
                    if self.trainTypeCode in sensorB[0]:
                        preJudgeVal = 115  # 预判温度阀值
                        preJudgeDiff = [35, 40]  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                    elif self.trainTypeCode in sensorB[1]:
                        preJudgeVal = 115  # 预判温度阀值
                        preJudgeDiff = [35, 40]  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                # if len(warningDesc) > 0 and warningType == 'YP':
                #     # 输出结果
                #     self.output(
                #         # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                #         [sensorKind, sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0, warningType,
                #          warningDesc, modelType])

        ############################################################
        elif seq in (4, 5, 6):
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

                if seq == 5:
                    if self.trainTypeCode in sensorC[0]:
                        preJudgeVal = 105  # 预判温度阀值
                        preJudgeDiff = 35  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                elif seq == 4:
                    if self.trainTypeCode in sensorD[0]:
                        preJudgeVal = 180  # 预判温度阀值
                        preJudgeDiff = 35  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                    elif self.trainTypeCode in sensorD[1]:
                        preJudgeVal = 175  # 预判温度阀值
                        preJudgeDiff = 35  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                elif seq == 6:
                    if self.trainTypeCode in sensorE[0]:
                        preJudgeVal = 85  # 预判温度阀值
                        preJudgeDiff = 13  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                # if len(warningDesc) > 0 and warningType == 'YP':
                #     # 输出结果
                #     self.output(
                #         # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                #         [sensorKind, sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0, warningType,
                #          warningDesc, modelType])

    def output(self, outputList):
        """函数功能：输出结果信息"""
        # 获取参数信息
        """
        sensorKindName = outputList[0]
        sensorValue = outputList[1]
        diffValue = outputList[2]
        speedValue = outputList[3]
        warningType = outputList[4]
        warningDesc = outputList[5]
        modelType = outputList[6]

        print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
        self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, sensorKindName, sensorValue, diffValue,
        speedValue, warningType, warningDesc, modelType, self.fTime)
        """
        sensorTypeL = {'1位轴端': '1', '2位轴端': '2', '3位轴端': '3', '4位轴端': '4',
                       '5位轴端': '5', '6位轴端': '6', '7位轴端': '7', '8位轴端': '8',
                       '1轴小齿轮箱电机侧': '9', '2轴小齿轮箱电机侧': '10', '3轴小齿轮箱电机侧': '11', '4轴小齿轮箱电机侧': '12',
                       '1轴小齿轮箱车轮侧': '13', '2轴小齿轮箱车轮侧': '14', '3轴小齿轮箱车轮侧': '15', '4轴小齿轮箱车轮侧': '16',
                       '1轴电机定子': '17', '1轴电机传动端': '18', '1轴电机非传动端': '19',
                       '2轴电机定子': '20', '2轴电机传动端': '21', '2轴电机非传动端': '22',
                       '3轴电机定子': '23', '3轴电机传动端': '24', '3轴电机非传动端': '25',
                       '4轴电机定子': '26', '4轴电机传动端': '27', '4轴电机非传动端': '28',
                       '1轴大齿轮箱电机侧': '29', '2轴大齿轮箱电机侧': '30', '3轴大齿轮箱电机侧': '31', '4轴大齿轮箱电机侧': '32',
                       '1轴大齿轮箱车轮侧': '33', '2轴大齿轮箱车轮侧': '34', '3轴大齿轮箱车轮侧': '35', '4轴大齿轮箱车轮侧': '36'}

        # 获取参数信息
        outputTime = self.fTime
        sensorKind = outputList[0]
        sensorKindName = outputList[1]
        sensorValueList = outputList[2]
        sensorValue = outputList[3]
        diffValue = outputList[4]
        speedValue = outputList[5]
        warningType = outputList[6]
        warningDesc = outputList[7]
        modelType = outputList[8]
        sensorType = sensorTypeL[sensorKindName.split(':')[-1]]
        sameSideVal = ','.join([str(item) for item in sensorValueList])

        prejudgeDesc = ''
        preWarnDesc = ''
        warnDesc = ''

        typeinfo = warningDesc[:2]

        warn_Key = self.trainNo + '_' + self.coachId + '_' + warningType + '_' + self.model_type_code + '_' + sensorType + '_' + typeinfo

        # 如果以前没有报过警 则进行报警输出
        flag = 0

        # 如果以前没有报过警 则进行报警输出
        if outputTime not in self.warmDict.keys():
            self.warmDict[outputTime] = {warn_Key: ''}
            flag = 1
        elif warn_Key not in self.warmDict[outputTime].keys():
            self.warmDict[outputTime][warn_Key] = ''
            flag = 1
        else:
            flag = 0

        if flag == 1:
            if warningType == 'YP':
                prejudgeDesc = warningType + ':' + warningDesc
            elif warningType == 'YJ':
                preWarnDesc = warningType + ':' + warningDesc
            elif warningType == 'BJ':
                warnDesc = warningType + ':' + warningDesc

            # print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
            #     self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.out_degree, self.trainsite_id,
            #     self.brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '', diffValue,
            #     speedValue,
            #     '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime)

            tmpList = [self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.out_degree,
                       self.trainsite_id,
                       self.brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '',
                       diffValue,
                       speedValue,
                       '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime]

            # sd.saveToFile(, tmpList)
            kazooModel.saveDataToHive(tmpList)

            tmpList2 = [self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.trainsite_id, sensorKind,
                        sensorKindName, sensorType, sensorValue, sameSideVal, prejudgeDesc, preWarnDesc, warnDesc,
                        modelType, outputTime]

            # sd.updateData(tmpList2)

            # 控制 self.warmDict1的内存
            if len(self.warmDict.keys()) > 200:
                timeList = sorted(self.warmDict.keys())
                for i in range(len(timeList) - 20):
                    self.warmDict.pop(timeList[i])

    def output2(self, outputList, infoList):
        """函数功能：输出结果信息"""
        # 获取参数信息
        """
        sensorKindName = outputList[0]
        sensorValue = outputList[1]
        diffValue = outputList[2]
        speedValue = outputList[3]
        warningType = outputList[4]
        warningDesc = outputList[5]
        modelType = outputList[6]

        print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
        self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, sensorKindName, sensorValue, diffValue,
        speedValue, warningType, warningDesc, modelType, self.fTime)
        """
        sensorTypeL = {'1位轴端': '1', '2位轴端': '2', '3位轴端': '3', '4位轴端': '4',
                       '5位轴端': '5', '6位轴端': '6', '7位轴端': '7', '8位轴端': '8',
                       '1轴小齿轮箱电机侧': '9', '2轴小齿轮箱电机侧': '10', '3轴小齿轮箱电机侧': '11', '4轴小齿轮箱电机侧': '12',
                       '1轴小齿轮箱车轮侧': '13', '2轴小齿轮箱车轮侧': '14', '3轴小齿轮箱车轮侧': '15', '4轴小齿轮箱车轮侧': '16',
                       '1轴电机定子': '17', '1轴电机传动端': '18', '1轴电机非传动端': '19',
                       '2轴电机定子': '20', '2轴电机传动端': '21', '2轴电机非传动端': '22',
                       '3轴电机定子': '23', '3轴电机传动端': '24', '3轴电机非传动端': '25',
                       '4轴电机定子': '26', '4轴电机传动端': '27', '4轴电机非传动端': '28',
                       '1轴大齿轮箱电机侧': '29', '2轴大齿轮箱电机侧': '30', '3轴大齿轮箱电机侧': '31', '4轴大齿轮箱电机侧': '32',
                       '1轴大齿轮箱车轮侧': '33', '2轴大齿轮箱车轮侧': '34', '3轴大齿轮箱车轮侧': '35', '4轴大齿轮箱车轮侧': '36'}

        # 获取参数信息
        sensorKind = outputList[0]
        sensorKindName = outputList[1]
        sensorValueList = outputList[2]
        sensorValue = outputList[3]
        diffValue = outputList[4]
        speedValue = outputList[5]
        warningType = outputList[6]
        warningDesc = outputList[7]
        modelType = outputList[8]
        sensorType = sensorTypeL[sensorKindName.split(':')[-1]]
        sameSideVal = ','.join([str(item) for item in sensorValueList])

        trainTypeCode = infoList[0]
        trainNo = infoList[1]
        coachId = infoList[2]
        trainSpeed = infoList[3]
        out_degree = infoList[4]
        trainsite_id = infoList[5]
        brake_pos = infoList[6]
        outputTime = infoList[7]

        prejudgeDesc = ''
        preWarnDesc = ''
        warnDesc = ''

        typeinfo = warningDesc[:2]

        warn_Key = self.trainNo + '_' + self.coachId + '_' + warningType + '_' + self.model_type_code + '_' + sensorType + '_' + typeinfo

        # 如果以前没有报过警 则进行报警输出
        flag = 0

        # 如果以前没有报过警 则进行报警输出
        if outputTime not in self.warmDict.keys():
            self.warmDict[outputTime] = {warn_Key: ''}
            flag = 1
        elif warn_Key not in self.warmDict[outputTime].keys():
            self.warmDict[outputTime][warn_Key] = ''
            flag = 1
        else:
            flag = 0

        if flag == 1:
            if warningType == 'YP':
                prejudgeDesc = warningType + ':' + warningDesc
            elif warningType == 'YJ':
                preWarnDesc = warningType + ':' + warningDesc
            elif warningType == 'BJ':
                warnDesc = warningType + ':' + warningDesc

            # print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
            #     trainTypeCode, trainNo, coachId, trainSpeed, out_degree, trainsite_id,
            #     brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '', diffValue,
            #     speedValue,
            #     '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime)

            tmpList = [trainTypeCode, trainNo, coachId, trainSpeed, out_degree, trainsite_id,
                       brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '', diffValue,
                       speedValue,
                       '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime]

            # sd.saveToFile(, tmpList)
            kazooModel.saveDataToHive(tmpList)

            tmpList2 = [trainTypeCode, trainNo, coachId, trainSpeed, trainsite_id, sensorKind, sensorKindName,
                        sensorType, sensorValue, sameSideVal, prejudgeDesc, preWarnDesc, warnDesc, modelType,
                        outputTime]

            # sd.updateData(tmpList2)

            # 控制 self.warmDict1的内存
            if len(self.warmDict.keys()) > 200:
                timeList = sorted(self.warmDict.keys())
                for i in range(len(timeList) - 20):
                    self.warmDict.pop(timeList[i])


class onGroundModel:
    # 传感器位置
    sensorTypeList = [[['1', '1位轴端'], ['1', '3位轴端'], ['1', '5位轴端'], ['1', '7位轴端']],
                      [['2', '2位轴端'], ['2', '4位轴端'], ['2', '6位轴端'], ['2', '8位轴端']],
                      [['3', '1轴小齿轮箱电机侧'], ['3', '2轴小齿轮箱电机侧'], ['3', '3轴小齿轮箱电机侧'], ['3', '4轴小齿轮箱电机侧']],
                      [['4', '1轴小齿轮箱车轮侧'], ['4', '2轴小齿轮箱车轮侧'], ['4', '3轴小齿轮箱车轮侧'], ['4', '4轴小齿轮箱车轮侧']],
                      [['7', '1轴电机定子'], ['7', '2轴电机定子'], ['7', '3轴电机定子'], ['7', '4轴电机定子']],
                      [['8', '1轴电机传动端'], ['8', '2轴电机传动端'], ['8', '3轴电机传动端'], ['8', '4轴电机传动端']],
                      [['9', '1轴电机非传动端'], ['9', '2轴电机非传动端'], ['9', '3轴电机非传动端'], ['9', '4轴电机非传动端']],
                      [['5', '1轴大齿轮箱电机侧'], ['5', '2轴大齿轮箱电机侧'], ['5', '3轴大齿轮箱电机侧'], ['5', '4轴大齿轮箱电机侧']],
                      [['6', '1轴大齿轮箱车轮侧'], ['6', '2轴大齿轮箱车轮侧'], ['6', '3轴大齿轮箱车轮侧'], ['6', '4轴大齿轮箱车轮侧']]
                      ]
    model_type_code = 'm2'
    warmDict = {}

    def doData(self, d):
        """函数功能：数据清洗和转换"""
        # 增加数据标志
        addDataFlag = 0
        flag30 = 0
        flag60 = 0

        global dataSet3

        # 获取基础数据信息
        trainTypeCode, trainNo, coachId, fTime, trainSpeed = d[36], d[37], d[38], d[39], d[40]

        self.out_degree, self.trainsite_id, self.brake_pos = d[41], d[42], d[43]

        if len(coachId) < 2:
            coachId = '0' + coachId

        dataId = trainTypeCode + '_' + trainNo + '_' + coachId  # 确定数据记录ID

        # 获取轴温数据
        # sv = [int(item) for item in d[4:40]]
        sv = []
        for item in d[0:36]:
            if item.split('-')[-1].isdigit():
                v = int(item)
            else:
                v = 999
            sv.append(v)

        # 同侧轴温列表
        sensorValueList = [[sv[0], sv[2], sv[4], sv[6]],  # 轴箱1
                           [sv[1], sv[3], sv[5], sv[7]],  # 轴箱2
                           [sv[8], sv[9], sv[10], sv[11]],  # 小齿轮箱电机侧
                           [sv[12], sv[13], sv[14], sv[15]],  # 小齿轮箱车轮侧
                           [sv[16], sv[19], sv[22], sv[25]],  # 电机定子
                           [sv[17], sv[20], sv[23], sv[26]],  # 电机传动端
                           [sv[18], sv[21], sv[24], sv[27]],  # 电机非传动端
                           [sv[28], sv[29], sv[30], sv[31]],  # 大齿轮箱电机侧
                           [sv[32], sv[33], sv[34], sv[35]]  # 大齿轮箱车轮侧
                           ]

        coachType = 'M'
        if coachType == 'M':
            num = 9
        elif coachType == 'T':  # 如果是拖车, 则只处理轴端
            num = 2

        self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.fTime = trainTypeCode, trainNo, coachId, trainSpeed, fTime

        #########################################################
        # 准备平滑数据
        if len(dataSet3) > 0:
            # 判断值是否已经在字典表里存在
            existsKey = [item for item in dataSet3 if item == dataId]
            if len(existsKey) > 0:
                # 获取时间值
                t1 = dataSet3[dataId][-1][0]
                t2 = fTime

                # 计算时间差(单位：秒)
                diffTime = int(t2.split(' ')[1].split(':')[0]) * 60 * 60 + int(
                    t2.split(' ')[1].split(':')[1]) * 60 + int(t2.split(' ')[1].split(':')[2]) - int(
                    t1.split(' ')[1].split(':')[0]) * 60 * 60 - int(t1.split(' ')[1].split(':')[1]) * 60 - int(
                    t1.split(' ')[1].split(':')[2])

                # 如果数据时间间隔超过80s，则认为断2包及以上,重新添加数据
                if diffTime >= 80 or diffTime < 0:
                    addDataFlag = 1
                # 断包以60s计算为主
                elif diffTime >= 40 and diffTime < 80:
                    dataSet3[dataId].append([fTime, sensorValueList])
                    flag60 = 1
                # 不断包以30s计算为主
                elif diffTime >= 20 and diffTime < 40:
                    dataSet3[dataId].append([fTime, sensorValueList])
                    flag30 = 1
            else:
                addDataFlag = 1
        else:
            addDataFlag = 1

        if addDataFlag == 1:
            dataSet3.clear()
            dataSet3[dataId] = [[fTime, sensorValueList]]

        # 获取数据包
        if addDataFlag == 0:
            # 齿轮箱、轴箱的温升斜率计算
            # 处理30秒平滑
            if len(dataSet3[dataId]) >= 2:
                # 获取第1包和第2包数据
                p1 = dataSet3[dataId][-2]
                p2 = dataSet3[dataId][-1]

        #########################################################
        for i in xrange(0, num):
            if 999 not in sensorValueList[i]:
                self.fTime = fTime

                # 调用温差计算函数
                # 有顶棚值
                self.calDiffLimit(i, sensorValueList[i])

                # 已去掉顶棚值
                # self.calDiffNoLimit(i, sensorValueList[i])

            # 计算温升斜率
            if flag30 == 1:
                calFlag = 1  # 判断温度值是否有999的值，如果没有，则开始计算
                if 999 in p1[1][i] or 999 in p2[1][i]:
                    calFlag = 0
                if calFlag == 1:
                    # 计算第一个位置温升斜率
                    s1 = p2[1][i][0]
                    s11 = p1[1][i][0]
                    s12 = p2[1][i][0]
                    sp1 = 2 * (s12 - s11)

                    # 计算第二个位置温升斜率
                    s2 = p2[1][i][1]
                    s21 = p1[1][i][1]
                    s22 = p2[1][i][1]
                    sp2 = 2 * (s22 - s21)

                    # 计算第三个位置温升斜率
                    s3 = p2[1][i][2]
                    s31 = p1[1][i][2]
                    s32 = p2[1][i][2]
                    sp3 = 2 * (s32 - s31)

                    # 计算第四个位置温升斜率
                    s4 = p2[1][i][3]
                    s41 = p1[1][i][3]
                    s42 = p2[1][i][3]
                    sp4 = 2 * (s42 - s41)

                    # 获取报警时间
                    self.fTime = dataSet3[dataId][-1][0]

                    # 调用规则函数进行判断
                    # 有顶棚值
                    self.calSpeedLimit(30, i, [s1, s2, s3, s4], [sp1, sp2, sp3, sp4])

                    # 已去掉顶棚值
                    # self.calSpeedNoLimit(30, i, [s1, s2, s3, s4], [sp1, sp2, sp3, sp4])

            # 1分钟平滑
            elif flag60 == 1:
                calFlag = 1  # 判断温度值是否有999的值，如果没有，则开始计算
                if 999 in p1[1][i] or 999 in p2[1][i]:
                    calFlag = 0

                if calFlag == 1:
                    # 计算第一个位置温升斜率
                    s1 = p2[1][i][0]
                    s11 = p1[1][i][0]
                    s12 = p2[1][i][0]
                    sp1 = s12 - s11

                    # 计算第二个位置温升斜率
                    s2 = p2[1][i][1]
                    s21 = p1[1][i][1]
                    s22 = p2[1][i][1]
                    sp2 = s22 - s21

                    # 计算第三个位置温升斜率
                    s3 = p2[1][i][2]
                    s31 = p1[1][i][2]
                    s32 = p2[1][i][2]
                    sp3 = s32 - s31

                    # 计算第四个位置温升斜率
                    s4 = p2[1][i][3]
                    s41 = p1[1][i][3]
                    s42 = p2[1][i][3]
                    sp4 = s42 - s41

                    # 获取报警时间
                    self.fTime = dataSet3[dataId][-1][0]

                    # 调用规则函数进行判断
                    # 有顶棚值
                    self.calSpeedLimit(60, i, [s1, s2, s3, s4], [sp1, sp2, sp3, sp4])

                    # 已去掉顶棚值
                    # self.calSpeedNoLimit(60, i, [s1, s2, s3, s4], [sp1, sp2, sp3, sp4])

    def calSpeedLimit(self, interval, seq, sensorValueList, speedList):
        """函数功能：计算温升速率"""
        #########################################
        # 轴箱、齿轮箱温升斜率计算
        if seq in (0, 1, 2, 3, 7, 8):
            modelType = '地面模型_转开'
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

                # 轴箱 无温升预判
                # 齿轮箱
                if seq in (2, 3, 7, 8):
                    if self.trainTypeCode in sensorB[0]:
                        # 预判
                        preJudgeSpeed = 8
                        if speedList[i] >= preJudgeSpeed:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按' + str(interval) + '秒平滑计算）'

                    elif self.trainTypeCode in sensorB[1]:
                        # 预判
                        preJudgeSpeed = 10
                        if speedList[i] >= preJudgeSpeed:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按' + str(interval) + '秒平滑计算）'

                if len(warningDesc) > 0 and warningType == 'YP':
                    # 输出结果
                    self.output(
                        # [sensorKindName, sensorValueList[i], 0, speedList[i], warningType, warningDesc, modelType])
                        [sensorKind, '轴温温升:' + sensorKindName, sensorValueList, sensorValueList[i], 0, speedList[i],
                         warningType,
                         warningDesc, modelType])

        #########################################
        # 牵引电机温升斜率计算
        if seq in (4, 5, 6):
            modelType = '地面模型_电开'
            for i in xrange(0, 4):
                warningType = ''  # 告警类型
                warningDesc = ''  # 告警信息
                sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
                sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

                if seq == 5:
                    if self.trainTypeCode in sensorC[0]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 60
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按' + str(
                                interval) + '秒平滑计算）,同时温度值≥' + str(preJudgeValue) + '℃'

                elif seq == 4:
                    if self.trainTypeCode in sensorD[0]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 90
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按' + str(
                                interval) + '秒平滑计算）,同时温度值≥' + str(preJudgeValue) + '℃'
                    elif self.trainTypeCode in sensorD[1]:
                        # 预判
                        preJudgeSpeed = 13
                        preJudgeValue = 90
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按' + str(
                                interval) + '秒平滑计算）,同时温度值≥' + str(preJudgeValue) + '℃'

                elif seq == 6:
                    if self.trainTypeCode in sensorE[0]:
                        # 预判
                        preJudgeSpeed = 8
                        preJudgeValue = 40
                        if speedList[i] >= preJudgeSpeed and sensorValueList[i] >= preJudgeValue:
                            warningType = 'YP'
                            warningDesc = '温升速率≥' + str(preJudgeSpeed) + '℃/min（按' + str(
                                interval) + '秒平滑计算）,同时温度值≥' + str(preJudgeValue) + '℃'

                if len(warningDesc) > 0 and warningType == 'YP':
                    # 输出结果
                    self.output(
                        # [sensorKindName, sensorValueList[i], 0, speedList[i], warningType, warningDesc, modelType])
                        [sensorKind, '轴温温升:' + sensorKindName, sensorValueList, sensorValueList[i], 0, speedList[i],
                         warningType,
                         warningDesc, modelType])

    def calDiffLimit(self, seq, sensorValueList):
        """函数功能：计算温差"""

        diff1 = sensorValueList[0] - sum(sensorValueList[1:4]) / 3
        diff2 = sensorValueList[1] - (sensorValueList[0] + sum(sensorValueList[2:4])) / 3
        diff3 = sensorValueList[2] - (sum(sensorValueList[0:2]) + sensorValueList[3]) / 3
        diff4 = sensorValueList[3] - sum(sensorValueList[0:3]) / 3
        diffList = [diff1, diff2, diff3, diff4]

        for i in xrange(0, 4):
            warningType = ''  # 告警类型
            warningDesc = ''  # 告警信息
            sensorKind = self.sensorTypeList[seq][i][0]  # 传感器位置编码
            sensorKindName = self.sensorTypeList[seq][i][1]  # 传感器位置

            # 轴箱、齿轮箱温差计算
            if seq in (0, 1, 2, 3, 7, 8):
                modelType = '地面模型_转开'

                # 轴箱
                if seq in (0, 1):
                    if self.trainTypeCode in sensorA[0]:
                        preJudgeVal = 100  # 预判温度阀值
                        preJudgeDiff = [40, 50]  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                    elif self.trainTypeCode in sensorA[1]:
                        preJudgeVal = 90  # 预判温度阀值
                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                # 齿轮箱
                elif seq in (2, 3, 7, 8):
                    if self.trainTypeCode in sensorB[0]:
                        preJudgeVal = 115  # 预判温度阀值
                        preJudgeDiff = [35, 40]  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                    elif self.trainTypeCode in sensorB[1]:
                        preJudgeVal = 115  # 预判温度阀值
                        preJudgeDiff = [35, 40]  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff[0] and diffList[i] < preJudgeDiff[1]:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff[0]) + '℃小于' + str(preJudgeDiff[1]) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                # if len(warningDesc) > 0 and warningType == 'YP':
                #     # 输出结果
                #     self.output(
                #         # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                #         [sensorKind, sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0, warningType,
                #          warningDesc, modelType])

            # 牵引电机温差计算
            elif seq in (4, 5, 6):
                modelType = '地面模型_电开'

                if seq == 5:
                    if self.trainTypeCode in sensorC[0]:
                        preJudgeVal = 105  # 预判温度阀值
                        preJudgeDiff = 35  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                elif seq == 4:
                    if self.trainTypeCode in sensorD[0]:
                        preJudgeVal = 180  # 预判温度阀值
                        preJudgeDiff = 35  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            # 输出结果
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            # 输出结果
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                    elif self.trainTypeCode in sensorD[1]:
                        preJudgeVal = 175  # 预判温度阀值
                        preJudgeDiff = 35  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            # 输出结果
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            # 输出结果
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])
                elif seq == 6:
                    if self.trainTypeCode in sensorE[0]:
                        preJudgeVal = 85  # 预判温度阀值
                        preJudgeDiff = 13  # 温差阀值

                        if sensorValueList[i] >= preJudgeVal:
                            warningType = 'YP'
                            warningDesc = '温度达到' + str(preJudgeVal) + '℃'
                            # 输出结果
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温阈值:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                        if diffList[i] >= preJudgeDiff:
                            warningType = 'YP'
                            warningDesc = '同车同侧温度差值大于等于' + str(preJudgeDiff) + '℃'
                            # 输出结果
                            self.output(
                                # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                                [sensorKind, '轴温温差:' + sensorKindName, sensorValueList, sensorValueList[i], diffList[i],
                                 0,
                                 warningType,
                                 warningDesc, modelType])

                # if len(warningDesc) > 0 and warningType == 'YP':
                #     # 输出结果
                #     self.output(
                #         # [sensorKindName, sensorValueList[i], diffList[i], 0, warningType, warningDesc, modelType])
                #         [sensorKind, sensorKindName, sensorValueList, sensorValueList[i], diffList[i], 0, warningType,
                #          warningDesc, modelType])

    def output(self, outputList):
        """函数功能：输出结果信息"""
        """
        # 获取参数信息
        sensorKindName = outputList[0]
        sensorValue = outputList[1]
        diffValue = outputList[2]
        speedValue = outputList[3]
        warningType = outputList[4]
        warningDesc = outputList[5]
        modelType = outputList[6]

        print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
        self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, sensorKindName, sensorValue, diffValue,
        speedValue, warningType, warningDesc, modelType, self.fTime)
        """

        sensorTypeL = {'1位轴端': '1', '2位轴端': '2', '3位轴端': '3', '4位轴端': '4',
                       '5位轴端': '5', '6位轴端': '6', '7位轴端': '7', '8位轴端': '8',
                       '1轴小齿轮箱电机侧': '9', '2轴小齿轮箱电机侧': '10', '3轴小齿轮箱电机侧': '11', '4轴小齿轮箱电机侧': '12',
                       '1轴小齿轮箱车轮侧': '13', '2轴小齿轮箱车轮侧': '14', '3轴小齿轮箱车轮侧': '15', '4轴小齿轮箱车轮侧': '16',
                       '1轴电机定子': '17', '1轴电机传动端': '18', '1轴电机非传动端': '19',
                       '2轴电机定子': '20', '2轴电机传动端': '21', '2轴电机非传动端': '22',
                       '3轴电机定子': '23', '3轴电机传动端': '24', '3轴电机非传动端': '25',
                       '4轴电机定子': '26', '4轴电机传动端': '27', '4轴电机非传动端': '28',
                       '1轴大齿轮箱电机侧': '29', '2轴大齿轮箱电机侧': '30', '3轴大齿轮箱电机侧': '31', '4轴大齿轮箱电机侧': '32',
                       '1轴大齿轮箱车轮侧': '33', '2轴大齿轮箱车轮侧': '34', '3轴大齿轮箱车轮侧': '35', '4轴大齿轮箱车轮侧': '36'}

        # 获取参数信息
        outputTime = self.fTime
        sensorKind = outputList[0]
        sensorKindName = outputList[1]
        sensorValueList = outputList[2]
        sensorValue = outputList[3]
        diffValue = outputList[4]
        speedValue = outputList[5]
        warningType = outputList[6]
        warningDesc = outputList[7]
        modelType = outputList[8]
        sensorType = sensorTypeL[sensorKindName.split(':')[-1]]
        sameSideVal = ','.join([str(item) for item in sensorValueList])

        prejudgeDesc = ''
        preWarnDesc = ''
        warnDesc = ''

        typeinfo = warningDesc[:2]

        warn_Key = self.trainNo + '_' + self.coachId + '_' + warningType + '_' + self.model_type_code + '_' + sensorType + '_' + typeinfo

        # 如果以前没有报过警 则进行报警输出
        flag = 0

        # 如果以前没有报过警 则进行报警输出
        if outputTime not in self.warmDict.keys():
            self.warmDict[outputTime] = {warn_Key: ''}
            flag = 1
        elif warn_Key not in self.warmDict[outputTime].keys():
            self.warmDict[outputTime][warn_Key] = ''
            flag = 1
        else:
            flag = 0

        if flag == 1:
            if warningType == 'YP':
                prejudgeDesc = warningType + ':' + warningDesc
            elif warningType == 'YJ':
                preWarnDesc = warningType + ':' + warningDesc
            elif warningType == 'BJ':
                warnDesc = warningType + ':' + warningDesc

            # print '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s' % (
            #     self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.out_degree, self.trainsite_id,
            #     self.brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '', diffValue,
            #     speedValue,
            #     '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime)

            tmpList = [self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.out_degree,
                       self.trainsite_id,
                       self.brake_pos, sensorKind, sensorKindName, sensorType, sensorValue, '', sameSideVal, '',
                       diffValue,
                       speedValue,
                       '', prejudgeDesc, preWarnDesc, warnDesc, modelType, outputTime]

            # sd.saveToFile(, tmpList)
            kazooModel.saveDataToHive(tmpList)

            tmpList2 = [self.trainTypeCode, self.trainNo, self.coachId, self.trainSpeed, self.trainsite_id, sensorKind,
                        sensorKindName, sensorType, sensorValue, sameSideVal, prejudgeDesc, preWarnDesc, warnDesc,
                        modelType, outputTime]

            # sd.updateData(tmpList2)

            # 控制 self.warmDict1的内存
            if len(self.warmDict.keys()) > 200:
                timeList = sorted(self.warmDict.keys())
                for i in range(len(timeList) - 20):
                    self.warmDict.pop(timeList[i])


dataDict = dict()
fileTime = None


class kazooClient:
    keySet = set()
    nameNodeA = [b'phmnn1.bigdata.com', 'phmnn1.bigdata.com']
    nameNodeB = [b'phmnn2.bigdata.com', 'phmnn2.bigdata.com']

    nameNodeADevelop = [b'spark.bigdevelop.com', 'spark.bigdevelop.com']
    nameNodeBDevelop = [b'nnredis.bigdevelop.com', 'nnredis.bigdevelop.com']

    zooQuorum = 'phmkfk1.bigdata.com:2181,phmkfk2.bigdata.com:2181,phmkfk3.bigdata.com:2181'
    zooQuorumDevelop = 'datanode1.bigdevelop.com:2181,schedule.bigdevelop.com:2181,datanode2.bigdevelop.com:2181'

    path = '/hadoop-ha/PHMBIGDATA/ActiveBreadCrumb'
    pathDevelop = '/hadoop-ha/hanamenode/ActiveBreadCrumb'

    def __init__(self):
        try:
            t0 = time.clock()
            self.zk = KazooClient(hosts=self.zooQuorum)
            self.zk.start()
            if self.zk.exists(self.path):
                data = self.zk.get(self.path)
                ip = None
                if self.nameNodeA[0] in data[0]:
                    ip = 'http://' + self.nameNodeA[1] + ':50070'
                elif self.nameNodeB[0] in data[0]:
                    ip = 'http://' + self.nameNodeB[1] + ':50070'

                if ip:
                    self.client = hdfs.Client(ip, root='/')
                    if not self.client:
                        print 'hdfs client 建立连接失败!'
                else:
                    print 'nameNode IP 获取失败!'
            t1 = time.clock()
            # print 'init 耗时:',t1-t0
        except Exception as e:
            print 'kazooClient __init__ ERROR!'
            print traceback.format_exc()

    def readData(self):
        global fileTime
        fileTime = 1567654337316
        try:
            t0 = time.clock()
            op_month = datetime.datetime.now().strftime('%Y%m')
            op_day = datetime.datetime.now().strftime('%Y%m%d')
            # dirPath = '/apps/hive/warehouse/dm_i32_sensor_online_dm3/op_month=' + op_month + '/op_day=' + op_day
            dirPath = '/apps/hive/warehouse/dm_i32_sensor_online_dm3/op_month=201909/op_day=20190905'
            print 'dirPaath:',dirPath
            fn = dict()  # key为文件时间,value为文件路径
            status = self.client.status(dirPath, False)
            if status:
                for root, dir, files in self.client.walk(dirPath, depth=1, status=True):
                    for fileName, infoDict in files:
                        # print(fileName,infoDict['modificationTime'])
                        if '_tmp' not in fileName:
                            filePath = dirPath + '/' + fileName
                            file_time = infoDict['modificationTime']
                            if fileTime:
                                if fileTime <= file_time:
                                    fn[file_time] = filePath
                            else:
                                fn[file_time] = filePath
            else:
                print 'hdfs目录获取失败:',dirPath,' ，程序重启，',datetime.datetime.now()
                sys.exit(0)

            if len(fn.keys()) > 0:
                if fileTime:
                    fKey = sorted(fn.keys())
                else:
                    fKey = sorted(fn.keys())[-10:]

                xList=[]
                length=len(fKey)
                fileTime = fKey[-1]
                format_date = datetime.datetime.fromtimestamp(float(fileTime) / 1000)
                print 'cur_fileTime:', fileTime, ',format_date:', format_date, ',cur_file:', fn[fileTime]

                s=0
                e=0
                for i in range(length):
                    s+=i
                    e=s+20
                    if e >= length-1:
                        e=length-1
                        xList.append([s,e])
                        break
                    xList.append([s,e])
                    s=e-1

                for x in xList:
                    s,e=x
                    for k in fKey[s:e]:
                        f = fn[k]
                        with self.client.read(f, encoding='utf-8') as reader:
                            dataString = reader.read()
                            lines = dataString.split('\n')
                            for line in lines:
                                if not line:
                                    break
                                d = str(line).strip().split(',')
                                self.dataProcess(d)
                    print s,e,length,'start getResult',datetime.datetime.now()
                    self.getResult()

        except Exception as e:
            print 'kazooClient 数据处理 ERROR!'
            print traceback.format_exc()

    def dataProcess(self, d):
        global dataDict

        out_degree = ''
        train_type_code, ftime, train_no, trainsite_valid, trainsite_id, speed, brake_pos, coach_id, bd_degree, jy_degree = d[
                                                                                                                            2:12]

        if train_type_code in ('E32', 'E32B', 'E32C', 'E44', 'E51'):
            out_degree = bd_degree
        else:
            out_degree = jy_degree

        if train_no not in trainBlackList:
            sensor_status1 = d[12:48]
            sensor_value1 = d[84:120]
            # if train_no =='2917':
            #     if coach_id =='01':
            #         print ftime
            # fTime = ftime[0:4] + '-' + ftime[4:6] + '-' + ftime[6:8] + ' ' + ftime[
            #                                                                  8:10] + ':' + ftime[
            #                                                                                10:12] + ':' + ftime[
            #                                                                                               12:14]
            fTime = ftime

            for i in range(0, 36):
                sensor_value1[i] = sensor_value1[i] if sensor_value1[i].strip().split('.')[
                    0].isdigit() else '999'
                if sensor_status1[i] in (
                        '故障', '报警', '预警', '阈值报警', '温差值报警', '温升值报警', '无效', '阈值预警', '温差值预警',
                        '温升值预警',
                        '临时故障',
                        '温差报警',
                        '温升报警', '温差预警', '温升预警', '进行警惕试验', '关', '备用', '备用2'):
                    sensor_value1[i] = '999'

            dd = sensor_value1 + [train_type_code, train_no, coach_id, fTime, speed, out_degree,
                                  trainsite_id, brake_pos]

            trainKey = train_type_code + '_' + train_no + '_' + coach_id
            self.keySet.add(trainKey)
            if trainKey not in dataDict:
                dataDict[trainKey] = {fTime: dd}
            elif fTime not in dataDict[trainKey]:
                dataDict[trainKey][fTime] = dd

    def getResult(self):
        global dataDict
        t0 = time.clock()
        for trainKey in self.keySet:
            if trainKey in dataDict:
                sortTimeList = sorted(dataDict[trainKey].keys())
                lastTime = sortTimeList[-1]
                # 标动有的10s一包,将其分拆成三份数据,处理成以前30s一包的格式
                if 'CR400AF' in trainKey:
                    sortTimeList = self.doTime(sortTimeList)
                for t in sortTimeList:
                    data = dataDict[trainKey][t]
                    # 调用阈值模型标准动车车型
                    if data[36] in ('E32', 'E32A', 'E32B', 'E44', 'E51'):
                        tm.transData(data)
                    else:
                        # 调用地面模型
                        og.doData(data)
                        # 调用滤波模型
                        om.doData(data)
                    # 调用融合模型
                    compM.getMinData(data)
                    last_ts = int(time.mktime(time.strptime(lastTime, "%Y-%m-%d %H:%M:%S")))
                    cur_ts = int(time.mktime(time.strptime(t, "%Y-%m-%d %H:%M:%S")))
                    timeDiff = last_ts - cur_ts
                    # 清除历史数据
                    if timeDiff >= 180:  # 超过3分钟的数据删除
                        dataDict[trainKey].pop(t)

        self.keySet.clear()
        t1 = time.clock()
        # print 'getResult 耗时:', t1 - t0

    def __del__(self):
        try:
            if self.zk:
                self.zk.stop()
                self.zk.close()
            if self.client:
                del (self.client)
        except Exception as e:
            print 'kazooClient __del__ ERROR!'
            print traceback.format_exc()
            sys.exit(1)

    def doTime(self, timeList):
        aList = list()
        bList = list()
        cList = list()
        for t in timeList:
            tail = t[-2:]
            if (tail >= '00' and tail <= '05') or (tail > '25' and tail <= '35') or (tail > '55' and tail <= '59'):
                aList.append(t)
            elif (tail > '05' and tail <= '15') or (tail > '35' and tail <= '45'):
                bList.append(t)
            elif (tail > '15' and tail <= '25') or (tail > '45' and tail <= '55'):
                cList.append(t)

        return aList + bList + cList

    def saveDataToHive(self, putList):
        try:
            dataTime = putList[-1]
            dataTime = dataTime[0:4] + dataTime[5:7] + dataTime[8:10] + dataTime[11:13] + '0000.dat'
            op_month = dataTime[0:6]
            op_day = dataTime[0:8]
            contents = '\t'.join([str(item) for item in putList]) + '\n'
            print contents
            fileName = 'wtds_i32_sensor_model_' + str(dataTime)
            fileDir_month = '/apps/hive/warehouse/dm_i32_sensor_online_model_dm_20181109/op_month=' + str(
                op_month) + '/'
            fileDir_day = '/apps/hive/warehouse/dm_i32_sensor_online_model_dm_20181109/op_month=' + str(
                op_month) + '/op_day=' + str(op_day) + '/'

            hdfsPath = fileDir_day + fileName
            if not self.client.status(fileDir_month, strict=False):
                self.client.makedirs(fileDir_month)
            if not self.client.status(fileDir_day, strict=False):
                self.client.makedirs(fileDir_day)
            if not self.client.status(hdfsPath, strict=False):
                self.client.write(hdfsPath, contents, overwrite=True)
            else:
                self.client.write(hdfsPath, contents, append=True)
        except Exception as e:
            print '模型结果上传至hive失败:', e
            print traceback.format_exc()


if __name__ == '__main__':
    # 自动调度
    if len(sys.argv) == 1:
        diffDate = lambda fTime, oldFTime: int(fTime.split(' ')[0].replace('-', '')) - int(
            oldFTime.split(' ')[0].replace('-', ''))
        diffSec = lambda fTime, oldFTime: int(fTime.split(' ')[1].split(':')[0]) * 60 * 60 + int(
            fTime.split(' ')[1].split(':')[1]) * 60 + int(
            fTime.split(' ')[1].split(':')[2]) - int(
            oldFTime.split(' ')[1].split(':')[0]) * 60 * 60 - int(
            oldFTime.split(' ')[1].split(':')[1]) * 60 - int(
            oldFTime.split(' ')[1].split(':')[2])

        # 定义对象
        compM = onlineCompModel()  # 融合
        om = onTrainModel()  # 滤波
        og = onGroundModel()  # 地面
        tm = onlineThresholdModel()  # 标动
        kazooModel = kazooClient()
        print '轴温模型脚本启动时间:', datetime.datetime.now()
        while True:
            try:
                print '本轮开始时间:', datetime.datetime.now()
                startTime = time.clock()
                sd = SaveData()
                db = MySQLData()
                trainMroStatus = db.queryTrainMroStatus()
                kazooModel.readData()
                del (db)
                del (sd)
                endTime = time.clock()
                print '本轮计算耗时:', endTime - startTime, ' 秒'
                time.sleep(3)
                sys.exit(0)
            except Exception as e:
                del (kazooModel)
                kazooModel = kazooClient()
                print '===========>> ERROR:', e, traceback.format_exc()

    # 程序调试
    if len(sys.argv) == 2 and sys.argv[1].lower() == 'debug':
        # 定义对象
        compM = onlineCompModel()
        tm = onlineThresholdModel()
        om = onTrainModel()
        og = onGroundModel()

        for line in sys.stdin:
            # dn = []
            d = line.strip().split('\t')
            # # 数据顺序转换：原始文件顺序转换成HIVE执行顺序
            # dn[0:36] = d[7:43]
            # dn.extend(d[0:3])
            # dn.append(d[43])
            # dn.append(d[3])
            # dn.append(d[4])
            # dn.append(d[5])
            # dn.append(d[6])

            # # 调用融合模型
            # compM.getMinData(d)

            # 调用阈值模型标准动车车型,
            if d[36] in ('E32', 'E32B', 'E32C', 'E44', 'E51'):
                tm.transData(d)
            else:
                # 调用滤波模型
                om.doData(d)

                # 调用地面模型
                og.doData(d)
