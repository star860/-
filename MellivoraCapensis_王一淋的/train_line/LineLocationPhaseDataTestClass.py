# -*- coding: utf-8 -*-

import sys
import datetime
import traceback
from db_hive_conn import HiveData
from db_mysql_conn import MySQLData
from db_hbase_conn import HBaseData
from data_util import dataModel


class Model():
    """过分相数据验证"""

    def __init__(self, open=True):
        """
        初始化连接
        :param open: 是否连接数据库
        """
        try:
            self.dataUtil = dataModel()
            if open is True:
                self.hive_db = HiveData()
                self.mysql_db = MySQLData()
                self.hbase_db = HBaseData()
        except Exception:
            print('%s 数据库连接异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit()

    def runTestPasheData(self, train_line_data, day_start, day_stop, phaseData):
        """
        运行接触网过分相数据程序,根据列车经过接触网分相时,网压的变化规律,找到对应时刻的里程,并记录到mysql数据库中,
        后续用于与静态工务数据做对比验证
        :param line_name:线路名称
        :param direction:行别
        :return:
        """

        line_code, line_direction_code, station_start, station_end,full_length = train_line_data
        line_keyId = line_code + '-' + line_direction_code

        op_day = datetime.datetime.now() - datetime.timedelta(days=1)
        op_day = op_day.strftime('%Y%m%d')
        info = self.hive_db.query_open_string_info_by_station(op_day, station_start, station_end)
        count = 0
        length = len(info)
        for keyId in info:
            count += 1
            if count >= 0:
                trainNo = keyId.split('_')[0]
                trainsite_id, op_day, start_time, end_time, start_station, end_station, run_time, run_long = \
                    info[
                        keyId]
                if op_day >= day_start and op_day <= day_stop:
                    print(count, length, trainNo, trainsite_id, op_day, start_time, end_time, start_station,
                          end_station, run_time, run_long)

                    s_time = self.dataUtil.change_time_format(op_day + start_time, '%Y-%m-%d%H:%M', '%Y%m%d%H%M00')
                    if end_time > start_time:
                        e_time = self.dataUtil.change_time_format(op_day + end_time, '%Y-%m-%d%H:%M',
                                                                  '%Y%m%d%H%M00',
                                                                  delta_minutes=5, delta_type='addition')
                    else:
                        e_time = self.dataUtil.change_time_format(op_day + end_time, '%Y-%m-%d%H:%M',
                                                                  '%Y%m%d%H%M00', delta_days=1,
                                                                  delta_minutes=5, delta_type='addition')
                    interface_code = '32'
                    for coach in ('02', '04', '05', '07'):
                        rowStart = interface_code + '_' + trainNo + '_' + coach + '_' + s_time
                        rowStop = interface_code + '_' + trainNo + '_' + coach + '_' + e_time
                        hbase_data_32 = self.hbase_db.queryTrainData(rowStart, rowStop, interface_code)

                        self.findPhasePosition(line_keyId,coach, hbase_data_32, phaseData)

    def findPhasePosition(self,line_keyId, coach, dataDict, phaseDict):
        """
        通过该车厢找到经过接触网分相时的里程数据,与电务数据做关联,存储到mysql表中
        :param line_keyId:线路编号行别编号ID
        :param coach:车厢号
        :param dataDict:车厢对应的字典数据
        :param phaseDict:接触网分相工务字典数据
        :return:
        """
        coach_length=25
        phase_valid = 0
        line_code, diretion_code = line_keyId.split('-')
        if dataDict:
            for train in dataDict:
                timelist = sorted(dataDict[train])
                for index in range(len(timelist)):
                    index_pre = index - 1
                    index_last = index + 1
                    if index_pre >= 0 and index_last <= len(timelist) - 1:
                        data_pre = dataDict[train][timelist[index_pre]]
                        data = dataDict[train][timelist[index]]
                        data_last = dataDict[train][timelist[index_last]]
                        colName = 'netVolt'
                        if colName in data_pre and colName in data and colName in data_last:
                            netVolt_pre = data_pre[colName]
                            netVolt = data[colName]
                            netVolt_last = data_last[colName]
                            if netVolt_pre.split('.')[0].isdigit() and \
                                    netVolt.split('.')[0].isdigit() and \
                                    netVolt_last.split('.')[0].isdigit():
                                netVolt_pre = float(netVolt_pre)
                                netVolt = float(netVolt)
                                netVolt_last = float(netVolt_last)
                                diff1 = netVolt_pre - netVolt
                                diff2 = netVolt_last - netVolt
                                if netVolt_pre > 20000 and netVolt < 10000 and netVolt_last > 20000:
                                    if diff1 > 10000 and diff2 > 10000:
                                        fTime = timelist[index]
                                        row = '32_' + train + '_00_' + fTime
                                        queryMile = self.hbase_db.getTrainMileData(row=row)
                                        if queryMile:
                                            mile =queryMile + coach_length*(int(coach)-1)
                                            for dataKey in phaseDict:
                                                _, mile_interval = dataKey.split('_')
                                                mile_start, mile_stop = mile_interval.split('-')
                                                mile_start = int(mile_start)
                                                mile_stop = int(mile_stop)
                                                if mile >= mile_start and mile <= mile_stop:
                                                    phase_valid = 1
                                                    condition = (
                                                    line_code, diretion_code, train, coach, mile, netVolt, fTime,
                                                    mile_start, mile_stop, phase_valid)
                                                    self.mysql_db.savePhaseData(condition)
                                                    break

                                            if phase_valid == 0:
                                                condition = (
                                                    line_code, diretion_code, train, coach, mile, netVolt, fTime, '', '',
                                                    phase_valid)
                                                self.mysql_db.savePhaseData(condition)

    def getFinalResult(self,line_keyId):
        """
        得到列车数据反映出的接触网分相数据与静态电务接触网分相数据的对比关系
        :param line_keyId:线路编号行别编号ID
        :return:静态电务接触网分相数据与列车数据反映的接触网分相数据的区间对比验证
        """

        dataDict=dict()
        line_code, direction_code = line_keyId.split('-')
        data = self.mysql_db.queryPhaseData(line_code, direction_code)

        mileData=set()
        mileLimitSet=set()
        for d in data:
            line_code, direction_code, train_no, coach_id, mile, netVolt, fTime, start_mile, end_mile, phase_valid = d
            mile = int(mile)
            mileData.add(mile)

            if start_mile.isdigit():
                mileLimit = start_mile.zfill(7)+'_'+end_mile.zfill(7)
                mileLimitSet.add(mileLimit)

        mileLimitSet = sorted(mileLimitSet)

        for mile in mileData:
            for mileLimit in mileLimitSet:
                s_mile , e_mile = mileLimit.split('_')
                s_mile = int(s_mile)
                e_mile = int(e_mile)
                if mile >=s_mile-300 and mile <=e_mile+300:
                    if mileLimit not in dataDict:
                        dataDict[mileLimit]=[mile]
                    else:
                        dataDict[mileLimit].append(mile)
                    break

        for x in sorted(dataDict):
            print(x,min(dataDict[x]),max(dataDict[x]))

if __name__ == '__main__':
    try:
        m = Model()
        if len(sys.argv) == 5:
            line_name = sys.argv[1]
            direction = sys.argv[2]
            day_start = sys.argv[3]
            day_stop = sys.argv[4]
            # train_line_data = m.mysql_db.queryParamFromLineFeature(line_name, direction)
            # phaseData = m.mysql_db.query_electric_phase_data(line_name, direction)
            # m.runTestPasheData(train_line_data, day_start, day_stop, phaseData)
            m.getFinalResult('3002-0')
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)





