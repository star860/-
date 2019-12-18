# -*- coding: utf-8 -*-

import os
import sys
import random
import datetime
import subprocess
import traceback
import happybase
import param_conf
from db_mysql_conn import MySQLData

hbase_host = param_conf.hbase_host
dev_hbase_host = param_conf.dev_hbase_host


class HBaseData():

    def __init__(self):
        try:
            self.conn = happybase.Connection(hbase_host, autoconnect=False)  # 建立连接
            self.conn.open()
            self.dev_conn = happybase.Connection(dev_hbase_host, autoconnect=False)  # 建立连接
            self.dev_conn.open()
        except Exception:
            print('%s HBase连接异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit()

    def __del__(self):
        '''功能: 关闭数据库边接'''
        self.conn.close()

    def getTrainMileData(self, row):
        """
        rowkey中包含列号与时间,根据rowkey查询hbase中该列号该时刻的里程数据
        :param row: hbase rowkey
        :return: 里程数据(单位:米)
        """

        result_mile = None
        cols = []
        f1 = 'F_DATA'
        f2 = 'S_DATA'

        try:
            db = MySQLData()
            info = db.queryParamFromTrainRules('32')  # 获取数据编码
            table = self.conn.table('ODS_PHM_ONLINE_TRAIN_DATA_LOG')
            for item in info:
                col = str(f1 + ':' + item)
                cols.append(col)
                col = str(f2 + ':' + item)
                cols.append(col)

            data = table.row(row=row, columns=cols)

            # 解析数据内容
            for dcode in data:
                family_name, dc = dcode.decode('utf-8').split(":")
                if dc in info:
                    col_name = info[dc][1]
                    if col_name == 'mileage':
                        if result_mile is None:
                            result_mile = data[dcode].decode('UTF-8')
                        else:
                            cur_mileage = data[dcode].decode('UTF-8')
                            old_mileage = result_mile
                            if old_mileage in ('', '0', '无效'):
                                result_mile = cur_mileage

        except Exception:
            print('%s hbase获取数据失败: %s' % (datetime.datetime.now(), traceback.format_exc()))

        if result_mile.split('.')[0].isdigit():
            result_mile = int(float(result_mile))
        else:
            result_mile = None

        return result_mile

    def queryTrainData(self, rowStart, rowStop, interface_code):
        """
        查询hbase数据
        :param rowStart:rowkey起始
        :param rowStop:rowkey结束
        :param interface_code:数据所在接口
        :return:
        """
        ds = {}
        cols = []
        f1 = 'F_DATA'
        f2 = 'S_DATA'
        family_use = None
        try:
            db = MySQLData()
            info = db.queryParamFromTrainRules(interface_code)  # 获取数据编码
            table = self.conn.table('ODS_PHM_ONLINE_TRAIN_DATA_LOG')
            for item in info:
                col = str(f1 + ':' + item)
                cols.append(col)
                col = str(f2 + ':' + item)
                cols.append(col)
            # cols.append('F_DATA:MAIN_CONTROL_COACH')
            # cols.append('S_DATA:MAIN_CONTROL_COACH')
            # cols.append('F_DATA:TRAIN_NO')
            # cols.append('S_DATA:TRAIN_NO')
            g = table.scan(row_start=rowStart, row_stop=rowStop, columns=cols)
            for k, data in g:  # 解析数据
                # 接口,列号, 车号, 时间
                interfaceCode, h_trainNo, h_coachId, h_timeValue = k.decode('utf-8').split('_')
                h_timeValue = h_timeValue.replace('FF', '')
                if h_trainNo not in ds:
                    ds[h_trainNo] = {h_timeValue: {}}
                elif h_timeValue not in ds[h_trainNo]:
                    ds[h_trainNo][h_timeValue] = {}

                # 解析数据内容
                for dcode in data:
                    family_name, dc = dcode.decode('utf-8').split(":")
                    if dc in info:
                        col_name = info[dc][1]
                        if col_name not in ds[h_trainNo][h_timeValue]:
                            ds[h_trainNo][h_timeValue][col_name] = data[dcode].decode('UTF-8')
                        else:
                            if col_name == 'trainsite_id':
                                cur_trainsite_id = data[dcode].decode('UTF-8')
                                old_trainsite_id = ds[h_trainNo][h_timeValue][col_name]
                                if old_trainsite_id in ('', '--'):
                                    ds[h_trainNo][h_timeValue][col_name] = cur_trainsite_id
                            if col_name == 'speed':
                                cur_speed = data[dcode].decode('UTF-8')
                                old_speed = ds[h_trainNo][h_timeValue][col_name]
                                if old_speed in ('', '--'):
                                    ds[h_trainNo][h_timeValue][col_name] = cur_speed
                            if col_name == 'mileage':
                                cur_mileage = data[dcode].decode('UTF-8')
                                old_mileage = ds[h_trainNo][h_timeValue][col_name]
                                if old_mileage in ('', '0', '无效'):
                                    ds[h_trainNo][h_timeValue][col_name] = cur_mileage
                            if col_name == 'netVolt':
                                cur_netVolt = data[dcode].decode('UTF-8')
                                old_netVolt = ds[h_trainNo][h_timeValue][col_name]
                                if old_netVolt in ('', '0', '无效'):
                                    ds[h_trainNo][h_timeValue][col_name] = cur_netVolt

        except Exception:
            print('%s hbase获取数据失败: %s' % (datetime.datetime.now(), traceback.format_exc()))

        return ds

    def saveDataToHbase(self, tab_name, rowkey, dataDict, repeat=False):
        """
        数据存储到hbase表中
        :param tab_name:hbase表名
        :param rowkey:hbase rowkey
        :param dataDict:数据字典
        :return:
        """
        assert repeat in (True, False)
        table = self.dev_conn.table(tab_name)
        if repeat:
            exist = table.row(rowkey)
            if exist:
                random_list = random.sample('0123456789', 1)
                random_str = ''.join(random_list)
                rowkey = rowkey + '-' + random_str
        table.put(rowkey, dataDict)

    def queryGPS(self, tab_name, line_prefix=None, line_start=None, line_stop=None):
        """
        根据线路编号,查询里程GPS数据
        :param line_prefix:线路前缀
        :param tab_name:hbase表名
        :return:
        """
        table = self.dev_conn.table(tab_name)
        col_list = ['GPS_DATA:longitude', 'GPS_DATA:latitude', 'GPS_DATA:altitude']
        if line_prefix:
            g = table.scan(row_prefix=line_prefix, columns=col_list)
        elif line_start and line_stop:
            g = table.scan(row_start=line_start, row_stop=line_stop, columns=col_list)
        else:
            g = table.scan(columns=col_list)
        list_mileage = []
        list_longitude = []
        list_latitude = []
        list_altitude = []
        count = 0
        for rowKey, data in g:
            count += 1
            if count % 10000 == 0:
                print(count, rowKey)
            longitude = data[b'GPS_DATA:longitude'].decode('UTF-8')
            latitude = data[b'GPS_DATA:latitude'].decode('UTF-8')
            altitude = data[b'GPS_DATA:altitude'].decode('UTF-8')
            code, mileage = rowKey.decode('UTF-8').split('_')
            list_mileage.append(mileage)
            list_longitude.append(float(longitude))
            list_latitude.append(float(latitude))
            list_altitude.append(float(altitude))

        return [code, list_mileage, list_altitude, list_latitude, list_longitude]

    def save_work_tunnel_data(self, data, tab_name, full_length):
        """
        存储隧道数据到hbase数据宽表中
        :param data: 隧道相关数据
        :param tab_name: hbase表名
        :param full_length: 隧道所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'TUNNEL_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_tunnel_code = family_name + 'tunnel_code'
        col_tunnel_name = family_name + 'tunnel_name'
        col_start_mileage = family_name + 'start_mileage'
        col_end_mileage = family_name + 'end_mileage'
        col_center_mileage = family_name + 'center_mileage'
        col_full_length = family_name + 'full_length'
        col_auxiliary_line_name = family_name + 'auxiliary_line_name'
        col_auxiliary_line_code = family_name + 'auxiliary_line_code'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_tunnel_data = basic_name + 'has_tunnel_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, mile_interval = splitk.split('_')
            mile_start, mile_stop = mile_interval.split('-')
            center_mileage, full_length, railway_name, railway_code, line_name, \
            diff_lines, tunnel_code, tunnel_name, auxiliary_line_name, auxiliary_line_code, remarks = data[splitk]
            tmpSet = set(range(int(mile_start), int(mile_stop) + 1))
            mileSet -= tmpSet
            print(mile_start, mile_stop)
            for mile in range(int(mile_start), int(mile_stop) + 1):
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_line_name: line_name,
                    col_diff_lines: diff_lines,
                    col_has_tunnel_data: '1',
                    col_railway_name: railway_name,
                    col_railway_code: railway_code,
                    col_tunnel_code: tunnel_code,
                    col_tunnel_name: tunnel_name,
                    col_start_mileage: str(mile_start),
                    col_end_mileage: str(mile_stop),
                    col_center_mileage: str(center_mileage),
                    col_full_length: str(full_length),
                    col_auxiliary_line_name: auxiliary_line_name,
                    col_auxiliary_line_code: auxiliary_line_code,
                    col_remarks: remarks
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_tunnel_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_work_bridge_data(self, data, tab_name, full_length):
        """
        存储桥梁数据到hbase数据宽表中
        :param data: 桥梁相关数据
        :param tab_name: hbase表名
        :param full_length: 桥梁所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'BRIDGE_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_bridge_code = family_name + 'bridge_code'
        col_bridge_name = family_name + 'bridge_name'
        col_start_mileage = family_name + 'start_mileage'
        col_end_mileage = family_name + 'end_mileage'
        col_center_mileage = family_name + 'center_mileage'
        col_full_length = family_name + 'full_length'
        col_auxiliary_line_name = family_name + 'auxiliary_line_name'
        col_auxiliary_line_code = family_name + 'auxiliary_line_code'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_bridge_data = basic_name + 'has_bridge_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, mile_interval = splitk.split('_')
            mile_start, mile_stop = mile_interval.split('-')
            center_mileage, full_length, railway_name, railway_code, line_name, \
            diff_lines, bridge_code, bridge_name, auxiliary_line_name, auxiliary_line_code, remarks = data[splitk]
            tmpSet = set(range(int(mile_start), int(mile_stop) + 1))
            mileSet -= tmpSet
            print(mile_start, mile_stop)
            for mile in range(int(mile_start), int(mile_stop) + 1):
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_line_name: line_name,
                    col_diff_lines: diff_lines,
                    col_has_bridge_data: '1',
                    col_railway_name: railway_name,
                    col_railway_code: railway_code,
                    col_bridge_code: bridge_code,
                    col_bridge_name: bridge_name,
                    col_start_mileage: str(mile_start),
                    col_end_mileage: str(mile_stop),
                    col_center_mileage: str(center_mileage),
                    col_full_length: str(full_length),
                    col_auxiliary_line_name: auxiliary_line_name,
                    col_auxiliary_line_code: auxiliary_line_code,
                    col_remarks: remarks
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_bridge_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_work_ramp_data(self, data, tab_name, full_length):
        """
        存储坡道数据到hbase数据宽表中
        :param data: 坡道相关数据
        :param tab_name: hbase表名
        :param full_length: 坡道所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'RAMP_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_start_mileage = family_name + 'start_mileage'
        col_end_mileage = family_name + 'end_mileage'
        col_line_slope = family_name + 'line_slope'
        col_slope_length = family_name + 'slope_length'
        col_chain_mileage = family_name + 'chain_mileage'
        col_chain_length = family_name + 'chain_length'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_ramp_data = basic_name + 'has_ramp_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, mile_interval = splitk.split('_')
            mile_start, mile_stop = mile_interval.split('-')
            start_mileage, end_mileage, railway_name, railway_code, line_name, \
            diff_lines, line_slope, slope_length, chain_mileage, chain_length, remarks = data[splitk]
            tmpSet = set(range(int(mile_start), int(mile_stop) + 1))
            mileSet -= tmpSet
            print(mile_start, mile_stop)
            for mile in range(int(mile_start), int(mile_stop) + 1):
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_line_name: line_name,
                    col_diff_lines: diff_lines,
                    col_has_ramp_data: '1',
                    col_railway_name: railway_name,
                    col_railway_code: railway_code,
                    col_start_mileage: str(mile_start),
                    col_end_mileage: str(mile_stop),
                    col_line_slope: str(line_slope),
                    col_slope_length: str(slope_length),
                    col_chain_mileage: str(chain_mileage),
                    col_chain_length: str(chain_length),
                    col_remarks: remarks
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_ramp_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_work_turnout_data(self, data, tab_name, full_length):
        """
        存储道岔数据到hbase数据宽表中
        :param data: 道岔相关数据
        :param tab_name: hbase表名
        :param full_length: 道岔所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'TURNOUT_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_train_station_name = family_name + 'train_station_name'
        col_train_station_code = family_name + 'train_station_code'
        col_turnout_code = family_name + 'turnout_code'
        col_fork_number = family_name + 'fork_number'
        col_turnout_open = family_name + 'turnout_open'
        col_laying_number = family_name + 'laying_number'
        col_remarks = family_name + 'remarks'
        col_note = family_name + 'note'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_turnout_data = basic_name + 'has_turnout_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, sharp_point_mileage = splitk.split('_')
            railway_name, railway_code, line_name, diff_lines, train_station_name, train_station_code, \
            turnout_code, fork_number, turnout_open, laying_number, remarks, note = data[splitk]

            mileSet.remove(sharp_point_mileage)

            rowkey = line_keyId + '_' + str(sharp_point_mileage).zfill(7)
            dataDict = {
                col_line_name: line_name,
                col_diff_lines: diff_lines,
                col_has_turnout_data: '1',
                col_railway_name: railway_name,
                col_railway_code: railway_code,
                col_train_station_name: train_station_name,
                col_train_station_code: train_station_code,
                col_turnout_code: turnout_code,
                col_fork_number: fork_number,
                col_turnout_open: turnout_open,
                col_laying_number: laying_number,
                col_remarks: remarks,
                col_note: note,
            }
            self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_turnout_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_work_curve_data(self, data, tab_name, full_length):
        """
        存储曲线数据到hbase数据宽表中
        :param data: 曲线相关数据
        :param tab_name: hbase表名
        :param full_length: 曲线所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'CURVE_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_start_mileage = family_name + 'start_mileage'
        col_end_mileage = family_name + 'end_mileage'
        col_curve_direction = family_name + 'curve_direction'
        col_curve_radius = family_name + 'curve_radius'
        col_curve_length = family_name + 'curve_length'
        col_super_high = family_name + 'super_high'
        col_slow_length = family_name + 'slow_length'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_curve_data = basic_name + 'has_curve_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, mile_interval = splitk.split('_')
            mile_start, mile_stop = mile_interval.split('-')
            start_mileage, end_mileage, railway_name, railway_code, line_name, \
            diff_lines, curve_direction, curve_radius, curve_length, super_high, slow_length, remarks = data[splitk]

            tmpSet = set(range(int(mile_start), int(mile_stop) + 1))
            mileSet -= tmpSet
            print(mile_start, mile_stop)
            for mile in range(int(mile_start), int(mile_stop) + 1):
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_line_name: line_name,
                    col_diff_lines: diff_lines,
                    col_has_curve_data: '1',
                    col_railway_name: railway_name,
                    col_railway_code: railway_code,
                    col_start_mileage: str(start_mileage),
                    col_end_mileage: str(end_mileage),
                    col_curve_direction: curve_direction,
                    col_curve_radius: str(curve_radius),
                    col_curve_length: str(curve_length),
                    col_super_high: str(super_high),
                    col_slow_length: str(slow_length),
                    col_remarks: remarks
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_curve_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_work_speedLimit_data(self, data, tab_name, full_length):
        """
        存储限速数据到hbase数据宽表中
        :param data: 限速相关数据
        :param tab_name: hbase表名
        :param full_length: 限速所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'SPEED_LIMIT_DATA:'
        col_start_mileage = family_name + 'start_mileage'
        col_end_mileage = family_name + 'end_mileage'
        col_start_position = family_name + 'start_position'
        col_end_position = family_name + 'end_position'
        col_line_allowable_speed = family_name + 'line_allowable_speed'
        col_station_overspeed_straight = family_name + 'station_overspeed_straight'
        col_station_overspeed_lateral = family_name + 'station_overspeed_lateral'
        col_long_limit_start_mileage = family_name + 'long_limit_start_mileage'
        col_long_limit_end_mileage = family_name + 'long_limit_end_mileage'
        col_long_limit_start_position = family_name + 'long_limit_start_position'
        col_long_limit_end_position = family_name + 'long_limit_end_position'
        col_limit_speed = family_name + 'limit_speed'
        col_limit_reason = family_name + 'limit_reason'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_speed_limit_data = basic_name + 'has_speed_limit_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, mile_interval = splitk.split('_')
            mile_start, mile_stop = mile_interval.split('-')

            line_name, line_code, diff_lines, start_position, end_position, \
            line_allowable_speed, station_overspeed_straight, station_overspeed_lateral, \
            long_limit_start_mileage, long_limit_end_mileage, long_limit_start_position, \
            long_limit_end_position, limit_speed, limit_reason, remarks = data[splitk]
            tmpSet = set(range(int(mile_start), int(mile_stop) + 1))
            mileSet -= tmpSet
            print(mile_start, mile_stop)
            for mile in range(int(mile_start), int(mile_stop) + 1):
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_line_name: line_name,
                    col_diff_lines: diff_lines,
                    col_has_speed_limit_data: '1',
                    col_start_mileage: str(mile_start),
                    col_end_mileage: str(mile_stop),
                    col_start_position: start_position,
                    col_end_position: end_position,
                    col_line_allowable_speed: line_allowable_speed,
                    col_station_overspeed_straight: station_overspeed_straight,
                    col_station_overspeed_lateral: station_overspeed_lateral,
                    col_long_limit_start_mileage: long_limit_start_mileage,
                    col_long_limit_end_mileage: long_limit_end_mileage,
                    col_long_limit_start_position: long_limit_start_position,
                    col_long_limit_end_position: long_limit_end_position,
                    col_limit_speed: limit_speed,
                    col_limit_reason: limit_reason,
                    col_remarks: remarks
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_speed_limit_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_electric_signal_data(self, data, tab_name, full_length):
        """
        存储电务信号数据到hbase数据宽表中
        :param data: 电务信号相关数据
        :param tab_name: hbase表名
        :param full_length: 电务信号所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'SIGNAL_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_train_station_name = family_name + 'train_station_name'
        col_signal_machine_code = family_name + 'signal_machine_code'
        col_signal_machine_distance = family_name + 'signal_machine_distance'
        col_signal_machine_type = family_name + 'signal_machine_type'
        col_track_circuit_systems = family_name + 'track_circuit_systems'
        col_center_frequency = family_name + 'center_frequency'
        col_um71_category = family_name + 'um71_category'
        col_block_mode = family_name + 'block_mode'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_signal_data = basic_name + 'has_signal_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, signal_machine_position = splitk.split('_')
            railway_name, railway_code, line_name, diff_lines, train_station_name, signal_machine_code, signal_machine_distance, \
            signal_machine_type, track_circuit_systems, center_frequency, um71_category, block_mode, remarks = data[
                splitk]

            mileSet.remove(signal_machine_position)

            rowkey = line_keyId + '_' + str(signal_machine_position).zfill(7)
            dataDict = {
                col_line_name: line_name,
                col_diff_lines: diff_lines,
                col_has_signal_data: '1',
                col_railway_name: railway_name,
                col_railway_code: railway_code,
                col_train_station_name: train_station_name,
                col_signal_machine_code: signal_machine_code,
                col_signal_machine_distance: str(signal_machine_distance),
                col_signal_machine_type: signal_machine_type,
                col_track_circuit_systems: track_circuit_systems,
                col_center_frequency: str(center_frequency),
                col_um71_category: um71_category,
                col_block_mode: block_mode,
                col_remarks: remarks
            }
            self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_signal_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)

    def save_electric_phase_data(self, data, tab_name, full_length):
        """
        存储接触风过分相数据到hbase数据宽表中
        :param data: 接触风过分相相关数据
        :param tab_name: hbase表名
        :param full_length: 接触风过分相所在对应的线路的全长
        :return:
        """
        line_keyId = None
        family_name = 'PHASE_DATA:'
        col_railway_name = family_name + 'railway_name'
        col_railway_code = family_name + 'railway_code'
        col_train_station_name = family_name + 'train_station_name'
        col_start_mileage = family_name + 'start_mileage'
        col_end_mileage = family_name + 'end_mileage'
        col_center_mileage = family_name + 'center_mileage'
        col_phase_structure = family_name + 'phase_structure'
        col_remarks = family_name + 'remarks'

        basic_name = 'BASIC_DATA:'
        col_line_name = basic_name + 'line_name'
        col_diff_lines = basic_name + 'diff_lines'
        col_has_phase_data = basic_name + 'has_phase_data'

        mileSet = set(range(full_length + 1))

        for splitk in data:
            line_keyId, mile_interval = splitk.split('_')
            mile_start, mile_stop = mile_interval.split('-')
            railway_name, railway_code, line_name, diff_lines, train_station_name, \
            center_mileage, phase_structure, remarks = data[splitk]
            tmpSet = set(range(int(mile_start), int(mile_stop) + 1))
            mileSet -= tmpSet
            print(mile_start, mile_stop)
            for mile in range(int(mile_start), int(mile_stop) + 1):
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_line_name: line_name,
                    col_diff_lines: diff_lines,
                    col_has_phase_data: '1',
                    col_railway_name: railway_name,
                    col_railway_code: railway_code,
                    col_train_station_name: train_station_name,
                    col_start_mileage: str(mile_start),
                    col_end_mileage: str(mile_stop),
                    col_center_mileage: str(center_mileage),
                    col_phase_structure: phase_structure,
                    col_remarks: remarks
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)
        if line_keyId is not None:
            for mile in mileSet:
                rowkey = line_keyId + '_' + str(mile).zfill(7)
                dataDict = {
                    col_has_phase_data: '0',
                }
                self.saveDataToHbase(tab_name, rowkey, dataDict)


if __name__ == '__main__':
    db = HBaseData()
    # dataList=db.queryGPS(tab_name='lzb_test')
    dataList = db.queryTrainData('32_CR400AF2058_00_20180904090000', '32_CR400AF2058_00_20180904130000', '32')
    print(len(dataList))
    # print(random.choice('abcdefghijklmnopqrstuvwxyz!@#$%^&*()'))
