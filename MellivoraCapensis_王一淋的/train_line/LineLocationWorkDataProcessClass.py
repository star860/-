# -*- coding: utf-8 -*-

import sys
import time
import math
import datetime
import traceback
import numpy as np
from db_hive_conn import HiveData
from db_mysql_conn import MySQLData
from db_hbase_conn import HBaseData
from data_util import dataModel


class Model():
    """工务数据导入"""

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

    def runTrainLineWorkData(self, line_name, direction, use_table, final_hbase_tabName):
        """
        执行工务数据导入程序
        :param line_name: 线路名称
        :param direction: 行别
        :param use_table: 工务数据表
        :param final_hbase_tabName: 数据导入的hbase表名
        :return:
        """
        if use_table == 'ods_line_work_tunnel_data':
            data = self.mysql_db.query_work_tunnel_data(line_name, direction)
            self.hbase_db.save_work_tunnel_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_work_bridge_data':
            data = self.mysql_db.query_work_bridge_data(line_name, direction)
            self.hbase_db.save_work_bridge_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_work_ramp_data':
            data = self.mysql_db.query_work_ramp_data(line_name, direction)
            self.hbase_db.save_work_ramp_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_work_turnout_data':
            data = self.mysql_db.query_work_turnout_data(line_name, direction)
            self.hbase_db.save_work_turnout_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_work_curve_data':
            data = self.mysql_db.query_work_curve_data(line_name, direction)
            self.hbase_db.save_work_curve_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_work_speed_limit':
            data = self.mysql_db.query_work_speedLimit_data(line_name, direction)
            self.hbase_db.save_work_speedLimit_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_electric_signal_data':
            data = self.mysql_db.query_electric_signal_data(line_name, direction)
            self.hbase_db.save_electric_signal_data(data, final_hbase_tabName, full_length)

        elif use_table == 'ods_line_electric_phase_data':
            data = self.mysql_db.query_electric_phase_data(line_name, direction)
            self.hbase_db.save_electric_phase_data(data, final_hbase_tabName, full_length)

        else:
            print('无效表名')
            sys.exit(1)


if __name__ == '__main__':
    try:
        m = Model()
        if len(sys.argv) == 4:
            line_name = sys.argv[1]
            direction = sys.argv[2]
            use_table = sys.argv[3]
            final_hbase_tabName = 'DM_TRAIN_LINE_FEATURE_INFO'
            train_line_data = m.mysql_db.queryParamFromLineFeature(line_name, direction)
            line_code, line_direction_code, station_start, station_end, full_length = train_line_data
            m.runTrainLineWorkData(line_name, direction, use_table, final_hbase_tabName, full_length)
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
