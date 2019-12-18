# -*- coding: utf-8 -*-

import re
import sys
import datetime
import traceback
import param_conf
import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas

hive_host = param_conf.hive_host
hive_port = param_conf.hive_port
hive_user = param_conf.hive_user
hive_passwd = param_conf.hive_passwd
hive_db = param_conf.hive_db


class HiveData():

    def __init__(self):
        try:
            self.conn = connect(host=hive_host, port=hive_port, auth_mechanism='PLAIN', user=hive_user,
                                password=hive_passwd, database=hive_db)
        except Exception:
            print('%s Hive连接异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit()

    def __del__(self):
        '''功能: 关闭数据库边接'''
        self.conn.close()

    def test(self):
        """测试"""
        cursor = self.conn.cursor()
        cursor.execute('show tables like "dm*_sdr_*dm"')
        df = as_pandas(cursor)
        data = df.to_dict('list')
        print(data)

    def query_open_string_info_by_trainsite(self, op_day, trainsite):
        '''根据车次 查询 开行详细信息 表数据
            param op_day: 该表的分区日期
            param trainsite: 车次
        '''
        assert isinstance(trainsite, str) or isinstance(trainsite, tuple)
        if isinstance(trainsite, str):
            trainsite = '(\'' + trainsite + '\')'
        else:
            trainsite = str(trainsite)
        info = {}
        try:
            sqlStr = """select   S_DATE    --开行日期
              ,S_TRAINNO                   --车次
              ,S_TRAINSETNAME              --车组
              ,S_STARTTIME                 --始发时间
              ,S_ENDTIME                   --终到时间
              ,S_STARTSTATION              --始发站
              ,S_ENDSTATION                --终到站
              ,S_RUNTIME                   --运行时间
              ,I_RUNMILE                   --运行里程
              ,TIME                        --更新时间
            from ODS_CUX_PHM_OPENSTRINGINFO_FORSF_V
            where op_day='%s' and S_TRAINNO in %s and instr(S_TRAINSETNAME,'CR400AF')!=0 
            """ % (op_day, trainsite)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchall()
            for data in data_list:
                S_DATE, S_TRAINNO, S_TRAINSETNAME, S_STARTTIME, S_ENDTIME, S_STARTSTATION, S_ENDSTATION, S_RUNTIME, I_RUNMILE, TIME = data
                # hbase存储的列号是CR400AF2001,hive表里列号是CR400AF-2001,以hbaseCR400AF2001格式为准
                if 'CR400AF-' in S_TRAINSETNAME:
                    if 'CR400AF-A' not in S_TRAINSETNAME and 'CR400AF-B' not in S_TRAINSETNAME:
                        S_TRAINSETNAME = S_TRAINSETNAME.replace('-', '')
                else:
                    S_TRAINSETNAME = S_TRAINSETNAME.split('-')[-1]
                info[S_TRAINSETNAME + '_' + S_STARTTIME] = [S_TRAINNO, TIME[:10], S_STARTTIME, S_ENDTIME,
                                                            S_STARTSTATION, S_ENDSTATION,
                                                            S_RUNTIME, I_RUNMILE]

        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)

        finally:
            cursor.close()  # 关闭游标

        return info

    def query_open_string_info_by_station(self, op_day, s_station, e_station):
        '''根据始发站 终点站 查询 开行详细信息 表数据
            param op_day: 该表的分区日期
            param s_station: 始发站
            param e_station: 终点站
        '''
        info = {}
        try:
            sqlStr = """select   S_DATE    --开行日期
              ,S_TRAINNO                   --车次
              ,S_TRAINSETNAME              --车组
              ,S_STARTTIME                 --始发时间
              ,S_ENDTIME                   --终到时间
              ,S_STARTSTATION              --始发站
              ,S_ENDSTATION                --终到站
              ,S_RUNTIME                   --运行时间
              ,I_RUNMILE                   --运行里程
              ,TIME                        --更新时间
            from ODS_CUX_PHM_OPENSTRINGINFO_FORSF_V
            where op_day='%s' and (S_STARTSTATION='%s' AND S_ENDSTATION ='%s')
            """ % (op_day, s_station, e_station)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchall()
            for data in data_list:
                S_DATE, S_TRAINNO, S_TRAINSETNAME, S_STARTTIME, S_ENDTIME, S_STARTSTATION, S_ENDSTATION, S_RUNTIME, I_RUNMILE, TIME = data
                # hbase存储的列号是CR400AF2001,hive表里列号是CR400AF-2001,以CR400AF2001格式为准
                if 'CR400AF-' in S_TRAINSETNAME:
                    if 'CR400AF-A' not in S_TRAINSETNAME and 'CR400AF-B' not in S_TRAINSETNAME:
                        S_TRAINSETNAME = S_TRAINSETNAME.replace('-', '')
                else:
                    S_TRAINSETNAME = S_TRAINSETNAME.split('-')[-1]
                keyId = S_TRAINSETNAME + '_' + S_TRAINNO + '_' + TIME[:10]
                info[keyId] = [S_TRAINNO, TIME[:10], S_STARTTIME, S_ENDTIME,
                               S_STARTSTATION, S_ENDSTATION, S_RUNTIME, I_RUNMILE]
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标

        return info



if __name__ == '__main__':
    db = HiveData()
    line_name = '京沪高铁'
    direction = '上'
    # direction = '双'
    # data = db.query_work_tunnel_data(line_name, direction)
    # data = db.query_work_bridge_data(line_name, direction)
    # data = db.query_work_curve_data(line_name, direction)
    # data = db.query_work_ramp_data(line_name, direction)
    # data = db.query_work_turnout_data(line_name, direction)
    # data = db.query_electric_signal_data(line_name, direction)
    data = db.query_electric_phase_data(line_name, direction)
    for k in sorted(data):
        print (k,data[k])





