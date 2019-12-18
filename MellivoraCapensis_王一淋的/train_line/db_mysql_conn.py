# -*- coding: utf-8 -*-

import sys
import datetime
import traceback
import MySQLdb
import param_conf

hostName = param_conf.mysql_host
port = param_conf.mysql_port
userName = param_conf.mysql_userName
password = param_conf.mysql_password
dbName = param_conf.mysql_dbName


# hostName = param_conf.local_mysql_host
# port = param_conf.local_mysql_port
# userName = param_conf.local_mysql_userName
# password = param_conf.local_mysql_password
# dbName = param_conf.local_mysql_dbName


class MySQLData():

    def __init__(self):
        try:
            self.conn = MySQLdb.connect(
                host=hostName,
                port=port,
                user=userName,
                passwd=password,
                db=dbName,
                charset='utf8'
            )
        except Exception as e:
            print('%s MySQL数据库连接异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit()

    def __del__(self):
        '''功能: 关闭数据库边接'''
        self.conn.close()

    def queryParamFromTrainRules(self, interface_code):
        """
        功能: 查询列车协议配置数据
        :param interface_code: 数据接口
        :return: 对应接口协议的字段数据
        """

        if interface_code == '32':
            params = param_conf.p_32
        elif interface_code == '37':
            params = param_conf.p_37

        info = dict()
        try:
            cur = self.conn.cursor()

            sqlStr = """
            select t1.train_type,
                   t1.data_code,
                   t1.control_data_code, 
                   t1.display_code,
                   t2.is_long,
                   t1.coach_no,
                   t1.wtds16_coach_no
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type = '%s'
               and data_source in ('WTD') 
               and t1.display_code in(
            """ % (interface_code)

            flag = 0
            for item in params:
                if flag == 0:
                    sqlStr = sqlStr + "'" + item + "'"
                    flag = 1
                else:
                    sqlStr = sqlStr + ",'" + item + "'"

            sqlStr = sqlStr + ")"
            sqlStr = sqlStr + """
                 and t1.train_type=t2.type_code
               order by t1.train_type,t1.display_code
            """

            # 获取数据结果集
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in params:
                    colName = params[item[3]]
                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组

        except Exception as e:
            print('%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标

        return info

    def queryParamFromLineFeature(self, query_line_name, query_direction):
        """
        查询线路特征配置数据
        :param query_line_name: 线路名称
        :param query_direction: 线路行别
        :return:线路特征配置信息
        """
        info = {}
        try:
            cur = self.conn.cursor()

            sqlStr = """
            select  t1.line_name
                    ,t1.line_code
                    ,t1.line_direction
                    ,t1.line_direction_code
                    ,t1.station_start
                    ,t1.station_end
                    ,t1.full_length
            from train_line_feature_config t1
            """

            # 获取数据结果集
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                line_name, line_code, line_direction, line_direction_code, station_start, station_end, full_length = item
                if line_name == query_line_name and line_direction == query_direction:
                    return [line_code, line_direction_code, station_start, station_end, int(full_length) * 1000]

            # for item in data:
            #     line_name, line_code, line_direction,line_direction_code,station_start,station_end=item
            #     if line_name not in info:
            #         info[line_name] = {line_direction:[line_code,line_direction_code,station_start,station_end]}
            #     elif line_direction not in info[line_name]:
            #         info[line_name][line_direction]=[line_code,line_direction_code,station_start,station_end]

        except Exception as e:
            print('%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标

    def query_work_tunnel_data(self, line_name, direction):
        '''查询 工务数据-隧道表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''

        info = {}
        try:
            sqlStr = """
            select 
                 railway_name                       -- 路局名称
                ,railway_code                       -- 路局代码
                ,line_name                          -- 线路名称
                ,line_code                          -- 线路代码
                ,diff_lines                         -- 行别
                ,tunnel_code                        -- 隧道号
                ,tunnel_name                        -- 隧道名
                ,center_mileage                     -- 中心里程
                ,full_length                        -- 全长
                ,auxiliary_line_name                -- 辅助线名
                ,auxiliary_line_code                -- 辅助编号
                ,remarks                            -- 备注标记
            from ods_line_work_tunnel_data
            where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, tunnel_code, tunnel_name, center_mileage, full_length, auxiliary_line_name, auxiliary_line_code, remarks = data
                center_mileage = float(center_mileage) * 1000
                full_length = float(full_length)
                start_mileage = int(center_mileage - full_length / 2)
                end_mileage = int(center_mileage + full_length / 2)
                dList = [center_mileage, full_length, railway_name, railway_code, line_name,
                         diff_lines, tunnel_code, tunnel_name, auxiliary_line_name, auxiliary_line_code, remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(start_mileage) + '-' + str(end_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标

        return info

    def query_work_bridge_data(self, line_name, direction):
        '''查询 工务数据-桥梁表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称

        '''

        info = {}
        try:
            sqlStr = """
            select 
                 railway_name                    -- 路局名称
                ,railway_code                    -- 路局代码
                ,line_name                       -- 线路名称
                ,line_code                       -- 线路代码
                ,diff_lines                      -- 行别
                ,bridge_code                     -- 桥梁号
                ,bridge_name                     -- 桥梁名
                ,center_mileage                  -- 中心里程
                ,full_length                     -- 全长
                ,auxiliary_line_name             -- 辅助线名
                ,auxiliary_line_code             -- 辅助编号
                ,remarks                         -- 备注标记
            from ods_line_work_bridge_data
            where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, bridge_code, bridge_name, center_mileage, full_length, auxiliary_line_name, auxiliary_line_code, remarks = data
                center_mileage = float(center_mileage) * 1000
                full_length = float(full_length)
                start_mileage = int(center_mileage - full_length / 2)
                end_mileage = int(center_mileage + full_length / 2)
                dList = [center_mileage, full_length, railway_name, railway_code, line_name,
                         diff_lines, bridge_code, bridge_name, auxiliary_line_name, auxiliary_line_code, remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(start_mileage) + '-' + str(end_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return info

    def query_work_curve_data(self, line_name, direction):
        '''查询 工务数据-曲线表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''

        info = {}
        try:
            sqlStr = """
            select 
                 railway_name                  -- 路局名称
                ,railway_code                  -- 路局代码
                ,line_name                     -- 线路名称
                ,line_code                     -- 线路代码
                ,diff_lines                    -- 行别
                ,start_mileage                 -- 起点里程
                ,end_mileage                   -- 终点里程
                ,curve_direction               -- 曲线方向
                ,curve_radius                  -- 曲线半径
                ,curve_length                  -- 曲线长度
                ,super_high                    -- 超高
                ,slow_length                   -- 缓长
                ,remarks                       -- 备注标记
            from ods_line_work_curve_data
            where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, start_mileage, end_mileage, curve_direction, \
                curve_radius, curve_length, super_high, slow_length, remarks = data

                start_mileage = int(float(start_mileage) * 1000)
                end_mileage = int(float(end_mileage) * 1000)
                dList = [start_mileage, end_mileage, railway_name, railway_code, line_name,
                         diff_lines, curve_direction, curve_radius, curve_length, super_high, slow_length, remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(start_mileage) + '-' + str(end_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标

        return info

    def query_work_ramp_data(self, line_name, direction):
        '''查询 工务数据-坡道表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''

        info = {}
        try:
            sqlStr = """
            select 
                 railway_name                    -- 路局名称
                ,railway_code                    -- 路局代码
                ,line_name                       -- 线路名称
                ,line_code                       -- 线路代码
                ,diff_lines                      -- 行别
                ,start_mileage                   -- 起点里程
                ,end_mileage                     -- 终点里程
                ,line_slope                      -- 线路坡度
                ,slope_length                    -- 线路坡长
                ,chain_mileage                   -- 长短链里程
                ,chain_length                    -- 长短链长度
                ,remarks                         -- 备注标记
            from ods_line_work_ramp_data
            where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, start_mileage, end_mileage, line_slope, \
                slope_length, chain_mileage, chain_length, remarks = data

                start_mileage = int(float(start_mileage) * 1000)
                end_mileage = int(float(end_mileage) * 1000)
                dList = [start_mileage, end_mileage, railway_name, railway_code, line_name,
                         diff_lines, line_slope, slope_length, chain_mileage, chain_length, remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(start_mileage) + '-' + str(end_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标

        return info

    def query_work_turnout_data(self, line_name, direction):
        '''查询 工务数据-道岔表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''

        info = {}
        try:
            sqlStr = """
              select 
                 railway_name                      -- 路局名称
                ,railway_code                      -- 路局代码
                ,line_name                         -- 线路名称
                ,line_code                         -- 线路代码
                ,diff_lines                        -- 行别
                ,train_station_name                -- 站点名称
                ,train_station_code                -- 站点编号
                ,turnout_code                      -- 道岔编号
                ,sharp_point_mileage               -- 尖轨尖里程
                ,fork_number                       -- 辙叉号
                ,turnout_open                      -- 道岔开向
                ,laying_number                     -- 铺设图号
                ,remarks                           -- 备注标记
                ,note                              -- 附注
              from ods_line_work_turnout_data
              where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, train_station_name, train_station_code, turnout_code, \
                sharp_point_mileage, fork_number, turnout_open, laying_number, remarks, note = data

                sharp_point_mileage = int(float(sharp_point_mileage) * 1000)
                dList = [railway_name, railway_code, line_name, diff_lines, train_station_name, train_station_code,
                         turnout_code, \
                         fork_number, turnout_open, laying_number, remarks, note]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(sharp_point_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标

        return info

    def query_work_speedLimit_data(self, line_name, direction):
        '''查询 工务数据-限速表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''
        info = {}
        try:
            sqlStr = """
              select 
                 line_name                        -- 线路名称
                ,line_code                        -- 线路代码
                ,diff_lines                       -- 行别
                ,start_mileage                    -- 起点里程
                ,end_mileage                      -- 终点里程
                ,start_position                   -- 起点位置
                ,end_position                     -- 终点位置
                ,line_allowable_speed             -- 线路允许速度
                ,station_overspeed_straight       -- 车站过岔速度直向
                ,station_overspeed_lateral        -- 车站过岔速度侧向
                ,long_limit_start_mileage         -- 长期限制慢行起点里程
                ,long_limit_end_mileage           -- 长期限制慢行终点里程
                ,long_limit_start_position        -- 长期限制慢行起点位置
                ,long_limit_end_position          -- 长期限制慢行终点位置
                ,limit_speed                      -- 长期限制慢行速度
                ,limit_reason                     -- 长期限制慢行原因
                ,remarks                          -- 备注标记
              from ods_line_work_speed_limit
              where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                line_name, line_code, diff_lines, start_mileage, end_mileage, start_position, end_position, line_allowable_speed, \
                station_overspeed_straight, station_overspeed_lateral, long_limit_start_mileage, long_limit_end_mileage, \
                long_limit_start_position, long_limit_end_position, limit_speed, limit_reason, remarks = data

                start_mileage = int(float(start_mileage) * 1000)
                end_mileage = int(float(end_mileage) * 1000)
                dList = [line_name, line_code, diff_lines, start_position, end_position,
                         line_allowable_speed, station_overspeed_straight, station_overspeed_lateral,
                         long_limit_start_mileage, long_limit_end_mileage, long_limit_start_position,
                         long_limit_end_position, limit_speed, limit_reason, remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(start_mileage) + '-' + str(end_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return info

    def query_electric_signal_data(self, line_name, direction):
        '''查询 电务数据-电务信号表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''
        info = {}
        try:
            sqlStr = """
              select 
                 railway_name                       -- 路局名称
                ,railway_code                       -- 路局代码
                ,line_name                          -- 线路名称
                ,line_code                          -- 线路代码
                ,diff_lines                         -- 行别
                ,train_station_name                 -- 站点名称
                ,signal_machine_code                -- 信号机编号
                ,signal_machine_distance            -- 信号机间距离
                ,signal_machine_position            -- 信号机位置
                ,signal_machine_type                -- 信号机类型
                ,track_circuit_systems              -- 轨道电路制式
                ,center_frequency                   -- 中心频率
                ,um71_category                      -- UM71类别
                ,block_mode                         -- 闭塞方式
                ,remarks                            -- 备注标记
              from ods_line_electric_signal_data
              where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, train_station_name, signal_machine_code, signal_machine_distance, \
                signal_machine_position, signal_machine_type, track_circuit_systems, center_frequency, um71_category, block_mode, remarks = data

                # if '+' in signal_machine_position:
                #     head, tail = signal_machine_position.split('+')
                #     signal_machine_position = int(float(head.split('K')[-1]) * 1000 + float(tail))
                # else:
                signal_machine_position = int(float(signal_machine_position) * 1000)
                dList = [railway_name, railway_code, line_name, diff_lines, train_station_name, signal_machine_code,
                         signal_machine_distance, \
                         signal_machine_type, track_circuit_systems, center_frequency, um71_category, block_mode,
                         remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(signal_machine_position)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return info

    def query_electric_phase_data(self, line_name, direction):
        '''查询 电务数据-接触网分相表 表数据
            :param line_name: 线路名称
            :param direction: 行别名称
        '''
        info = {}
        try:
            sqlStr = """
              select 
                 railway_name                     -- 路局名称
                ,railway_code                     -- 路局代码
                ,line_name                        -- 线路名称
                ,line_code                        -- 线路代码
                ,diff_lines                       -- 行别
                ,train_station_name               -- 站点名称
                ,start_mileage                    -- 起点里程
                ,end_mileage                      -- 终点里程
                ,center_mileage                   -- 中心里程
                ,phase_structure                  -- 分相结构
                ,remarks                          -- 备注标记
              from ods_line_electric_phase_data
              where line_name = '%s' and  diff_lines ='%s'
            """ % (line_name, direction)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
            for data in data_list:
                railway_name, railway_code, line_name, line_code, diff_lines, train_station_name, \
                start_mileage, end_mileage, center_mileage, phase_structure, remarks = data

                start_mileage = int(float(start_mileage) * 1000)
                end_mileage = int(float(end_mileage) * 1000)
                center_mileage = int(float(center_mileage) * 1000)

                dList = [railway_name, railway_code, line_name, diff_lines, train_station_name, center_mileage,
                         phase_structure, remarks]
                if diff_lines == '上行':
                    diff_lines_list = ['0']
                elif diff_lines == '下行':
                    diff_lines_list = ['1']
                elif diff_lines == '双':
                    diff_lines_list = ['0', '1']
                for code in diff_lines_list:
                    keyId = line_code.strip() + '-' + code.strip() + '_' + str(start_mileage) + '-' + str(end_mileage)
                    info[keyId] = dList
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return info

    def savePhaseData(self, condition):
        """
        功能：将数据更新至数据库
        :param condition: 插入数据
        :return:
        """
        try:
            cur = self.conn.cursor()
            insStr = 'replace into train_line_phase_data_valid_info values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
            cur.execute(insStr, condition)
            self.conn.commit()
        except Exception:
            print('%s MySQL插入数据失败[%s]  %s' % (datetime.datetime.now(), insStr, traceback.format_exc()))
        finally:
            cur.close()  # 关闭游标

    def queryPhaseData(self, line_code, direction_code):
        '''查询 电务数据-电务信号表 表数据
            :param line_code: 线路编号
            :param direction_code: 行别编号
        '''
        try:
            sqlStr = """
              select 
                 line_code              -- 线路编号  
                ,direction_code         -- 行别编号  
                ,train_no               -- 列号
                ,coach_id               -- 车号
                ,mile                   -- 里程
                ,netVolt                -- 网压
                ,fTime                  -- 时间
                ,start_mile             -- 接触网分相起点里程
                ,end_mile               -- 接触网分相终点里程
                ,phase_valid            -- 接触网分相是否在起点终点之间
              from train_line_phase_data_valid_info
              where line_code = '%s' and  direction_code ='%s'
            """ % (line_code, direction_code)
            cursor = self.conn.cursor()
            d = cursor.execute(sqlStr)
            data_list = cursor.fetchmany(d)
        except Exception:
            print('%s Hive获取数据异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit(1)
        finally:
            cursor.close()  # 关闭游标
        return data_list





if __name__ == '__main__':
    db = MySQLData()

    line_name = '京沪高铁'
    direction = '上行'
    # direction = '双'
    # data = db.query_work_tunnel_data(line_name, direction)
    # data = db.query_work_bridge_data(line_name, direction)
    # data = db.query_work_curve_data(line_name, direction)
    # data = db.query_work_speedLimit_data(line_name, direction)
    # data = db.query_work_ramp_data(line_name, direction)
    # data = db.query_work_turnout_data(line_name, direction)
    # data = db.query_electric_signal_data(line_name, direction)
    data = db.query_electric_phase_data(line_name, direction)
    for k in data:
        print(k, data[k])

    # info = db.queryParamFromTrainRules('32')
    # print('32', info)
    # info = db.queryParamFromTrainRules('37')
    # print('37', info)
