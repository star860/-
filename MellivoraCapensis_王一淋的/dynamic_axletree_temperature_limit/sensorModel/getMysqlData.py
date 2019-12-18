# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 程    序：getMysqlData.py                                          #
# 创建时间：2016年07月18日                                           #
# 创 建 者：zhanggh                                                  #
# 参数:                                                              #
#                                                                    #
# 补充说明：                                                         #
# 功能：从MySQL数据库查询数据                                        #
# 调用示例：其它脚本调用                                             #
#                                                                    #
######################################################################

import sys, MySQLdb, datetime
import param_conf
import traceback

reload(sys)
sys.setdefaultencoding('utf-8')
# mysql连接参数
hostName = '10.73.95.29'
port = 3306
# userName = 'root'
# password = 'root123'
# userName = 'phmdm'
# password = '0l5imxhCr4gbgw=='
userName = 'phmconf'
password = 'FPW/JGN9vmml3Q=='
dbName = 'webdata'

class MySQLData():
    """功能：从MySQL数据库查询数据"""

    def __init__(self):
        try:  # 连接MySQL数据库获取数据
            self.conn = MySQLdb.connect(
                host=hostName,
                port=port,
                user=userName,
                passwd=password,
                db=dbName,
                charset='utf8'
            )
        except Exception as e:
            print '%s MySQL数据库连接异常, 请确认MySQL服务端[主机 wtds-mysql-01 用户名 root 数据库 webdata]:  %s' % (
                datetime.datetime.now(), e)
            sys.exit()

    def __del__(self):
        '''功能: 关闭数据库边接'''
        self.conn.close()

    def queryGlideConfig(self):
        '''功能: 从MySQL查询数据'''
        mysqlData = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select data_code, 
                   data_name, 
                   control_data_code, 
                   display_code 
              from dim_phm_trunscate_rules
             where interface_type in('32','3A')
               and display_code>'Z02'
               and display_code<'Z39'"""
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                mysqlData[item[0] + '_' + item[1] + '_' + item[2]] = item[3]
        except Exception, e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            sys.exit()
        finally:
            cur.close()  # 关闭游标

        if len(mysqlData) > 0:
            return mysqlData

    def queryOnlineMonitor(self, condition):
        '''功能：从MySQL实时监控数据'''
        recordNum = 0
        try:
            cur = self.conn.cursor()
            sqlStr = 'select count(1) record_num from zt_i3e_online_monitor where ' + condition
            cur.execute(sqlStr)
            recordNum = cur.fetchone()
        except Exception, e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            recordNum = 0
        finally:
            cur.close()  # 关闭游标

        return recordNum

    def getmysqldata(self,wtype):
        mysqldata = {}
        if self.conn:
            cur = self.conn.cursor()
            try:
                t1 = datetime.datetime.now() - datetime.timedelta(days=6)
                startDt = t1.strftime("%Y%m%d")
                sqlStr ="""
                        SELECT 
                            CONCAT_WS('_',train_type_code,train_no,coach_id,warning_type,warn_flag) 
                            ,op_day
                        FROM  zt_i3e_online_monitor 
                        where warning_type ='"""

                sqlStr += wtype + '\' and op_day >' + startDt

                d = cur.execute(sqlStr)
                data = cur.fetchmany(d)
                for item in data:
                    if str(item[0]) not in mysqldata:
                        mysqldata[str(item[0])] = [str(item[1])]
                    elif str(item[1]) not in mysqldata[str(item[0])]:
                        mysqldata[str(item[0])].append(str(item[1]))
            except Exception, e:
                print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            finally:
                cur.close()  # 关闭游标

            return mysqldata

    def updateOnlineMonitor(self, condition):
        '''功能：将数据更新至数据库'''
        try:
            cur = self.conn.cursor()
            insStr = 'replace into zt_i3e_online_monitor values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) '
            cur.execute(insStr, condition)
            self.conn.commit()
        except Exception as e:
            print '%s MySQL插入数据失败[%s]  %s' % (datetime.datetime.now(), insStr, e)
        finally:
            cur.close()  # 关闭游标

    def queryBlackList(self):
        '''功能: 查询黑名单数据'''
        blackList = []
        try:
            cur = self.conn.cursor()
            sqlStr = "select train_id from dm_nowarn_train_serv_mm where warn_status='1'"
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                blackList.append(item[0])
        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return blackList

    def querySensorCode(self):
        '''功能: 查询轴温配置数据'''
        info = {}
        sensorKind = param_conf.p_sensor
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type,
                   t1.data_code,
                   t1.control_data_code, 
                   t1.display_code,
                   t2.is_long
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type in('32','3A')
               and t1.display_code in("""

            flag = 0
            for k in sensorKind.keys():
                if flag == 0:
                    sqlStr = sqlStr + "'" + k + "'"
                    flag = 1
                else:
                    sqlStr = sqlStr + ",'" + k + "'"

            sqlStr = sqlStr + ")"
            sqlStr = sqlStr + """
                 and t1.train_type=t2.type_code
               order by t1.train_type,t1.display_code
            """

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in sensorKind:
                    if sensorKind[item[3]] in ('speed', 'out_degree', 'trainsite_id', 'brake_pos'):  # 速度
                        sk = sensorKind[item[3]]
                    else:  # 处理轴温温度值
                        sk = 'sensor_value_' + sensorKind[item[3]]

                    info[str(item[1])] = [str(item[0]), sk, str(item[4])]  # 车型, 传感器位置, 长/短编组

                    if len(item[2]) > 0 and sensorKind[item[3]] not in (
                            'speed', 'out_degree', 'trainsite_id', 'brake_pos'):  # 处理传感器状态
                        sk = 'sensor_status_' + sensorKind[item[3]]
                        info[str(item[2])] = [str(item[0]), sk, str(item[4])]  # 车型, 传感器位置, 长/短编组

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info

    def querySensorMiniCode(self):
        '''功能: 查询轴温配置数据'''
        info = {}
        sensorKind = param_conf.p_sensor_mining
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type,
                   t1.data_code,
                   t1.control_data_code,
                   t1.display_code,
                   t2.is_long
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type in('32','3A')
               and t1.display_code in("""

            flag = 0
            for k in sensorKind.keys():
                if flag == 0:
                    sqlStr = sqlStr + "'" + k + "'"
                    flag = 1
                else:
                    sqlStr = sqlStr + ",'" + k + "'"

            sqlStr = sqlStr + ")"
            sqlStr = sqlStr + """
                 and t1.train_type=t2.type_code
               order by t1.train_type,t1.display_code
            """

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in sensorKind:
                    if sensorKind[item[3]] in (
                            'speed', 'out_degree', 'trainsite_id', 'brake_pos', 'trainsite_valid', 'as_press',
                            'plus_minus_speed'):  # 速度
                        sk = sensorKind[item[3]]
                    else:  # 处理轴温温度值
                        sk = 'sensor_value_' + sensorKind[item[3]]

                    info[str(item[1])] = [str(item[0]), sk, str(item[4])]  # 车型, 传感器位置, 长/短编组

                    if len(item[2]) > 0 and sensorKind[item[3]] not in (
                            'speed', 'out_degree', 'trainsite_id', 'brake_pos', 'trainsite_valid', 'as_press',
                            'plus_minus_speed'):  # 处理传感器状态
                        sk = 'sensor_status_' + sensorKind[item[3]]
                        info[str(item[2])] = [str(item[0]), sk, str(item[4])]  # 车型, 传感器位置, 长/短编组

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info

    def querySensorPos(self):
        '''功能: 查询轴温对应位置，应用于程序onlineToHBase.py中变量sensorCodeList'''
        info = {}
        sensorKind = param_conf.p_sensor
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type,
                   t1.data_code,
                   t1.display_code,
                   t1.coach_no,
                   t1.wtds16_coach_no
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type in('32')
               and t1.display_code in("""

            flag = 0
            for k in sensorKind.keys():
                if k not in ('A09', 'H25', 'A06', 'A13', 'A02'):
                    if flag == 0:
                        sqlStr = sqlStr + "'" + k + "'"
                        flag = 1
                    else:
                        sqlStr = sqlStr + ",'" + k + "'"

            sqlStr = sqlStr + ")"
            sqlStr = sqlStr + """
                 and t1.train_type=t2.type_code
               order by t1.train_type,t1.coach_no,t1.display_code
            """

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                pos = sensorKind[item[2]]
                if len(pos) == 1:
                    pos = '0' + pos

                # 短编组
                keyId = str(item[0]) + '_' + str(item[3]) + '_' + pos
                info[str(keyId)] = str(item[1])

                if len(str(item[4])) > 0:  # 长编组
                    keyId = str(item[0]) + '_' + str(item[4]) + '_' + pos
                    info[str(keyId)] = str(item[1])

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            print traceback.format_exc()
        finally:
            cur.close()  # 关闭游标

        return info

    def querySdrParamCode(self):
        '''功能: 查询SDR参数编码'''
        info = {}
        trainTypeMapParam = {}
        paramMapping = {
            'C01': 'vcb_status', 'C04': 'volt3_value', 'C09': 'pantograph_check', 'D01': 'battery_v',
            'A09': 'speed'
        }
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
             where t1.interface_type in('32','3A')
               and t1.display_code in('C01','C04', 'C09', 'D01', 'A09')
               and t1.train_type=t2.type_code
             order by t1.train_type,t1.display_code"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in paramMapping:
                    colName = paramMapping[item[3]]
                    if item[5] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(item[5])
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = item[5]

                    if item[6] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(item[6])
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = item[6]

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info, trainTypeMapParam

    def querySdrParam(self):
        '''功能: 查询SDR参数'''
        params = param_conf.p_sdr  # 导入参数

        info = {}
        trainTypeMapParam = {}

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
             where t1.interface_type in('32','3A')
               and t1.display_code in(
            """

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

                    # if item[5]:
                    if item[5] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # print item[6]
                    # if item[6]:
                    if item[6] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info, trainTypeMapParam

    def queryParamMapCoach(self):
        '''功能: 查询SDR参数与车号的映射关系'''
        params = param_conf.p_sdr  # 导入参数

        info = {}

        try:
            cur = self.conn.cursor()

            sqlStr = """
            select t1.train_type,
                   t1.display_code,
                   t2.is_long,
                   t1.coach_no,
                   t1.wtds16_coach_no
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type in('32','3A')
               and t1.display_code in(
            """

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
            flag = 0

            for item in data:
                if item[1] in params:
                    colName = params[item[1]]

                    if colName in info:
                        if item[0] in info[colName]:
                            # if len(item[3]) > 0:
                            if item[3] <> '':
                                info[colName][item[0]].append(str(item[3]))
                            # if len(item[4]) > 0:
                            if item[4] <> '':
                                info[colName][item[0]].append(str(item[4]))
                        else:
                            info[colName][str(item[0])] = []

                            # if len(item[3]) > 0:
                            if item[3] <> '':
                                info[colName][item[0]].append(str(item[3]))
                            # if len(item[4]) > 0:
                            if item[4] <> '':
                                info[colName][item[0]].append(str(item[4]))
                    else:
                        info[str(colName)] = {str(item[0]): []}
                        # if len(item[3]) > 0:
                        if item[3] <> '':
                            info[colName][item[0]].append(str(item[3]))
                        # if len(item[4]) > 0:
                        if item[4] <> '':
                            info[colName][item[0]].append(str(item[4]))

        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info

    def queryCulParamCode(self):
        '''功能: 查询累计空转、滑行参数编码'''
        info = {}
        paramMapping = {
            'K01': 'glide_flag', 'K03': 'glide1_num', 'K04': 'glide2_num', 'K05': 'glide3_num', 'K06': 'glide4_num'
        }
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
             where t1.interface_type in('3E')
               and t1.display_code in('K01','K03','K04', 'K05', 'K06')
               and t1.train_type=t2.type_code
             order by t1.train_type,t1.display_code"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in paramMapping:
                    colName = paramMapping[item[3]]
                    info[str(item[1])] = [str(item[0]), colName, str(item[5])]  # 车型, 字段名称, 车号


        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info

    def queryGlideParam(self):
        '''功能: 查询累计空转、滑行参数编码'''
        params = param_conf.p_glide  # 导入参数
        info = {}

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
             where t1.interface_type in('3E')
               and t1.display_code in(
            """

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

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in params:
                    colName = params[item[3]]
                    info[str(item[1])] = [str(item[0]), colName, str(item[5])]  # 车型, 字段名称, 车号


        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info

    def insertTaskData(self, scriptName, dataDate, status, dataStatus):
        """功能：调度任务数据插入MySQL数据库"""
        try:
            # 查询数据库数据
            recordNum = 0
            updateFlag = 0

            cur = self.conn.cursor()
            sqlStr = """
            select count(1)
              from sch_task_config t1
                   ,sch_task_status t2
             where t1.task_id=t2.task_id
               and t1.script_name='""" + scriptName + """'
               and t2.data_date='""" + dataDate + """'
            """

            cur.execute(sqlStr)
            recordNum = int(cur.fetchone()[0])

            # 获取当前时间
            timeNow = str(datetime.datetime.now())[0:19]

            if recordNum == 0:
                # 更新数据至数据库
                sqlStr = """
                insert into sch_task_status(
                  task_id
                  ,group_id
                  ,task_name
                  ,table_name
                  ,task_description
                  ,status
                  ,data_status
                  ,start_time
                  ,end_time
                  ,data_date
                )
                select t1.task_id
                       ,t1.group_id
                       ,t1.task_name
                       ,t1.table_name
                       ,t1.task_description
                       ,'""" + status + """'
                       ,'""" + dataStatus + """'
                       ,'""" + timeNow + """'
                       ,''
                       ,'""" + dataDate + """'
                  from sch_task_config t1
                 where t1.script_name='""" + scriptName + """'
                """
                updateFlag = 1
            elif recordNum > 0:
                # 修改数据至数据库
                sqlStr = """
                update sch_task_status
                   set status = '""" + status + """',
                       data_status = '""" + dataStatus + """',
                       end_time = '""" + timeNow + """'
                 where task_id in(select task_id
                                    from sch_task_config
                                   where script_name='""" + scriptName + """')
                   and data_date='""" + dataDate + """'
                """
                updateFlag = 1

            # 数据修改标志为1，则更新数据
            if updateFlag == 1:
                cur = self.conn.cursor()
                cur.execute(sqlStr)
                self.conn.commit()
        except Exception as e:
            print '%s MySQL插入数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

    def getParamCoach(self):
        '''功能: 查询相关参数, 并导出至配置文件'''
        paramGroup = dict()

        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type,
                   t1.display_code,
                   t1.coach_no,
                   t1.wtds16_coach_no,
                   t2.is_long
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.train_type=t2.type_code
               and t1.display_code in('B01', 'B02', 'B03', 'B05', 'C02', 'C03', 'C04', 'C05', 'C06', 'C07', 'D01')
             order by t1.train_type, t1.display_code, t1.coach_no"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)

            for item in data:
                newId = item[0] + '_' + item[1]
                if newId not in paramGroup:
                    paramGroup[str(newId)] = [str(item[2])]
                else:
                    paramGroup[str(newId)].append(str(item[2]))

                if item[4] == '1':  # 处理长编组
                    paramGroup[str(newId)].append(str(item[3]))

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return paramGroup

    def queryMro(self):
        """功能: 查询MRO获取当前所有列车及状态"""
        mroList = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type,
                   t1.train_id,
                   t1.train_status_code,
                   t2.is_long
              from dm_mro_train_status_ds t1,
                   dim_phm_train_type t2
             where t1.train_type=t2.type_code"""
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                mroList[str(item[1])] = [str(item[3]), str(item[0])]

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return mroList

    def queryFaultParam(self):
        '''功能: 查询故障参数'''
        params = param_conf.p_fault  # 导入参数

        info = {}
        trainTypeMapParam = {}

        try:
            cur = self.conn.cursor()

            sqlStr = """
            select t1.train_type,
                   t1.data_code,
                   t1.control_data_code, 
                   t1.data_name,
                   t2.is_long,
                   t1.coach_no,
                   t1.wtds16_coach_no
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type in('33')
               and t1.data_name in(
            """

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
               order by t1.train_type,t1.data_name
            """

            # 获取数据结果集
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in params:
                    colName = params[item[3]]

                    # if len(item[5]) > 0:
                    if item[5] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # if len(item[6]) > 0:
                    if item[6] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info, trainTypeMapParam

    def queryE6AE6F(self):
        '''功能: 列车单元数据划分'''
        tu = {}
        trainTypeNum = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select  train_type_code
                    ,coach_id       
                    ,power_unit_no  
                    ,coach_type     
                    ,coach_type_no  
                    ,brake_unit_no  
                    ,is_battery     
                    ,is_VCB         
                    ,is_APU         
                    ,is_ACM      
              from dim_dm_train_unit
              where train_type_code in ('E6A','E6F')
              order by train_type_code, coach_id"""
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                tu[str(item[0]) + '_' + str(item[1])] = [str(item[2]), str(item[3]), str(item[4]), str(item[5]),
                                                         str(item[6]), str(item[7]), str(item[8]), str(item[9])]

            # 车型对应的车厢数
            sqlStr = """select train_type_code
                               ,count(1) num
                          from dim_dm_train_unit
                          where train_type_code in ('E6A','E6F')
                         group by train_type_code"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                trainTypeNum[str(item[0])] = int(item[1])
                # trainTypeNum[str(item[0])] = 8
        except Exception, e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            sys.exit()
        finally:
            cur.close()  # 关闭游标

        if len(tu) > 0:
            return tu, trainTypeNum

    def queryTrainUnit(self):
        '''功能: 列车单元数据划分'''
        tu = {}
        trainTypeNum = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select  train_type_code
                    ,coach_id       
                    ,power_unit_no  
                    ,coach_type     
                    ,coach_type_no  
                    ,brake_unit_no  
                    ,is_battery     
                    ,is_VCB         
                    ,is_APU         
                    ,is_ACM
                    ,cm_APU       
              from dim_dm_train_unit
              where train_type_code not in ('E6A' ,'E6A3' ,'E6B' ,'GSYE19' ,'E6F' ,'E6A4' ,'GSYE21' ,'E6F4')
              order by train_type_code, coach_id"""
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                tu[str(item[0]) + '_' + str(item[1])] = [str(item[2]), str(item[3]), str(item[4]), str(item[5]),
                                                         str(item[6]), str(item[7]), str(item[8]), str(item[9]),
                                                         str(item[10])]

            # 车型对应的车厢数
            sqlStr = """select train_type_code
                               ,count(1) num
                          from dim_dm_train_unit
                          where train_type_code not in ('E6A' ,'E6A3' ,'E6B' ,'GSYE19' ,'E6F' ,'E6A4' ,'GSYE21' ,'E6F4')
                         group by train_type_code"""

            # # 车型对应的车厢数(前台表)
            # sqlStr = """select TRAIN_MODEL_SUB_CODE
            #                  ,count(*)
            #             from train_model_coach
            #             group by TRAIN_MODEL_SUB_CODE"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                trainTypeNum[str(item[0])] = int(item[1])
                # trainTypeNum[str(item[0])] = 8
        except Exception, e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            sys.exit()
        finally:
            cur.close()  # 关闭游标

        if len(tu) > 0:
            return tu, trainTypeNum

    def queryTrainType(self):
        '''功能: 获取列车车型大类'''
        trainType = {}
        try:
            cur = self.conn.cursor()

            # 车型对应的车型大类
            sqlStr = """select train_type
                               ,train_type_code
                          from dim_dm_train_unit"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                trainType[str(item[1])] = str(item[0])
        except Exception, e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
            sys.exit()
        finally:
            cur.close()  # 关闭游标

        if len(trainType) > 0:
            return trainType

    def queryCoachType(self):
        """功能: 查询车厢类型"""
        ct = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type_code,
                   t1.coach_id,
                   t1.coach_type
              from dim_dm_train_unit t1
             group by t1.train_type_code, t1.coach_id"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)

            for item in data:
                if item[0] not in ct:
                    if item[2] == 'M':
                        ct[str(item[0])] = [1, []]
                    else:
                        ct[str(item[0])] = [1, [str(item[1])]]

                else:
                    ct[str(item[0])][0] = ct[str(item[0])][0] + 1
                    if item[2] == 'T':
                        ct[str(item[0])][1].append(str(item[1]))

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return ct

    def queryZhouwenTypeDefine(self):
        """功能: 查询事件中心配置表"""
        ztd = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.type_id,
                   t1.type_node
              from dim_zhouwen_type_define t1"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)

            for item in data:
                ztd[item[1]] = str(item[0])

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return ztd

    def queryAirParam(self):
        '''功能: 查询空调参数'''
        params = param_conf.p_air  # 导入参数

        info = {}
        trainTypeMapParam = {}

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
             where t1.interface_type in('32','3A')
               and t1.train_type in ('E32B','E32')
               and t1.display_code in(
            """

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

                    # if item[5]:
                    if item[5] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # print item[6]
                    # if item[6]:
                    if item[6] <> '':
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标
        print(sqlStr)
        return info, trainTypeMapParam

    def queryTractionParam(self):
        '''功能: 查询空调参数'''
        params = param_conf.p_traction  # 导入参数

        info = {}
        trainTypeMapParam = {}

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
             where t1.interface_type in('32','3A')
               and t1.train_type in ('E32B','E32')
               and t1.display_code in(
            """

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

                    # if item[5]:
                    if len(item[5]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # print item[6]
                    # if item[6]:
                    if len(item[6]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标
        print(sqlStr)
        return info, trainTypeMapParam

    def getCoachPower(self):
        '''功能: 查询相关参数, 并导出至配置文件'''
        powerInfo = dict()

        try:
            cur = self.conn.cursor()
            sqlStr = """
            select train_model_code
                   ,coach_no
                   ,power_flag
            from train_model_coach
            order by train_model_code,coach_no"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)

            for item in data:
                coach = str(item[1][1:])
                powerInfo[str(item[0]) + '_' + coach] = str(item[2])

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return powerInfo

    def querySdrExtParam(self):
        '''功能: 查询SDR参数'''
        params = param_conf.p_sdrext  # 导入参数

        info = {}
        trainTypeMapParam = {}

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
             where t1.interface_type in('32','3A')
               and t1.display_code in(
            """

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

                    # if item[5]:
                    if item[5] <> '':
                        # if len(item[5]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # print item[6]
                    # if item[6]:
                    if item[6] <> '':
                        # if len(item[6]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info, trainTypeMapParam

    def queryFaultExtParam(self):
        '''功能: 查询标动故障参数编码'''
        info = {}
        paramMapping = {
            'N06': 'fault_status'
        }
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.train_type,
                   t1.data_code,
                   t1.control_data_code,
                   t1.display_code
              from dim_phm_trunscate_rules t1,
                   dim_phm_train_type t2
             where t1.interface_type in('31')
               and t1.display_code in('N06')
               and t1.train_type=t2.type_code
               and t1.train_type in('E32','E32A','E32B','E6F','E6F4','E6A4')
             order by t1.train_type,t1.display_code"""

            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                if item[3] in paramMapping:
                    colName = paramMapping[item[3]]

                    info[str(item[1])] = [str(item[0]), colName]  # 车型, 字段名称, 长/短编组

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info

    def queryParamE44(self):
        '''功能: 查询SDR参数'''
        params = param_conf.p_E44  # 导入参数

        info = {}
        trainTypeMapParam = {}

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
             where t1.interface_type in('32','3A')
               and t1.display_code in(
            """

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

                    # if item[5]:
                    if item[5] <> '':
                        # if len(item[5]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # print item[6]
                    # if item[6]:
                    if item[6] <> '':
                        # if len(item[6]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])

                    info[str(item[1])] = [str(item[0]), colName, str(item[4])]  # 车型, 字段名称, 长/短编组


        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return info, trainTypeMapParam

    def queryTrainMroStatus(self):
        """功能: 查询MRO获取当前所有列车及状态"""
        mroList = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
            select t1.rail_area_name,
                   t1.train_id,
                   t1.train_status_code,
                   t1.train_status
            from dm_mro_train_status_ds t1"""
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                mroList[str(item[1])] = [str(item[0]), str(item[2]), str(item[3])]

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return mroList

    def queryParamSpecial(self, params):
        '''功能: 查询SDR参数'''

        info = {}
        trainTypeMapParam = {}

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
             where t1.interface_type in('32','3A')
               and t1.display_code in(
            """

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

                    # if item[5]:
                    if item[5] <> '':
                        # if len(item[5]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[5]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[5])

                    # print item[6]
                    # if item[6]:
                    if item[6] <> '':
                        # if len(item[6]) > 0:
                        if item[0] in trainTypeMapParam:
                            trainTypeMapParam[str(item[0]) + '_' + colName].append(str(item[6]))
                        else:
                            trainTypeMapParam[str(item[0]) + '_' + colName] = str(item[6])




        # except Exception as e:
        #     print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return trainTypeMapParam


    def queryWarnTypeInfo(self):
        """功能: 查询模型warnType信息"""
        infoList = {}
        try:
            cur = self.conn.cursor()
            sqlStr = """
                select fault_code,
                    fault_name,
                    fault_reason
                from dim_phm_fault_detail 
                where fault_code like '%m%' """
            d = cur.execute(sqlStr)
            data = cur.fetchmany(d)
            for item in data:
                infoList[str(item[1])] = [str(item[0]),str(item[2])]

        except Exception as e:
            print '%s MySQL查询数据失败[%s]  %s' % (datetime.datetime.now(), sqlStr, e)
        finally:
            cur.close()  # 关闭游标

        return infoList

    def updateWarnTypeInfo(self, condition):
        '''功能：将模型warnType信息更新至表'''
        try:
            cur = self.conn.cursor()
            insStr = 'insert into dim_phm_warn_type_define(warn_type,type_id) values(%s,%s)'
            cur.execute(insStr, condition)
            self.conn.commit()
        except Exception as e:
            print '%s MySQL插入数据失败[%s]  %s' % (datetime.datetime.now(), insStr, e)
        finally:
            cur.close()  # 关闭游标

if __name__ == '__main__':
    db = MySQLData()
    info = db.getmysqldata('sensorComp1')
    print info.keys()
    info2 = db.getmysqldata('sensorComp2')
    print info2.keys()

    keyms = 'E12_2543_09_sensorComp1_'
    print keyms in info
    print keyms in info2
