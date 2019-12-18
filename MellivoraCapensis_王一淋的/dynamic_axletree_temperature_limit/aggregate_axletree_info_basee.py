# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 创建时间：2019年08月15日                                            #
# 创 建 者：wyl                                                       #
# 功能：动态轴温阈值模型                                              #
#                                                                    #
######################################################################

import re
import pymysql
import happybase
import hdfs_handler
from impala.dbapi import connect
from datetime import datetime


class AggregateAxletreeData:
    """
    将轴温有关的信息关联写入hive表中
    """

    def __init__(self):
        """
        初始化
        """
        # 连接mysql生产数据库
        self.mq_prd_connection = pymysql.connect(host='10.73.95.29',
                                                 user='phmconf',
                                                 password='FPW/JGN9vmml3Q==',
                                                 database='webdata',
                                                 port=3307,
                                                 # 设置pymysql的字符串格式
                                                 charset='utf8')
        self.mq_prd_cursor = self.mq_prd_connection.cursor()

        # 连接hive生产数据库
        self.hive_prd_connection = connect(host='10.73.95.1',
                                           port=10000,
                                           auth_mechanism='PLAIN',
                                           user='phmdm',
                                           password='ebdcea82',
                                           database='default')
        self.hive_prd_cursor = self.hive_prd_connection.cursor()

        # 连接hive开发数据库
        self.hive_dev_connection = connect(host='10.73.95.74',
                                           port=10088,
                                           auth_mechanism='PLAIN',
                                           user='phmdm',
                                           password='phmdm@123',
                                           database='default')
        self.hive_dev_cursor = self.hive_dev_connection.cursor()

        # 连接hbase生产数据库
        self.hb_prd_connection = happybase.Connection(host='10.73.95.1', port=9090, autoconnect=True)
        self.hb_prd_connection.open()
        self.table_data_log = self.hb_prd_connection.table('ODS_PHM_ONLINE_TRAIN_DATA_LOG')
        self.table_gps = self.hb_prd_connection.table('DM_TRAIN_LINE_FEATURE_INFO')

        # 连接hdfs文件系统
        self.hdfs_handler = hdfs_handler.HdfsHandler()

        # 列族对应的车厢号
        self.column_family = {'F_DATA': ['01', '02', '03', '04'],
                              'S_DATA': ['05', '06', '07', '08'],
                              'T_DATA': ['09', '10', '11', '12'],
                              'FO_DATA': ['13', '14', '15', '16'],
                              }
        # 记录开始时间
        self.start_time = datetime.now()

    def end_action(self):
        """
        结束操作
        :return:
        """
        print('#################################################')
        print('Program start time is {}'.format(str(self.start_time).split('.')[0]))
        print('Program end time is {}'.format(str(datetime.now()).split('.')[0]))
        print('Program running time is {} seconds'.format(str(datetime.now() - self.start_time)).split('.')[0])
        print('This program has been finished successfully')

    def get_data_code(self, train_type, coach_no, code_type):
        """
        根据display_code查询data_code
        :param train_type: 列车类型
        :param coach_no: 车厢号
        :param code_type: 要获取的data_code类型(1.速度 2.里程 3.轴温 4.经度 5.纬度 6.新风温度检测值)
        """
        display_code_dict = {1: 'A09',  # 速度
                             2: 'A10',  # 里程
                             3: 'Z03',  # 1路1位轴箱温度
                             4: 'GPS04',  # 经度
                             5: 'GPS06',  # 纬度
                             6: 'G17',  # 新风温度检测值
                             }
        display_code = display_code_dict[code_type]

        sql = '''  SELECT
                        display_code,
                        data_code, 
                        data_name
                    FROM
                        dim_phm_trunscate_rules 
                    WHERE
                        display_code = "{}"
                        AND train_type = "{}"
                        AND coach_no = "{}"
                        '''.format(display_code, train_type, coach_no)
        # print(sql)
        self.mq_prd_cursor.execute(sql)
        result_tuple = self.mq_prd_cursor.fetchone()

        # display_code = result_tuple[0]
        # data_code = result_tuple[1]
        # data_name = result_tuple[2]
        # print('display_code:{}---data_code:{}---data_name:{}'.format(display_code, data_code, data_name))
        return result_tuple

    def transform_dict(self, binary_dict):
        """
        将从hbase取出的二进制字典转化成普通字典
        """
        t_dict = {}
        for bkey in binary_dict:
            key = bkey.decode('utf8')
            value = binary_dict[bkey].decode('utf8')
            t_dict[key] = value
        return t_dict

    def get_train_line_feature(self):
        """
        从mysql获取火车线路信息
        :return:
        """
        # 从mysql数据库取得线路信息
        mq_sql = '''
                            SELECT
                                line_name,
                                line_code,
                                line_direction,
                                line_direction_code,
                                station_start,
                                station_end 
                            FROM
                                webdata.train_line_feature_config
                          '''
        self.mq_prd_cursor.execute(mq_sql)
        train_line_tuples = self.mq_prd_cursor.fetchall()
        return train_line_tuples

    def get_openstringinfo(self, station_start, station_end, start_date, end_date):
        """
        从hive数据库取得开行详细信息
        :return:
        """
        sql = '''   SELECT
                        S_DATE,
                        S_TRAINNO,
                        S_TRAINSETNAME,
                        S_ORDERDIR,
                        S_STARTTIME,
                        S_ENDTIME,
                        S_STARTSTATION,
                        S_ENDSTATION,
                        S_RUNTIME,
                        I_RUNMILE,
                        C_CLFLAG,
                        C_NLLFLAG,
                        C_PULLINFLAG,
                        C_REPAIRFLAG,
                        S_NEXTTRAINNO,
                        TIME 
                    FROM
                        ODS_CUX_PHM_OPENSTRINGINFO_FORSF_V 
                    WHERE
                        S_STARTSTATION = '{}' 
                        AND S_ENDSTATION = '{}' 
                        AND TIME >= '{}' 
                        AND TIME < '{}' 
                        AND op_day = '20190720'
                    '''.format(station_start, station_end, start_date, end_date)
        self.hive_prd_cursor.execute(sql)
        openstringinfo_tuples = self.hive_prd_cursor.fetchall()
        return openstringinfo_tuples

    def truncate_target_table(self):
        """
        清除目标表数据
        :return:
        """
        hive_sql = '''  insert overwrite table real_time_axletree_temperature
                        values('-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-') 
                    '''
        self.hive_dev_cursor.execute(hive_sql)

    def get_train_type(self, column_00):
        """
        获取列车型号
        :return:
        """
        if 'F_DATA:TRAIN_TYPE' in column_00:
            train_type = column_00['F_DATA:TRAIN_TYPE']
        elif 'S_DATA:TRAIN_TYPE' in column_00:
            train_type = column_00['S_DATA:TRAIN_TYPE']
        elif 'T_DATA:TRAIN_TYPE' in column_00:
            train_type = column_00['T_DATA:TRAIN_TYPE']
        elif 'FO_DATA:TRAIN_TYPE' in column_00:
            train_type = column_00['FO_DATA:TRAIN_TYPE']
        else:
            raise Exception('没有找到 TRAIN_TYPE 信息')
        return train_type

    def get_train_speed_mileage(self, train_type, column_00, data_type):
        """
        根据00车的数据获取列车的准确的速度和里程信息
        :param train_type:列车类型
        :param column_00:列字典
        :param data_type:1.速度 2.里程
        :return:
        """
        if data_type == 1:
            # 获取速度对应的data_code
            data_code = self.get_data_code(train_type, '00', 1)[1]
        elif data_type == 2:
            # 获取里程对应的data_code
            data_code = self.get_data_code(train_type, '00', 2)[1]
        else:
            raise Exception('错误的data_type')

        # 从F_DATA开始,哪个有效取哪个,都无效则赋值为0
        if 'F_DATA:{}'.format(data_code) in column_00 \
                and str(column_00['F_DATA:{}'.format(data_code)]) not in ('无效', '0'):
            speed_mileage = column_00['F_DATA:{}'.format(data_code)]
        elif 'S_DATA:{}'.format(data_code) in column_00 \
                and str(column_00['S_DATA:{}'.format(data_code)]) not in ('无效', '0'):
            speed_mileage = column_00['S_DATA:{}'.format(data_code)]
        elif 'T_DATA:{}'.format(data_code) in column_00 \
                and str(column_00['T_DATA:{}'.format(data_code)]) not in ('无效', '0'):
            speed_mileage = column_00['T_DATA:{}'.format(data_code)]
        elif 'FO_DATA:{}'.format(data_code) in column_00 \
                and str(column_00['FO_DATA:{}'.format(data_code)]) not in ('无效', '0'):
            speed_mileage = column_00['FO_DATA:{}'.format(data_code)]
        else:
            speed_mileage = '0'
        return speed_mileage

    def aggregate_data(self, start_date, end_date):
        """
        从各个数据库里面取出数据,关联,写入hive
        """
        # 先清空目标表数据
        # self.truncate_target_table()
        # 从mysql数据库取得线路信息
        train_line_tuples = self.get_train_line_feature()
        for train_line in train_line_tuples:
            train_line_name = train_line[0]
            train_line_code = train_line[1]
            train_line_direction = train_line[2]
            train_line_direction_code = train_line[3]
            station_start = train_line[4]
            station_end = train_line[5]

            # 从hive数据库取得开行详细信息
            openstringinfo_tuples = self.get_openstringinfo(station_start, station_end, start_date, end_date)

            # 取单次运行列车的信息
            for one in openstringinfo_tuples:
                lines_str = ''
                print(one)
                train_id = one[2]
                train_id_rp = train_id.replace('-', '')
                # 获取列车的开行时间和到终点站时间
                start_time = ''.join((one[15][:10].replace('-', ''), one[4].replace(':', ''), '00'))
                end_time = ''.join((one[15][:10].replace('-', ''), one[5].replace(':', ''), '00'))

                # 根据列车的开始时间和结束时间获取对应的00车信息
                s_rowkey_00 = ''.join(('32', '_', train_id_rp, '_', '00', '_', start_time))
                e_rowkey_00 = ''.join(('32', '_', train_id_rp, '_', '00', '_', end_time))
                print('00车开始和结束rowkey:', s_rowkey_00, '--------', e_rowkey_00)
                ts = self.table_data_log.scan(row_start=s_rowkey_00, row_stop=e_rowkey_00)
                scan_list = []
                for row in ts:
                    scan_list.append(row)

                # 根据00车信息关联相关车厢的信息
                for rowkey_00, column_00 in scan_list:
                    # print(rowkey_00, column_00)
                    rowkey_00 = rowkey_00.decode('utf8')
                    column_00 = self.transform_dict(column_00)
                    # 获取列车类型,速度,里程信息
                    train_type = self.get_train_type(column_00)
                    train_speed = self.get_train_speed_mileage(train_type, column_00, 1)
                    train_mileage = self.get_train_speed_mileage(train_type, column_00, 2)
                    # 获取列车运行的实时时间
                    real_time = re.search(r'_(\d{14})', rowkey_00).group(1)

                    for culumn_family in ['F_DATA', 'S_DATA', 'T_DATA', 'FO_DATA']:
                        # 依次判断四个列族是否存在
                        if '{}:TRAIN_TYPE'.format(culumn_family) in column_00:

                            # 不同的列族记录的数据对应不同的车厢号
                            # 获取车厢号列表
                            coach_list = self.column_family[culumn_family]

                            # 获取每个车厢的信息
                            for coach in coach_list:
                                # 转换成对应车厢的rowkey
                                rowkey_coach = rowkey_00.replace('_00_', '_{}_'.format(coach))
                                column_coach = self.table_data_log.row(rowkey_coach)
                                column_coach = self.transform_dict(column_coach)
                                # print('rowkey_coach:{}'.format(rowkey_coach))
                                # print(column_coach)

                                # 获取新风温度
                                fresh_wind_tem_data_code = self.get_data_code(train_type, coach, 6)[1]
                                fresh_wind_tem = column_coach[('F_DATA:{}'.format(fresh_wind_tem_data_code))]
                                # 获取轴温
                                axletree_temperature1_display_code = self.get_data_code(train_type, coach, 3)[0]
                                axletree_temperature1_data_code = self.get_data_code(train_type, coach, 3)[1]
                                axletree_temperature1_data_name = self.get_data_code(train_type, coach, 3)[2]
                                axletree_temperature1 = column_coach[
                                    ('F_DATA:{}'.format(axletree_temperature1_data_code))]

                                line = ','.join((train_line_name,
                                                 train_line_code,
                                                 train_line_direction,
                                                 train_line_direction_code,
                                                 train_speed,
                                                 train_mileage,
                                                 rowkey_coach,
                                                 real_time,
                                                 train_type,
                                                 train_id,
                                                 coach,
                                                 fresh_wind_tem,
                                                 axletree_temperature1_display_code,
                                                 axletree_temperature1_data_code,
                                                 axletree_temperature1_data_name,
                                                 axletree_temperature1,
                                                 str(datetime.now()))) + '\n'
                                # print('-----------------------------------')
                                # print(line)
                                lines_str = ''.join((lines_str, line))

                # 一条开行信息写一次
                self.hdfs_handler.write_into_hdfs_file(lines_str)
                print('\033[1;35;0m写入单次列车成功!!! \033[0m')


if __name__ == '__main__':
    aad = AggregateAxletreeData()
    aad.aggregate_data('2019-01-01', '2019-01-02')
    aad.end_action()
