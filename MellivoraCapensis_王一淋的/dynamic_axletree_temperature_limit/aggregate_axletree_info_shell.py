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
import hive_handler
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
        self.hive_handler = hive_handler.HiveHandler()

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

        # 整车数据
        self.total_display_code_dict = {1: 'A09',  # 速度
                                        2: 'A10',  # 里程
                                        }
        # 所有车厢都有的数据
        self.coach_display_code_dict = {3: 'Z03',  # 1位轴箱温度(默认取1路)
                                        4: 'Z04',  # 2位轴箱温度(默认取1路)
                                        5: 'Z05',  # 3位轴箱温度(默认取1路)
                                        6: 'Z06',  # 4位轴箱温度(默认取1路)
                                        7: 'Z07',  # 5位轴箱温度(默认取1路)
                                        8: 'Z08',  # 6位轴箱温度(默认取1路)
                                        9: 'Z09',  # 7位轴箱温度(默认取1路)
                                        10: 'Z10',  # 8位轴箱温度(默认取1路)
                                        99: 'G17',  # 新风温度检测值
                                        }
        # 只有动车才有的轴承
        self.mcoach_display_code_dict = {11: 'Z11',  # 1轴小齿轮箱电机侧轴承温度
                                         12: 'Z12',  # 2轴小齿轮箱电机侧轴承温度
                                         13: 'Z13',  # 3轴小齿轮箱电机侧轴承温度
                                         14: 'Z14',  # 4轴小齿轮箱电机侧轴承温度
                                         15: 'Z15',  # 1轴小齿轮箱车轮侧轴承温度
                                         16: 'Z16',  # 2轴小齿轮箱车轮侧轴承温度
                                         17: 'Z17',  # 3轴小齿轮箱车轮侧轴承温度
                                         18: 'Z18',  # 4轴小齿轮箱车轮侧轴承温度
                                         19: 'Z19',  # 1轴电机定子温度
                                         20: 'Z20',  # 1轴电机传动端轴承温度
                                         21: 'Z21',  # 1轴电机非传动端轴承温度
                                         22: 'Z22',  # 2轴电机定子温度
                                         23: 'Z23',  # 2轴电机传动端轴承温度
                                         24: 'Z24',  # 2轴电机非传动端轴承温度
                                         25: 'Z25',  # 3轴电机定子温度
                                         26: 'Z26',  # 3轴电机传动端轴承温度
                                         27: 'Z27',  # 3轴电机非传动端轴承温度
                                         28: 'Z28',  # 4轴电机定子温度
                                         29: 'Z29',  # 4轴电机传动端轴承温度
                                         30: 'Z30',  # 4轴电机非传动端轴承温度
                                         31: 'Z31',  # 1轴大齿轮箱电机侧轴承温度
                                         32: 'Z32',  # 2轴大齿轮箱电机侧轴承温度
                                         33: 'Z33',  # 3轴大齿轮箱电机侧轴承温度
                                         34: 'Z34',  # 4轴大齿轮箱电机侧轴承温度
                                         35: 'Z35',  # 1轴大齿轮箱车轮侧轴承温度
                                         36: 'Z36',  # 2轴大齿轮箱车轮侧轴承温度
                                         37: 'Z37',  # 3轴大齿轮箱车轮侧轴承温度
                                         38: 'Z38',  # 4轴大齿轮箱车轮侧轴承温度
                                         }

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

    def get_data_code(self, train_type, coach_no, display_code):
        """
        根据display_code查询data_code
        :param train_type: 列车类型
        :param coach_no: 车厢号
        :param display_code: 要获取的数据的display_code
        """
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
        # 关闭数据库连接,防止因连接时间过长被杀死
        self.mq_prd_connection.close()
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
        self.hive_handler.exec_sql(sql)
        openstringinfo_tuples = self.hive_prd_cursor.fetchall()
        return openstringinfo_tuples

    def truncate_target_table(self):
        """
        清除目标表数据
        :return:
        """
        hive_sql = '''  insert overwrite table real_time_all_axletree_temperature
                        values('-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-','-') 
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

    def get_coach_type(self):
        """
        获取车厢类型(动车或拖车)
        :return:
        """
        # 标动 2,4,5,7是动车,其他是拖车
        coach_type_dict = {'01': 'T',
                           '02': 'M',
                           '03': 'T',
                           '04': 'M',
                           '05': 'M',
                           '06': 'T',
                           '07': 'M',
                           '08': 'T',
                           '09': 'T',
                           '10': 'M',
                           '11': 'T',
                           '12': 'M',
                           '13': 'M',
                           '14': 'T',
                           '15': 'M',
                           '16': 'T',
                           }
        return coach_type_dict

    def get_train_speed_mileage(self, train_type, column_00, display_code):
        """
        根据00车的数据获取列车的准确的速度和里程信息
        :param train_type:列车类型
        :param column_00:列字典
        :param display_code:对应的display_code
        :return:
        """
        # 获取对应的data_code
        data_code = self.get_data_code(train_type, '00', display_code)[1]

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
        self.truncate_target_table()

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

                # 根据列车的运行开始和结束时间扫描hbase表
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
                    train_speed = self.get_train_speed_mileage(train_type, column_00, self.total_display_code_dict[1])
                    train_mileage = self.get_train_speed_mileage(train_type, column_00, self.total_display_code_dict[2])
                    # 获取列车运行的实时时间
                    real_time = re.search(r'_(\d{14})', rowkey_00).group(1)
                    # 获取车型对应的动拖车信息,默认都为标动
                    coach_type_dict = self.get_coach_type()

                    for culumn_family in ['F_DATA', 'S_DATA', 'T_DATA', 'FO_DATA']:
                        # 依次判断四个列族是否存在
                        if '{}:TRAIN_TYPE'.format(culumn_family) in column_00:

                            # 不同的列族记录的数据对应不同的车厢号
                            # 获取车厢号列表
                            coach_list = self.column_family[culumn_family]

                            # 获取每个车厢的信息
                            for coach in coach_list:
                                coach_data_list = []
                                # 转换成对应车厢的rowkey
                                rowkey_coach = rowkey_00.replace('_00_', '_{}_'.format(coach))
                                column_coach = self.table_data_log.row(rowkey_coach)
                                column_coach = self.transform_dict(column_coach)
                                # print('rowkey_coach:{}'.format(rowkey_coach))
                                # print(column_coach)

                                # 获取通用列车数据,新风温度和轴箱温度等
                                for cdn in self.coach_display_code_dict:
                                    temp_dict = {}
                                    temp_dict['display_code'] = self.get_data_code(train_type, coach, self.coach_display_code_dict[cdn])[0]
                                    temp_dict['data_code'] = self.get_data_code(train_type, coach, self.coach_display_code_dict[cdn])[1]
                                    temp_dict['data_name'] = self.get_data_code(train_type, coach, self.coach_display_code_dict[cdn])[2]
                                    temp_dict['data_value'] = column_coach[
                                        ('F_DATA:{}'.format(temp_dict['data_code']))]
                                    coach_data_list.append(temp_dict)

                                # 获取车厢类型(动车或拖车)
                                coach_type = coach_type_dict[coach]
                                # 获取动车所有的轴温数据
                                for mcdn in self.mcoach_display_code_dict:
                                    mtemp_dict = {}
                                    if coach_type == 'M':
                                        mtemp_dict['display_code'] = self.get_data_code(train_type, coach, self.mcoach_display_code_dict[mcdn])[0]
                                        mtemp_dict['data_code'] = self.get_data_code(train_type, coach, self.mcoach_display_code_dict[mcdn])[1]
                                        mtemp_dict['data_name'] = self.get_data_code(train_type, coach, self.mcoach_display_code_dict[mcdn])[2]
                                        mtemp_dict['data_value'] = column_coach[
                                            ('F_DATA:{}'.format(mtemp_dict['data_code']))]
                                    else:
                                        mtemp_dict['display_code'] = ' '
                                        mtemp_dict['data_code'] = ' '
                                        mtemp_dict['data_name'] = ' '
                                        mtemp_dict['data_value'] = ' '
                                    coach_data_list.append(mtemp_dict)

                                # 拼接coach_data_list里的信息
                                cdt = ''
                                for dd in coach_data_list:
                                    cdt = cdt + ','.join((dd['display_code'],
                                                          dd['data_code'],
                                                          dd['data_name'],
                                                          dd['data_value']))

                                # 生成行数据
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
                                                 cdt,
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
