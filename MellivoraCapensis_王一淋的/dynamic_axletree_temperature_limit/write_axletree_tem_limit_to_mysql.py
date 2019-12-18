# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 创建时间：2019年08月15日                                            #
# 创 建 者：wyl                                                       #
# 功能：轴温数据从hive到mysql                                         #
#                                                                    #
######################################################################


import pymysql
from impala.dbapi import connect as hive_connect


class WriteAxletreeTemLimitToMysql:
    """
    将轴温有关的信息关联写入hive表中
    """

    def __init__(self):
        """
        初始化
        """

        # 连接mysql开发数据库
        self.mq_dev_connection = pymysql.connect(host='10.73.82.30',
                                                 user='webuser',
                                                 password='webuser@123',
                                                 database='webdata',
                                                 port=3306,
                                                 # 设置pymysql的字符串格式
                                                 charset='utf8')
        self.mq_dev_cursor = self.mq_dev_connection.cursor()

        # 连接hive开发数据库
        self.hive_dev_connection = hive_connect(host='10.73.95.74',
                                                port=10088,
                                                auth_mechanism='PLAIN',
                                                user='phmdm',
                                                password='phmdm@123',
                                                database='default')
        self.hive_dev_cursor = self.hive_dev_connection.cursor()

    def show_tables(self):
        self.mq_dev_cursor.execute('show tables')
        result = self.mq_dev_cursor.fetchall()
        print(result)

    def get_data_from_hive(self):
        hive_sql = '''
                                SELECT
                                    train_line_code,
                                    line_dire_code,
                                    floor(mileage/1000),
                                    train_type,
                                    train_id,
                                    coach_no,
                                    env_temperature,
                                    axletree_data_name1,
                                    avg(axletree_temperature1),
                                    stddev_pop(axletree_temperature1)
                                FROM
                                    real_time_axletree_temperature
                                GROUP BY
                                    train_line_code,
                                    line_dire_code,
                                    floor(mileage/1000),
                                    train_type,
                                    train_id,
                                    coach_no,
                                    env_temperature,
                                    axletree_data_name1
                            '''

        self.hive_dev_cursor.execute(hive_sql)
        result = self.hive_dev_cursor.fetchall()
        return result

    def write_into_mysql(self, data):
        mq_sql = '''
                                insert into webdata.dynamic_axletree_tem_limit
                                (
                                    `line_code`,
                                    `direction_code`,
                                    `mileage`,
                                    `train_type`,
                                    `train_id`,
                                    `train_coach_no`,
                                    `env_temperature`,
                                    `axletree_type1`,
                                    `axletree_tem_upper_limit1`,
                                    `axletree_tem_lower_limit1`
                                )
                                values
                                ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")
                              '''.format(data[0],
                                         data[1],
                                         data[2],
                                         data[3],
                                         data[4],
                                         data[5],
                                         data[6],
                                         data[7],
                                         int(data[8]) + 3 * int(data[9]),
                                         int(data[8]) - 3 * int(data[9]))
        self.mq_dev_cursor.execute(mq_sql)

    def main(self):
        result = self.get_data_from_hive()
        print(result)
        for data in result:
            data = list(data)
            data[8] = 0 if data[8] is None else data[8]
            data[9] = 0 if data[9] is None else data[9]
            self.write_into_mysql(data)
        self.mq_dev_connection.commit()


if __name__ == '__main__':
    wtm = WriteAxletreeTemLimitToMysql()
    wtm.main()
