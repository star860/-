# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 创建时间：2019年08月09日                                           #
# 创 建 者：wyl                                                      #
# 功能：从kafuka接收过去24小时和未来24小时的天气数据                   #
#                                                                    #
######################################################################


import happybase
import pymysql
import json
from kafka import KafkaConsumer


class DatabaseHandler:
    """
    从kafka中接收数据,写入hbase
    """

    def __init__(self, kfk_topic):
        # 连接hbase数据库,映射到hbase的9090端口(hbase的默认端口为9090)
        self.hb_connection = happybase.Connection(host='10.73.95.71', port=9090)
        self.hb_connection.open()
        self.table = self.hb_connection.table(kfk_topic.upper())
        # 连接mysql数据库
        self.mq_connection = pymysql.connect(host='10.73.95.29',
                                             user='phmconf',
                                             password='FPW/JGN9vmml3Q==',
                                             database='webdata',
                                             port=3307,
                                             # 设置pymysql的字符串格式
                                             charset='utf8')
        self.mq_cursor = self.mq_connection.cursor()
        # kafka
        self.kafka_acceptor = KafkaConsumer(kfk_topic, bootstrap_servers='10.73.95.19:9092')
        self.gps_tuples = tuple()

    def fetch_gps_data(self):
        """
        从mysql获取城市gps信息,放在内存中
        :return:
        """
        mq_sql = '''
                SELECT
                    province_weather, 
                    city_weather, 
                    county_weather,
                    latitude,
                    longitude
                FROM
                    train_sitegps_weather
            '''
        self.mq_cursor.execute(mq_sql)
        self.gps_tuples = self.mq_cursor.fetchall()
        self.mq_connection.close()

    def get_gps(self, column):
        """
        根据column字典取mysql数据库中的gps
        """
        if 'f_data:province' in column:
            province = column['f_data:province']
            city = column['f_data:city']
            district_county = column['f_data:district_county']
            for province_weather, city_weather, county_weather, latitude, longitude in self.gps_tuples:
                if province == province_weather and city == city_weather and district_county == county_weather:
                    column['f_data:latitude'] = latitude
                    column['f_data:longitude'] = longitude
        else:
            province = column['f_data0:province']
            city = column['f_data0:city']
            district_county = column['f_data0:district_county']
            for province_weather, city_weather, county_weather, latitude, longitude in self.gps_tuples:
                if province == province_weather and city == city_weather and district_county == county_weather:
                    column['f_data0:latitude'] = latitude
                    column['f_data0:longitude'] = longitude
        return column

    def write_into_hbase(self):
        """
        依次从kafka中读取数据,写入hbase
        :return:
        """
        self.fetch_gps_data()
        for msg_obj in self.kafka_acceptor:
            msg_tuple = json.loads(msg_obj.value.decode('utf8'))
            rowkey, column_dict = msg_tuple
            column_gps = self.get_gps(column_dict)
            self.table.put(rowkey, column_gps)
