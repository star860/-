# -*- coding: utf-8 -*-

import time
import happybase


class HbaseHandler:
    def __init__(self):
        # 映射到hbase的9090端口(hbase的默认端口为9090),建立连接
        self.connection = happybase.Connection(host='10.73.95.1', port=9090, autoconnect=True)
        # 打开连接
        self.connection.open()

    def list_tables(self):
        table_list = self.connection.tables()
        for table in table_list:
            print(table)

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

    def insert_data(self, table_name):
        table = self.connection.table(table_name)
        table.put('201908121358', {'f_data:weather_info': '雷阵雨', 'f_data:temperature': '25'})

    def get_temperature(self, date, ):
        pass

    def scan_data(self):
        table = self.connection.table('ODS_PHM_ONLINE_TRAIN_DATA_LOG')
        ts = table.scan(row_start='32_CR400AF-A0004_00_20181028134430',
                        row_stop='32_CR400AF-A0004_00_20181028135000',
                        limit=10
                        )
        # scan 返回tuple类型的generator
        for rowkey, column in ts:
            # print(rowkey, column)
            # column为字典
            rowkey = rowkey.decode('utf8')
            column = self.transform_dict(column)
            # print(rowkey, '******', column)
            print(1)
            time.sleep(80)
            ts = table.scan(row_start='32_CR400AF-A0004_00_20181028134430',
                            row_stop='32_CR400AF-A0004_00_20181028135000',
                            limit=10
                            )

    def row_data(self):
        table = self.connection.table('ODS_PHM_ONLINE_TRAIN_DATA_LOG')
        column = table.row(row='32_CR400AF2025_05_20180801191300')
        # row 返回字典
        column = self.transform_dict(column)
        for key in column:
            print(key, column[key])


if __name__ == '__main__':
    hbase = HbaseHandler()
    hbase.scan_data()
