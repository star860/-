# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 创建时间：2019年09月20日                                            #
# 创 建 者：wyl                                                       #
# 功能：利用hive里面的轴温数据绘图分析                                 #
#                                                                    #
######################################################################


from impala.dbapi import connect as hive_connect
import numpy
from matplotlib import pyplot


class PlotData:
    """
    将轴温有关的信息关联写入hive表中
    """

    def __init__(self):
        """
        初始化
        """

        # 连接hive开发数据库
        self.hive_dev_connection = hive_connect(host='10.73.95.74',
                                                port=10088,
                                                auth_mechanism='PLAIN',
                                                user='phmdm',
                                                password='phmdm@123',
                                                database='default')
        self.hive_dev_cursor = self.hive_dev_connection.cursor()

    def get_data_from_hive(self):
        hive_sql = '''
                    SELECT
                        axletree_temperature1
                    FROM
                        temp_real_time_axletree_temperature 
                    WHERE
                        train_line_code = "3002"
                        and line_dire_code = "0"
                        and train_id = 'CR400AF-2021'
                        and mileage >= 1
                        and mileage <= 1000000
                        and env_temperature = '31'
        '''
        self.hive_dev_cursor.execute(hive_sql)
        hive_result = self.hive_dev_cursor.fetchall()
        data_list = []
        for tp in hive_result:
            data_list.append(tp[0])
        print(data_list)
        return data_list

    def count_frequency(self, list_data):
        # 转化成numpy array格式
        data_array = numpy.array(list_data)
        # 将array去重并按照从小到大的顺序排序
        unique_array = numpy.unique(data_array)
        x_list = []
        y_list = []
        for x in unique_array:
            mask = (data_array == x)
            array_new = data_array[mask]
            y = array_new.size
            x_list.append(x)
            y_list.append(y)
        return [x_list, y_list]

    def plot_data(self):
        data_list = self.get_data_from_hive()
        x, y = self.count_frequency(data_list)
        pyplot.title("axletree temperature demo")
        pyplot.xlabel("x temperature")
        pyplot.ylabel("y temperature count")
        pyplot.plot(x, y)
        pyplot.show()


if __name__ == '__main__':
    pd = PlotData()
    pd.plot_data()
