# -*- coding: utf-8 -*-

import sys
import math
import datetime
import traceback
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d.axes3d import Axes3D

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class dataModel():

    def change_time_format(self, t, format_in, format_out, delta_days=0, delta_hours=0, delta_minutes=0,
                           delta_type=None):
        """时间格式转换,同时可以在原来时间基础上增加,削减天数,小时数和分钟数
            param t: 时间
            param format_in:输入的时间格式
            param format_out:输出的时间格式
            param delta_days:天数
            param delta_hours:小时数
            param delta_minutes:分钟数
            param delta_type:加法 或者 减法
        """
        assert delta_type in (None, 'addition', 'subtraction')
        if delta_type == 'addition':
            tmp_time = datetime.datetime.strptime(t, format_in) + datetime.timedelta(days=delta_days, hours=delta_hours,
                                                                                     minutes=delta_minutes)
        elif delta_type == 'subtraction':
            tmp_time = datetime.datetime.strptime(t, format_in) - datetime.timedelta(days=delta_days, hours=delta_hours,
                                                                                     minutes=delta_minutes)
        else:
            tmp_time = datetime.datetime.strptime(t, format_in)
        final_time = tmp_time.strftime(format_out)
        return final_time

    def cal_diff_sec(self, t1, t0, format):
        """计算时间差(秒)
            param t1: 时间,较大的
            param t0: 时间,较小的
            param format: 时间格式
        """
        date_t1 = datetime.datetime.strptime(t1, format)
        date_t0 = datetime.datetime.strptime(t0, format)
        diff = (date_t1 - date_t0).total_seconds()
        return diff

    def get_closely_time(self, cur_time, time_list):
        """根据一个时间点,从一个时间列表里寻找到最接近该时间的前后两个时间点
            param cur_time: 当前时间
            param time_list: 时间列表
        """
        small_list = list(filter(lambda t: t < cur_time, time_list))
        big_list = list(filter(lambda t: t > cur_time, time_list))
        if small_list and big_list:
            time_pre = max(small_list)
            time_last = min(big_list)
            return [time_pre, time_last]

    def get_split_data(self, cur_value, data_list):
        """根据一个时间点,从一个时间列表里寻找到最接近该时间的前后两个时间点
            param cur_time: 当前时间
            param time_list: 时间列表
        """
        small_list = list(filter(lambda t: t < cur_value, data_list))
        big_list = list(filter(lambda t: t > cur_value, data_list))
        return [small_list, big_list]

    def saveToFile(self, filePath, d):
        """功能：将数据写入文件
                   参数：fileName-写入文件路径
                         d-内容(列表格式)
                """
        contents = ','.join([str(item) for item in d])
        # print contents

        # 写入文件
        f = open(filePath, "a+")
        f.write(contents + '\n')
        f.close()

    def save_GPS_To_File(self,dataList,path):
        """
        将里程GPS数据存储到本地文件
        :param dataList:里程GPS数据
        :param path: 本地文件路径
        :return:
        """
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        np_mile= np.array(list_mileage).reshape((-1,1))
        np_altitude= np.array(list_altitude).reshape((-1,1))
        np_latitude= np.array(list_latitude).reshape((-1,1))
        np_longitude= np.array(list_longitude).reshape((-1,1))
        np_keyId = np.array([line_keyId]*len(np_mile)).reshape((-1,1))


        result = np.concatenate((np_keyId,np_mile,np_altitude,np_latitude,np_longitude),axis=1)
        np.savetxt(path, result, fmt="%s", delimiter=',')

    def get_Mile_GPS_Data(self, filePath):
        """
        功能: 读取文件,获取里程,GPS数据
        :param filePath: 文件路径
        :return: 里程,GPS数据集合
        """
        list_mileage, list_longitude, list_latitude, list_altitude = [], [], [], []
        with open(filePath, 'r') as files:
            while True:
                lines = files.readline()  # 读取行数据
                if not lines:
                    break
                line_keyId, mile, alt, lat, lng = lines.strip().split(',')
                list_mileage.append(float(mile.split('-')[0]))
                list_longitude.append(float(lng))
                list_latitude.append(float(lat))
                list_altitude.append(float(alt))

        return [line_keyId, list_mileage, list_altitude, list_latitude, list_longitude]

    def data_plot_Mile_GPS(self, dataList, size=1000):
        """
        功能: 画图
        :param dataList: 数据集
        :param size: 每次画图使用的数据量
        :return:
        """
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        length = len(list_mileage)
        head = 0
        for i in range(1000):
            tail = head + size
            print(i, head, tail, length)
            if tail <= length:
                plt.figure()
                plt.subplot(3, 1, 1)
                plt.scatter(list_mileage[head:tail], list_longitude[head:tail], s=1)
                plt.title('里程与经度')
                plt.subplot(3, 1, 2)
                plt.scatter(list_mileage[head:tail], list_latitude[head:tail], s=1)
                plt.title('里程与纬度')
                plt.subplot(3, 1, 3)
                plt.scatter(list_mileage[head:tail], list_altitude[head:tail], s=1)
                plt.title('里程与高度')
                # plt.ylim((0, 200))
                plt.show()
            else:
                plt.figure()
                plt.subplot(3, 1, 1)
                plt.scatter(list_mileage[head:], list_longitude[head:], s=1)
                plt.title('里程与经度')
                plt.subplot(3, 1, 2)
                plt.scatter(list_mileage[head:], list_latitude[head:], s=1)
                plt.title('里程与纬度')
                plt.subplot(3, 1, 3)
                plt.scatter(list_mileage[head:], list_altitude[head:], s=1)
                plt.title('里程与高度')
                # plt.ylim((0,30))
                plt.show()
                break
            head += size

    def data_plot_GPS_3D(self, dataList, size=1000):
        """
        画图
        :param dataList:
        :param sie:
        :return:
        """
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        length = len(list_mileage)
        head = 0
        for i in range(1000):
            tail = head + size
            print(i, head, tail, length)
            if tail <= length:
                # 此处fig是二维
                fig = plt.figure()
                # 将二维转化为三维
                axes3d = Axes3D(fig)
                axes3d.scatter3D(list_latitude[head:tail], list_longitude[head:tail], list_altitude[head:tail], s=0.5)
                plt.title('GPS三维图')
                axes3d.set_title("GPS三维图")
                axes3d.set_xlabel("纬度")
                axes3d.set_ylabel("经度")
                axes3d.set_zlabel("高度")
                axes3d.invert_xaxis()  # x轴反向
                plt.show()
            else:
                # 此处fig是二维
                fig = plt.figure()
                # 将二维转化为三维
                axes3d = Axes3D(fig)
                axes3d.scatter3D(list_latitude[head:], list_longitude[head:], list_altitude[head:], s=0.5)
                axes3d.set_title("GPS三维图")
                axes3d.set_xlabel("纬度")
                axes3d.set_ylabel("经度")
                axes3d.set_zlabel("高度")
                axes3d.invert_xaxis()  # x轴反向
                plt.show()
                break
            head += size

    def data_plot_GPS_2D(self, dataList, size=1000):
        """
        画图
        :param dataList:
        :param sie:
        :return:
        """
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        length = len(list_mileage)
        head = 0
        for i in range(1000):
            tail = head + size
            print(i, head, tail, length)
            if tail <= length:
                plt.figure()
                plt.scatter(list_longitude[head:tail], list_latitude[head:tail], s=0.1)
                plt.title('GPS经纬度')
                plt.xlabel('经度')
                plt.ylabel('纬度')
                plt.show()
            else:
                plt.figure()
                plt.scatter(list_longitude[head:], list_latitude[head:], s=0.1)
                plt.title('GPS经纬度')
                plt.xlabel('经度')
                plt.ylabel('纬度')
                plt.show()
                break
            head += size


if __name__ == '__main__':
    m = dataModel()
    dataList = m.get_Mile_GPS_Data('data/xltz_all.dat')
    # dataList = m.get_Mile_GPS_Data('data/xltz_数据.smooth_53910')
    # dataList = m.get_Mile_GPS_Data('data/xltz_数据.res_pred')
    m.data_plot_Mile_GPS(dataList, size=100000)
    # m.data_plot_GPS_2D(dataList,size=10000000)
    # m.data_plot_GPS_3D(dataList,size=10000000)




