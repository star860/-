# -*- coding: utf-8 -*-

import sys
import time
import math
import datetime
import traceback
import numpy as np
from db_hive_conn import HiveData
from db_mysql_conn import MySQLData
from db_hbase_conn import HBaseData
from data_util import dataModel


class Model():
    """线路特征模型"""

    def __init__(self, open=True):
        """
        初始化连接
        :param open: 是否连接数据库
        """
        try:
            self.dataUtil = dataModel()
            if open is True:
                self.hive_db = HiveData()
                self.mysql_db = MySQLData()
                self.hbase_db = HBaseData()
        except Exception:
            print('%s 数据库连接异常:  %s' % (datetime.datetime.now(), traceback.format_exc()))
            sys.exit()

    def run_train_line_feature_data(self, train_line_data, day_start, day_stop, tab_name):
        """
        根据线路信息数据,从mysql数据库中获取历史开行信息表信息,根据表中每一条该线路的开行信息,
        得到列号,车次,日期,始发站,终点站,始发时间,终点时间等信息,根据这些信息从hbase里分别取32接口和37接口的数据,
        获取到该线路的原始里程与GPS数据集,最后调用combine_mileage_gps方法,整合里程与GPS数据
        :param train_line_data: 线路信息数据
        :param day_start: 日期起始(yyyy-mm-dd)
        :param day_stop: 日期截止(yyyy-mm-dd)
        :param tab_name: hbase表名
        :return:
        """
        line_code, line_direction_code, station_start, station_end, full_length = train_line_data
        line_keyId = line_code + '-' + line_direction_code

        op_day = datetime.datetime.now() - datetime.timedelta(days=1)
        op_day = op_day.strftime('%Y%m%d')
        info = self.hive_db.query_open_string_info_by_station(op_day, station_start, station_end)
        count = 0
        length = len(info)
        for keyId in info:
            count += 1
            if count >= 0:
                trainNo = keyId.split('_')[0]
                trainsite_id, op_day, start_time, end_time, start_station, end_station, run_time, run_long = \
                    info[
                        keyId]
                if op_day >= day_start and op_day <= day_stop:
                    print(count, length, trainNo, trainsite_id, op_day, start_time, end_time, start_station,
                          end_station, run_time, run_long)

                    s_time = self.dataUtil.change_time_format(op_day + start_time, '%Y-%m-%d%H:%M', '%Y%m%d%H%M00')
                    if end_time > start_time:
                        e_time = self.dataUtil.change_time_format(op_day + end_time, '%Y-%m-%d%H:%M',
                                                                  '%Y%m%d%H%M00',
                                                                  delta_minutes=5, delta_type='addition')
                    else:
                        e_time = self.dataUtil.change_time_format(op_day + end_time, '%Y-%m-%d%H:%M',
                                                                  '%Y%m%d%H%M00', delta_days=1,
                                                                  delta_minutes=5, delta_type='addition')

                    interface_code = '37'
                    rowStart = interface_code + '_' + trainNo + '_00_' + s_time
                    rowStop = interface_code + '_' + trainNo + '_00_' + e_time
                    hbase_data_37 = self.hbase_db.queryTrainData(rowStart, rowStop, interface_code)

                    interface_code = '32'
                    rowStart = interface_code + '_' + trainNo + '_00_' + s_time
                    rowStop = interface_code + '_' + trainNo + '_00_' + e_time
                    hbase_data_32 = self.hbase_db.queryTrainData(rowStart, rowStop, interface_code)

                    filterDataList = self.dataFilter(direction, trainsite_id, hbase_data_32,
                                                     hbase_data_37)
                    if filterDataList:
                        filter_data_32, filter_data_37 = filterDataList
                        self.combine_mileage_gps(line_keyId, filter_data_32, filter_data_37, tab_name)

    def dataFilter(self, direction, trainsite_id, data_32, data_37):
        """
        数据清洗过滤,32接口数据过滤车次错误和里程无效的数据,以及里程变化与行别方向不一致的数据,
        37接口数据过滤GPS有效位无效的数据
        :param direction: 行别
        :param trainsite_id: 车次
        :param data_32: 32接口数据
        :param data_37: 37接口数据
        :param alt_limit: GPS高度数据峰值过滤
        :return: 清洗后的32接口数据和37接口数据
        """
        if data_37:
            for train in data_37:
                for fTime in sorted(data_37[train]):
                    data = data_37[train][fTime]
                    if data:
                        # if 'altitude' in data:
                        #     altitude = data['altitude']
                        #     if altitude.split('.')[0].isdigit():
                        #         if float(altitude)>=alt_limit:
                        #             del (data_37[train][fTime])
                        #             continue

                        if 'gps_valid' in data:
                            gps_valid = data['gps_valid']
                            if gps_valid != '1':
                                del (data_37[train][fTime])
                                continue
                        else:
                            del (data_37[train][fTime])

        old_mileage = None
        flag = 1
        if data_32:
            for train in data_32:
                for fTime in sorted(data_32[train]):
                    data = data_32[train][fTime]
                    if data:
                        hbase_trainsite_id = data['trainsite_id']
                        speed = data['speed']
                        mileage = data['mileage']
                        if hbase_trainsite_id != trainsite_id or mileage == '无效':
                            del (data_32[train][fTime])
                        else:
                            if old_mileage:
                                if direction == '上行' and float(mileage) < old_mileage:
                                    flag = 0
                                    break
                                if direction == '下行' and float(mileage) > old_mileage:
                                    flag = 0
                                    break
                            old_mileage = float(mileage)
                    else:
                        del (data_32[train][fTime])

        if flag == 1:
            return [data_32, data_37]

    def combine_mileage_gps(self, line_keyId, data_32, data_37, tab_name):
        """关联合并里程和gps数据
            :param line_keyId: hbase rowkey前缀,线路编号+上下行
            :param data_32: 32接口的数据
            :param data_37: 37接口的数据
            :param tab_name: hbase表名
        """
        if data_32 and data_37:
            for train in data_32:
                time_list_32 = sorted(data_32[train])
                time_list_37 = sorted(data_37[train])
                print(len(time_list_32), len(time_list_37))
                for t37 in time_list_37:
                    closely_time_list = self.dataUtil.get_closely_time(t37, time_list_32)
                    if closely_time_list:
                        t32_pre, t32_last = closely_time_list

                        speed_pre = float(data_32[train][t32_pre]['speed'])
                        mileage_pre = float(data_32[train][t32_pre]['mileage'])
                        speed_last = float(data_32[train][t32_last]['speed'])
                        mileage_last = float(data_32[train][t32_last]['mileage'])

                        diff_pre = self.dataUtil.cal_diff_sec(t37, t32_pre, format='%Y%m%d%H%M%S')
                        diff_last = self.dataUtil.cal_diff_sec(t32_last, t37, format='%Y%m%d%H%M%S')
                        tmp_mileage_pre = mileage_pre + speed_pre * diff_pre / 3.6
                        tmp_mileage_last = mileage_last - speed_last * diff_last / 3.6
                        # 加权平均计算里程
                        mileage_37 = self.cal_weight_mileage(tmp_mileage_pre, tmp_mileage_last, diff_pre, diff_last)
                        # 过滤负数里程
                        if '-' not in mileage_37:
                            data = data_37[train][t37]
                            longitude, latitude, altitude = data['longitude'], data['latitude'], data['altitude']
                            if longitude != '--' and latitude != '--' and altitude != '--':
                                family_name = 'GPS_DATA:'
                                col_longitude = family_name + 'longitude'
                                col_latitude = family_name + 'latitude'
                                col_altitude = family_name + 'altitude'
                                dataDict = {
                                    col_latitude: latitude
                                    , col_longitude: longitude
                                    , col_altitude: altitude
                                }
                                rowkey = line_keyId + '_' + mileage_37
                                # 存储数据到hbase
                                self.hbase_db.saveDataToHbase(tab_name, rowkey, dataDict, repeat=True)

    def cal_weight_mileage(self, mileage1, mileage2, second1, second2):
        """根据时间差,反向加权平均计算里程
            :param mileage1: 里程1
            :param mileage2: 里程2
            :param second1: 时间1
            :param second2: 时间2
        """
        second_sum = second1 + second2
        weight1 = second2 / second_sum
        weight2 = second1 / second_sum
        mileage = mileage1 * weight1 + mileage2 * weight2
        final_mileage = str(int(mileage)).zfill(7)  ##里程使用7位数表达,hbase scan出来就是按里程从小到大排序的
        return final_mileage

    def getDistance(self, lat1, lng1, lat2, lng2):
        """
        功能:根据GPS坐标,计算两点的距离
        :param lat1: 纬度1
        :param lng1: 经度1
        :param lat2: 纬度2
        :param lng2: 经度2
        :return: 距离(单位:米)
        """

        def rad(d):
            """
            经纬度转成弧度
            :param d: 经纬度
            :return: 弧度
            """
            return d * math.pi / 180.0

        EARTH_REDIUS = 6378.137
        radLat1 = rad(lat1)
        radLat2 = rad(lat2)
        a = radLat1 - radLat2
        b = rad(lng1) - rad(lng2)
        s = 2 * 1000 * math.asin(math.sqrt(
            math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
        s = s * EARTH_REDIUS
        return s

    def dataDealWithAlt(self, dataList):
        """
        通过数据发现列车GPS中高度数据存在准度高但精度低的问题,对高度数据做滑动平均处理
        :param dataList:里程GPS数据
        :return:处理后的数据
        """
        alt_limit = 0
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        np_altitude = np.array(list_altitude).astype(np.float)
        sigma_3 = np.percentile(np_altitude, 0.997)
        sigma_2 = np.percentile(np_altitude, 0.954)
        sigma_1 = np.percentile(np_altitude, 0.683)
        print(sigma_3, sigma_2, sigma_1)

        if sigma_3 - sigma_2 > 100:
            alt_limit = sigma_2
        else:
            alt_limit = sigma_3

        alt_length = len(list_altitude)
        for index in range(alt_length):
            if index >= 20 and index < alt_length - 20:
                alt_data = list_altitude[index - 20:index + 20]
                filter_data = list(filter(lambda x: x <= alt_limit, alt_data))
                if filter_data:
                    list_altitude[index] = sum(filter_data) / len(filter_data)
                else:
                    list_altitude[index] = list_altitude[index - 1]

        return [line_keyId, list_mileage, list_altitude, list_latitude, list_longitude]

    def dataDealWithLatAndlng(self, dataList):
        """
        通过数据发现列车在始发站开始运行的2公里内,gps经纬度数据偏差很大,取2公里内的众数经纬度作为基准gps,过滤偏差过大的数据
        :param dataList:里程GPS数据
        :return:过滤后的数据
        """
        size = 2000
        min_count_latitude_list = []
        max_count_latitude_list = []
        min_count_longitude_list = []
        max_count_longitude_list = []
        min_longitude_set = set()
        min_latitude_set = set()
        max_longitude_set = set()
        max_latitude_set = set()
        min_data_choose_list = []
        max_data_choose_list = []
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        min_mileage = float(min(list_mileage))
        max_mileage = float(max(list_mileage))
        min_limit = [min_mileage, min_mileage + size]
        max_limit = [max_mileage - size, max_mileage]
        for i in range(len(list_mileage)):
            cur_mile = float(list_mileage[i].split('-')[0])
            if cur_mile >= min_limit[0] and cur_mile <= min_limit[1]:
                tmpList = [i, list_mileage[i], int(list_longitude[i]), int(list_latitude[i])]
                min_data_choose_list.append(tmpList)
                min_longitude_set.add(int(list_longitude[i]))
                min_latitude_set.add(int(list_latitude[i]))
            if cur_mile >= max_limit[0] and cur_mile <= max_limit[1]:
                tmpList = [i, list_mileage[i], int(list_longitude[i]), int(list_latitude[i])]
                max_data_choose_list.append(tmpList)
                max_longitude_set.add(int(list_longitude[i]))
                max_latitude_set.add(int(list_latitude[i]))

        for x in min_longitude_set:
            tmp_list_longitude = [x[2] for x in min_data_choose_list]
            min_count_longitude_list.append([x, tmp_list_longitude.count(x)])
        for x in max_longitude_set:
            tmp_list_longitude = [x[2] for x in max_data_choose_list]
            max_count_longitude_list.append([x, tmp_list_longitude.count(x)])

        for x in min_latitude_set:
            tmp_list_latitude = [x[3] for x in min_data_choose_list]
            min_count_latitude_list.append([x, tmp_list_latitude.count(x)])
        for x in max_latitude_set:
            tmp_list_latitude = [x[3] for x in max_data_choose_list]
            max_count_latitude_list.append([x, tmp_list_latitude.count(x)])

        min_choose_longitude = sorted(min_count_longitude_list, key=lambda x: x[-1], reverse=True)[0][0]
        max_choose_longitude = sorted(max_count_longitude_list, key=lambda x: x[-1], reverse=True)[0][0]
        min_choose_latitude = sorted(min_count_latitude_list, key=lambda x: x[-1], reverse=True)[0][0]
        max_choose_latitude = sorted(max_count_latitude_list, key=lambda x: x[-1], reverse=True)[0][0]

        final_list_mileage, final_list_longitude, final_list_latitude, final_list_altitude = [], [], [], []
        for i in range(len(list_mileage)):
            cur_mile = float(list_mileage[i].split('-')[0])
            cur_longitude = int(list_longitude[i])
            cur_latitude = int(list_latitude[i])
            if cur_mile >= min_limit[0] and cur_mile <= min_limit[1]:
                if cur_longitude == min_choose_longitude and cur_latitude == min_choose_latitude:
                    final_list_mileage.append(list_mileage[i])
                    final_list_longitude.append(list_longitude[i])
                    final_list_latitude.append(list_latitude[i])
                    final_list_altitude.append(list_altitude[i])
            elif cur_mile >= max_limit[0] and cur_mile <= max_limit[1]:
                if cur_longitude == max_choose_longitude and cur_latitude == max_choose_latitude:
                    final_list_mileage.append(list_mileage[i])
                    final_list_longitude.append(list_longitude[i])
                    final_list_latitude.append(list_latitude[i])
                    final_list_altitude.append(list_altitude[i])
            else:
                final_list_mileage.append(list_mileage[i])
                final_list_longitude.append(list_longitude[i])
                final_list_latitude.append(list_latitude[i])
                final_list_altitude.append(list_altitude[i])

        return [line_keyId, final_list_mileage, final_list_altitude, final_list_latitude, final_list_longitude]

    def generateFinalData(self, dataList, diff_limit=10):
        """
        对初始里程、GPS数据过滤、清洗， 最终保留里程、GPS对应关系准确的数据
        :param dataList: 里程、GPS数据
        :param diff_limit: 过滤，两个里程差的误差精度
        :return:
        """
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList
        dList = []
        for i in range(1, len(list_mileage)):
            old_mile = int(list_mileage[i - 1].split('-')[0])
            old_lng = list_longitude[i - 1]
            old_lat = list_latitude[i - 1]
            old_alt = list_altitude[i - 1]
            cur_mile = int(list_mileage[i].split('-')[0])
            cur_lng = list_longitude[i]
            cur_lat = list_latitude[i]
            cur_alt = list_altitude[i]

            diff1 = cur_mile - old_mile  # 两个运营里程的距离
            diff2 = self.getDistance(old_lat, old_lng, cur_lat, cur_lng)  # 使用对应两个GPS经度纬度计算的距离
            # 两个距离误差满足精度要求,则存储数据,否则舍去
            if abs(diff1 - diff2) <= diff_limit:
                old_tmpLlist = [old_mile, old_lat, old_lng, old_alt]
                cur_tmpLlist = [cur_mile, cur_lat, cur_lng, cur_alt]
                if old_tmpLlist not in dList[-10:]:
                    dList.append(old_tmpLlist)
                if cur_tmpLlist not in dList[-10:]:
                    dList.append(cur_tmpLlist)

        mile_data, lat_data, lng_data, alt_data = [], [], [], []

        for data in dList:
            mile, lat, lng, alt = data
            mile_data.append(mile)
            lat_data.append(lat)
            lng_data.append(lng)
            alt_data.append(alt)

        return [line_keyId, mile_data, alt_data, lat_data, lng_data]

    def smooth(self, dataList, des_tab_name):
        """
        根据给出的数据,采用平滑移动法补全每一米里程对应的GPS数据,最后数据存入hbase数据宽表中
        :param dataList: 里程GPS数据集
        :param des_tab_name: hbase结果表表名
        :return:
        """
        line_keyId, list_mileage, list_altitude, list_latitude, list_longitude = dataList

        length = len(list_mileage)
        step = 50
        p = 0.5
        count = 0
        for i in range(length):
            head = int((i - p) * step)
            tail = int((i + 1 + p) * step)
            start = i * step
            stop = (i + 1) * step

            if head < 0:
                head = 0
                tail += step * 2
            if tail > length - 1:
                tail = length - 1
                stop = length - 1
                count += 1
            if count < 2:
                mile_data = np.array(list_mileage[head:tail])
                alt_data = np.array(list_altitude[head:tail])
                lat_data = np.array(list_latitude[head:tail])
                lng_data = np.array(list_longitude[head:tail])

                alt_avg = sum(alt_data) / len(alt_data)
                for idx in range(len(alt_data)):
                    if abs(alt_data[idx] - alt_avg) > 100:
                        alt_data[idx] = alt_avg

                start_mile = list_mileage[start]
                stop_mile = list_mileage[stop]

                if start == 0:
                    start_mile = 0

                print(start_mile, stop_mile)
                x_new = np.linspace(start_mile, stop_mile, stop_mile - start_mile + 1)

                alt_z = np.polyfit(mile_data, alt_data, 3)
                alt_f = np.poly1d(alt_z)
                alt_y = alt_f(x_new)

                lat_z = np.polyfit(mile_data, lat_data, 3)
                lat_f = np.poly1d(lat_z)
                lat_y = lat_f(x_new)

                lng_z = np.polyfit(mile_data, lng_data, 3)
                lng_f = np.poly1d(lng_z)
                lng_y = lng_f(x_new)

                x_new = np.around(x_new.reshape((-1, 1)), 0).astype(np.int32)
                alt_y = np.around(alt_y.reshape((-1, 1)), 2)
                lat_y = np.around(lat_y.reshape((-1, 1)), 5)
                lng_y = np.around(lng_y.reshape((-1, 1)), 5)
                line_keyId_list = np.array([line_keyId] * len(x_new)).reshape((-1, 1))

                np_result = np.concatenate((line_keyId_list, x_new, alt_y, lat_y, lng_y), axis=1)
                # np.savetxt("data/xltz_数据.smooth_" + str(start_mile), np_result, fmt="%s", delimiter=',')

                for data in np_result:
                    x_new = str(data[1]).zfill(7)
                    alt_y = str(data[2])
                    lat_y = str(data[3])
                    lng_y = str(data[4])

                    family_name = 'GPS_DATA:'
                    col_altitude = family_name + 'altitude'
                    col_longitude = family_name + 'longitude'
                    col_latitude = family_name + 'latitude'
                    dataDict = {
                        col_altitude: alt_y
                        , col_latitude: lat_y
                        , col_longitude: lng_y
                    }
                    rowkey = line_keyId + '_' + x_new
                    # 存储数据到hbase
                    self.hbase_db.saveDataToHbase(des_tab_name, rowkey, dataDict)

    def run_data_smooth(self, train_line_data, src_tab_name, des_tab_name):
        """
        获取hbase临时表的里程GPS数据,经过滤清洗后得到最终的里程GPS数据,然后使用数据分段平滑的方式,铺满一条线的所有里程和GPS,存储到最终hbase表里
        :param train_line_data: 线路配置数据
        :param src_tab_name: 源数据表
        :param des_tab_name: 目标数据表
        :return:
        """
        line_code, line_direction_code, station_start, station_end, full_length = train_line_data
        line_keyId = line_code + '-' + line_direction_code
        line_prefix = line_keyId.encode()

        print('process:queryGPS', datetime.datetime.now())
        dataList = self.hbase_db.queryGPS(tab_name=src_tab_name, line_prefix=line_prefix)

        print('process:dataDealWithAlt', datetime.datetime.now())
        dataList = self.dataDealWithAlt(dataList)

        print('process:dataDealWithLatAndlng', datetime.datetime.now())
        dataList = self.dataDealWithLatAndlng(dataList)

        print('process:generateFinalData', datetime.datetime.now())
        dataList = self.generateFinalData(dataList, diff_limit=10)

        print('process:smooth', datetime.datetime.now())
        self.smooth(dataList, des_tab_name)


if __name__ == '__main__':
    try:
        m = Model()
        if len(sys.argv) == 5:
            line_name = sys.argv[1]
            direction = sys.argv[2]
            day_start = sys.argv[3]
            day_stop = sys.argv[4]
            tmp_hbase_tabName = 'lzb_test'
            final_hbase_tabName = 'DM_TRAIN_LINE_FEATURE_INFO'
            train_line_data = m.mysql_db.queryParamFromLineFeature(line_name, direction)
            m.run_train_line_feature_data(train_line_data, day_start, day_stop, tmp_hbase_tabName)
            m.run_data_smooth(train_line_data, tmp_hbase_tabName, final_hbase_tabName)
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)

    # m = Model()
    # dataList=m.hbase_db.queryGPS(tab_name='DM_TRAIN_LINE_FEATURE_INFO')
    # dataList = m.dataFilterAfterCombine(dataList)
    # dataList = m.generateFinalData(dataList, diff_limit=10)

    # des_filePath1='data/xltz_test.dat'
    # des_filePath2='data/xltz_all.dat'
    # m.dataUtil.save_GPS_To_File(dataList,des_filePath2)
    # dataList = m.dataUtil.get_Mile_GPS_Data(des_filePath2)

    # m.dataUtil.data_plot_Mile_GPS(dataList,size=100000)
    # m.dataUtil.data_plot_GPS_2D(dataList,size=100000000)
    # m.dataUtil.data_plot_GPS_3D(dataList,size=100000)
