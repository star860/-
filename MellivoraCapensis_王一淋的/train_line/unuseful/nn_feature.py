# -*- coding: utf-8 -*-

import sys
import random
import traceback
import numpy as np
from data_util import dataModel
import matplotlib.pyplot as plt


np.set_printoptions(suppress=True)


def featureProcess(filePath):
    np_data = np.loadtxt(filePath, delimiter=',', dtype=str)
    line_keyId = np_data[:, :1]
    mile_data = np_data[:, 1:2].astype(np.float)
    alt_data = np_data[:, 2:3].astype(np.float)
    lng_data = np_data[:, 3:4].astype(np.float)
    lat_data = np_data[:, 4:5].astype(np.float)

    line_keyId = line_keyId[0:-4]
    mile_data_1 = mile_data[0:-4]
    mile_data_2 = mile_data[1:-3]
    mile_data_3 = mile_data[2:-2]
    mile_data_4 = mile_data[3:-1]
    mile_data_5 = mile_data[4:]

    mile_diff_21 = mile_data_2 - mile_data_1
    mile_diff_32 = mile_data_3 - mile_data_2
    mile_diff_43 = mile_data_4 - mile_data_3
    mile_diff_54 = mile_data_5 - mile_data_4

    lng_data_1 = lng_data[0:-4]
    lng_data_2 = lng_data[1:-3]
    lng_data_3 = lng_data[2:-2]
    lng_data_4 = lng_data[3:-1]
    lng_data_5 = lng_data[4:]


    lat_data_1 = lat_data[0:-4]
    lat_data_2 = lat_data[1:-3]
    lat_data_3 = lat_data[2:-2]
    lat_data_4 = lat_data[3:-1]
    lat_data_5 = lat_data[4:]


    data_merge = np.concatenate((line_keyId, mile_diff_21, mile_diff_32, mile_diff_43,mile_diff_54,
                                 lng_data_1, lng_data_2, lng_data_4,lng_data_5,
                                 lat_data_1, lat_data_2, lat_data_4,lat_data_5,
                                 mile_data_3,lng_data_3,lat_data_3), axis=1)

    out_path = filePath.split('.')[0] + '.for_train'
    np.savetxt(out_path, data_merge, fmt="%s", delimiter=',')

def generatePredData(filePath):

    dataUtil = dataModel()

    list_mileage, list_longitude, list_latitude, list_altitude = [], [], [], []
    with open(filePath, 'r') as files:
        while True:
            lines = files.readline()  # 读取行数据
            if not lines:
                break
            line_keyId, mile, alt, lat, lng = lines.strip().split(',')
            list_mileage.append(int(float(mile)))
            list_longitude.append(float(lng))
            list_latitude.append(float(lat))
            list_altitude.append(float(alt))

    mile_data_list = [mile for mile in range(min(list_mileage),max(list_mileage)+1)]
    count=0
    length=max(mile_data_list)
    idx=-1
    for mile in mile_data_list:
        count+=1
        if count % 1000 ==0:
            print (count,length)

        if idx > 0 and idx+2 < length:
            mile_data_1 = list_mileage[idx-1]
            mile_data_2 = list_mileage[idx]
            mile_data_3 = mile
            mile_data_4 = list_mileage[idx+1]
            mile_data_5 = list_mileage[idx+2]

            mile_diff_21 = mile_data_2 - mile_data_1
            mile_diff_32 = mile_data_3 - mile_data_2
            mile_diff_43 = mile_data_4 - mile_data_3
            mile_diff_54 = mile_data_5 - mile_data_4

            lng_data_1 = list_longitude[idx-1]
            lng_data_2 = list_longitude[idx]
            lng_data_4 = list_longitude[idx+1]
            lng_data_5 = list_longitude[idx+2]

            lat_data_1 = list_latitude[idx-1]
            lat_data_2 = list_latitude[idx]
            lat_data_4 = list_latitude[idx+1]
            lat_data_5 = list_latitude[idx+2]

            lng_data_3=999
            lat_data_3=999

            dParam = [line_keyId, mile_diff_21, mile_diff_32, mile_diff_43,mile_diff_54,
                                 lng_data_1, lng_data_2, lng_data_4,lng_data_5,
                                 lat_data_1, lat_data_2, lat_data_4,lat_data_5,
                                 mile_data_3,lng_data_3,lat_data_3]

            predfilePath = filePath.split('.')[0] + '.for_pred'

            dataUtil.saveToFile(predfilePath, dParam)


        if mile in list_mileage:
            idx+=1


        # small_list,big_list = dataUtil.get_split_data(mile,list_mileage)
        # small_list = sorted(small_list)
        # big_list = sorted(big_list)
        # if len(small_list)>=2 and len(big_list)>=2:
        #     mile_data_1 = small_list[-2]
        #     mile_data_2 = small_list[-1]
        #     mile_data_3 = mile
        #     mile_data_4 = big_list[0]
        #     mile_data_5 = big_list[1]
        #
        #     mile_diff_21 = mile_data_2 - mile_data_1
        #     mile_diff_32 = mile_data_3 - mile_data_2
        #     mile_diff_43 = mile_data_4 - mile_data_3
        #     mile_diff_54 = mile_data_5 - mile_data_4
        #
        #     idx_1 = list_mileage.index(mile_data_1)
        #     idx_2 = list_mileage.index(mile_data_2)
        #     idx_4 = list_mileage.index(mile_data_4)
        #     idx_5 = list_mileage.index(mile_data_5)
        #
        #     lng_data_1 = list_longitude[idx_1]
        #     lng_data_2 = list_longitude[idx_2]
        #     lng_data_4 = list_longitude[idx_4]
        #     lng_data_5 = list_longitude[idx_5]
        #
        #     lat_data_1 = list_latitude[idx_1]
        #     lat_data_2 = list_latitude[idx_2]
        #     lat_data_4 = list_latitude[idx_4]
        #     lat_data_5 = list_latitude[idx_5]
        #
        #     lng_data_3=999
        #     lat_data_3=999
        #
        #     dParam = [line_keyId, mile_diff_21, mile_diff_32, mile_diff_43,mile_diff_54,
        #                          lng_data_1, lng_data_2, lng_data_4,lng_data_5,
        #                          lat_data_1, lat_data_2, lat_data_4,lat_data_5,
        #                          mile_data_3,lng_data_3,lat_data_3]
        #
        #     predfilePath = filePath.split('.')[0] + '.for_pred'
        #
        #     dataUtil.saveToFile(predfilePath, dParam)

def generateFinalData(filePath):

    dataUtil = dataModel()

    list_mileage, list_longitude, list_latitude, list_altitude = [], [], [], []
    with open(filePath, 'r') as files:
        while True:
            lines = files.readline()  # 读取行数据
            if not lines:
                break
            line_keyId, mile, alt, lat, lng = lines.strip().split(',')
            list_mileage.append(int(float(mile)))
            list_longitude.append(float(lng))
            list_latitude.append(float(lat))
            list_altitude.append(float(alt))

    for i in range(len(list_mileage)):
        if i % 128 ==0:
            print(i)
        mile_1 = list_mileage[i]
        mile_2 = list_mileage[i+1]

        lat_1 = list_latitude[i]
        lat_2 = list_latitude[i+1]

        lng_1 = list_longitude[i]
        lng_2 = list_longitude[i+1]

        mile_diff = mile_2-mile_1
        lat_diff = lat_2 - lat_1
        lng_diff = lng_2 - lng_1

        for mile_cur in range(mile_1+1,mile_2):
            lat_cur = lat_1 + (mile_cur-mile_1)/mile_diff *lat_diff
            lng_cur = lng_1 + (mile_cur-mile_1)/mile_diff *lng_diff

            dParam = [line_keyId, mile_cur,0,lat_cur,lng_cur]

            predfilePath = filePath.split('.')[0] + '.final_data'

            dataUtil.saveToFile(predfilePath, dParam)

def smooth(filePath):

    list_mileage, list_longitude, list_latitude, list_altitude = [], [], [], []
    with open(filePath, 'r') as files:
        while True:
            lines = files.readline()  # 读取行数据
            if not lines:
                break
            line_keyId, mile, alt, lat, lng = lines.strip().split(',')
            list_mileage.append(int(float(mile)))
            list_longitude.append(float(lng))
            list_latitude.append(float(lat))
            list_altitude.append(float(alt))

    length = len(list_mileage)
    step = 50
    p=0.6
    count=0
    for i in range(length):
        head=int((i-p)*step)
        tail = int((i+1+p)*step)
        start = i*step
        stop = (i+1)*step

        if head<0:
            head=0
            tail+=step*2
        if tail > length-1:
            tail = length-1
            stop = length-1
            count+=1
        if count <2:
            mile_data = np.array(list_mileage[head:tail])
            alt_data = np.array(list_altitude[head:tail])
            lat_data = np.array(list_latitude[head:tail])
            lng_data= np.array(list_longitude[head:tail])

            alt_avg = sum(alt_data) / len(alt_data)
            for idx in range(len(alt_data)):
                if abs(alt_data[idx] - alt_avg) > 100:
                    alt_data[idx] = alt_avg

            start_mile = list_mileage[start]
            stop_mile = list_mileage[stop]

            if start == 0:
                start_mile=0
            x_new = np.linspace(start_mile,stop_mile,stop_mile-start_mile+1)

            alt_z = np.polyfit(mile_data, alt_data, 3)
            alt_f = np.poly1d(alt_z)
            alt_y = alt_f(x_new)

            lat_z = np.polyfit(mile_data, lat_data, 3)
            lat_f = np.poly1d(lat_z)
            lat_y = lat_f(x_new)

            lng_z = np.polyfit(mile_data, lng_data, 3)
            lng_f = np.poly1d(lng_z)
            lng_y = lng_f(x_new)

            x_new = np.around(x_new.reshape((-1,1)),0)
            alt_y = np.around(alt_y.reshape((-1,1)),5)
            lat_y = np.around(lat_y.reshape((-1,1)),5)
            lng_y = np.around(lng_y.reshape((-1,1)),5)
            line_keyId_list = np.array([line_keyId] * len(x_new)).reshape((-1, 1))

            np_result = np.concatenate((line_keyId_list,x_new, alt_y, lat_y, lng_y), axis=1)

            # for data in np_result:
            #     line_keyId = str(data[0])
            #     x_new = str(data[0])
            #     alt_y = str(data[1])
            #     lat_y = str(data[2])
            #     lng_y = str(data[3])

                # dParam = [lng_y, lat_y, alt_y]
                # rowkey = line_keyId + '_' + x_new
                # # 存储数据到hbase
                # self.hbase_db.saveData(rowkey, dParam)
            np.savetxt(filePath.split('.')[0] + ".smooth_"+str(start_mile), np_result, fmt="%s", delimiter=',')
            break
            # plt.figure()
            # plt.subplot(3,1,1)
            # plt.plot(mile_data,lat_data)
            # plt.plot(x_new,lat_y)
            # plt.subplot(3,1,2)
            # plt.plot(mile_data,lng_data)
            # plt.plot(x_new,lng_y)
            # plt.subplot(3,1,3)
            # plt.plot(mile_data,alt_data)
            # plt.plot(x_new,alt_y)
            # plt.show()

            # print (head,tail,start_mile,stop_mile,'----',lat_y[0],lat_y[-1],'----',lng_y[0],lng_y[-1],'----',alt_y[0],alt_y[-1])



    # print(lat_data[0:10])
    # print(lat_y_smooth[0:10])

    # alt_data = np.array([0] * len(all_mile)).reshape([-1, 1])
    # line_keyId = np.array([line_keyId] * len(all_mile)).reshape([-1, 1])

    # result = np.concatenate((line_keyId, all_mile, alt_data, lat_y_smooth, lng_y_smooth), axis=1)
    # np.savetxt(filePath.split('.')[0] + ".res_smooth", result, fmt="%s", delimiter=',')



if __name__ == '__main__':
    try:
        if len(sys.argv) >= 2:
            filePath = sys.argv[1]
            # featureProcess(filePath)
            # generatePredData(filePath)
            # generateFinalData(filePath)
            smooth(filePath)
    except Exception:
        print(traceback.format_exc())






