# -*- coding: utf-8 -*-

import os
import sys
import random
import traceback
import numpy as np
from sklearn.linear_model import LinearRegression

np.set_printoptions(suppress=True)

path = 'data/xltz_数据.for_train'
np_data = np.loadtxt(path, delimiter=',', dtype=str)
X_train = np_data[:, 1:13].astype(np.float)
mile_data_train = np_data[:, 13:14].astype(np.float)
Y_lng_train = np_data[:, 14:15].astype(np.float)
Y_lat_train = np_data[:, 15:16].astype(np.float)

model_lat = LinearRegression()
model_lat.fit(X_train, Y_lat_train)

model_lng = LinearRegression()
model_lng.fit(X_train, Y_lng_train)

# Y_lat_test = model_lat.predict(X_train)
# Y_lng_test = model_lng.predict(X_train)

# result_test_lat = np.concatenate((mile_data_train,Y_lat_train,Y_lat_test),axis=1)
# np.savetxt(path.split('.')[0]+".res_test_lat", result_test_lat, fmt="%s", delimiter=',')
#
# result_test_lng = np.concatenate((mile_data_train,Y_lng_train,Y_lng_test),axis=1)
# np.savetxt(path.split('.')[0]+".res_test_lng", result_test_lng, fmt="%s", delimiter=',')


path = 'data/xltz_数据.for_pred'
np_data = np.loadtxt(path, delimiter=',', dtype=str)
line_keyId = np_data[:,0:1]
X_pred = np_data[:, 1:13].astype(np.float)
mile_data_pred = np_data[:, 13:14].astype(np.float)

Y_lat_pred = model_lat.predict(X_pred)
Y_lng_pred = model_lng.predict(X_pred)

alt_data = np.array([0]*len(mile_data_pred)).reshape([-1,1])

result_pred_lat = np.concatenate((line_keyId,mile_data_pred,alt_data,Y_lat_pred,Y_lng_pred),axis=1)
np.savetxt(path.split('.')[0]+".res_pred", result_pred_lat, fmt="%s", delimiter=',')

# result_pred_lng = np.concatenate((mile_data_pred,Y_lng_pred),axis=1)
# np.savetxt(path.split('.')[0]+".res_pred_lng", result_pred_lng, fmt="%s", delimiter=',')


