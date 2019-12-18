# -*- coding: utf-8 -*-

import sys
import random
import traceback
import numpy as np
import tensorflow as tf

np.set_printoptions(suppress=True)
'''
将训练好的神经网络模型对数据进行预测
'''


path = 'data/xltz_数据.for_train'
np_data = np.loadtxt(path, delimiter=',', dtype=str)
line_keyId = np_data[0, 0]
mile_data = np_data[:, 1:5].astype(np.float)
alt_data = np_data[:, 5:9].astype(np.float)
lng_data = np_data[:, 9:13].astype(np.float)
lat_data = np_data[:, 13:17].astype(np.float)

mile_diff_data = np_data[:,17:20].astype(np.float)
alt_diff_data = np_data[:,20:22].astype(np.float)
lng_diff_data = np_data[:,22:24].astype(np.float)
lat_diff_data = np_data[:,24:26].astype(np.float)

alt_label = np_data[:, 26:27].astype(np.float)
lng_label = np_data[:, 27:28].astype(np.float)
lat_label = np_data[:, 28:29].astype(np.float)

lr = 0.0015
x = tf.placeholder(tf.float32, [None, 5])
y = tf.placeholder(tf.float32, [None, 1])
keep_prob = tf.placeholder(tf.float32)

weights = {
    # (32, 32)
    'in': tf.Variable(tf.zeros([5, 128])),
    # (32, 16)
    'hidden': tf.Variable(tf.zeros([128, 64])),
    # (16, 1)
    'out': tf.Variable(tf.zeros([64, 1]))
}
biases = {
    # (32, )
    'in': tf.Variable(tf.constant(0.1, shape=[128, ])),
    # (16, )
    'hidden': tf.Variable(tf.constant(0.1, shape=[64, ])),
    # (1, )
    'out': tf.Variable(tf.constant(0.1, shape=[1, ]))
}

y_in = tf.matmul(x, weights['in']) + biases['in']
y_hidden = tf.matmul(y_in, weights['hidden']) + biases['hidden']
y_drop = tf.nn.dropout(y_hidden, keep_prob=keep_prob)
y_out = tf.matmul(y_drop, weights['out']) + biases['out']

dd = y_out - y
diff = tf.abs(y_out - y)
cost = tf.reduce_mean(tf.pow(y_out - y, 2.0))
train_op = tf.train.AdamOptimizer(lr).minimize(cost)

mean_diff = tf.reduce_mean(diff)
rmse = tf.pow(cost, 0.5)
# r2 = 1 - tf.divide(tf.reduce_sum(tf.pow(y_out - y, 2)), tf.reduce_sum(tf.pow(y_out - tf.reduce_mean(y), 2)))



if __name__ == '__main__':
    try:
        if len(sys.argv) >= 2:
            label_type = sys.argv[1]
            print(label_type)
            assert label_type in ('lat', 'lng', 'alt')
            if label_type == 'lat':
                label = lat_label
                data = np.concatenate((mile_diff_data,lat_diff_data),axis=1)
            elif label_type == 'lng':
                label = lng_label
                data = np.concatenate((mile_diff_data,lng_diff_data),axis=1)
            else:
                label = alt_label
                data = np.concatenate((mile_diff_data,alt_diff_data),axis=1)

            model_save_path = 'model/NN_' + line_keyId+'_'+label_type
            print (data.shape)

            ## 第一次生成神经网络模型
            with tf.Session() as sess:
                init = tf.global_variables_initializer()
                sess.run(init)
                saver = tf.train.Saver(tf.global_variables())
                saver.restore(sess, save_path=model_save_path)

                pred = sess.run(y_out, feed_dict={x: data, y: label, keep_prob: 1.0})
                dff = sess.run(diff, feed_dict={x: data, y: label, keep_prob: 1.0}).reshape([-1])
                # R2 = sess.run(r2, feed_dict={x: data, y: label, keep_prob: 1.0}).reshape([-1])
                mean_dff = sess.run(mean_diff, feed_dict={x: data, y: label, keep_prob: 1.0}).reshape([-1])
                MSE = sess.run(cost, feed_dict={x: data, y: label, keep_prob: 1.0})
                RMSE = sess.run(rmse, feed_dict={x: data, y: label, keep_prob: 1.0})

                idx = np.argwhere(dff > 1)
                a = len(idx)
                b = len(dff)
                print("绝对误差>1的个数:", a)
                print("总样本数:", b)
                print("百分比:", a / b)

                print("MAE:", mean_dff)
                # print("R2:", R2)
                print("MSE:", MSE)
                print("RMSE:", RMSE)

                # err = tmp[idx2.reshape([-1])]
                # err_p = pred[idx2.reshape([-1])]
                # res = np.concatenate((idx2 + 1, err[:, 1:6], err[:, -1:], err_p, dff[idx2]), axis=1)
                # np.savetxt(path.split('.')[0] + ".error", res, fmt="%s", delimiter='\t')

                if label_type == 'lat':
                    dataY = lat_data[:,-2:-1]+label
                    dataY_pred = lat_data[:,-2:-1]+pred
                elif label_type == 'lng':
                    dataY = lng_data[:, -2:-1] + label
                    dataY_pred = lng_data[:, -2:-1] + pred
                else:
                    dataY = alt_data[:, -2:-1] + label
                    dataY_pred = alt_data[:, -2:-1] + pred

                result = np.concatenate((mile_data[:,-1:],dataY,dataY_pred),axis=1)
                np.savetxt(path.split('.')[0]+".res_test_"+label_type, result, fmt="%s", delimiter=',')


    except Exception:
        print(traceback.format_exc())

