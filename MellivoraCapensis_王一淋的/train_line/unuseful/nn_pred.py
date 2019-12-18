# -*- coding: utf-8 -*-

import os
import sys
import random
import traceback
import numpy as np
import tensorflow as tf

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
np.set_printoptions(suppress=True)
'''
将训练好的神经网络模型对数据进行预测
'''


path = 'data/xltz_数据.for_pred'
np_data = np.loadtxt(path, delimiter=',', dtype=str)
line_keyId = np_data[0, 0]
mile_data = np_data[:, 1:5].astype(np.float)
alt_data = np_data[:, 5:8].astype(np.float)
lng_data = np_data[:, 8:11].astype(np.float)
lat_data = np_data[:, 11:14].astype(np.float)

mile_diff_data = np_data[:,14:17].astype(np.float)
alt_diff_data = np_data[:,17:19].astype(np.float)
lng_diff_data = np_data[:,19:21].astype(np.float)
lat_diff_data = np_data[:,21:23].astype(np.float)

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

if __name__ == '__main__':
    try:
        if len(sys.argv) >= 2:
            label_type = sys.argv[1]
            print(label_type)
            assert label_type in ('lat', 'lng', 'alt')
            if label_type == 'lat':
                data = np.concatenate((mile_diff_data,lat_diff_data),axis=1)
            elif label_type == 'lng':
                data = np.concatenate((mile_diff_data,lng_diff_data),axis=1)
            else:
                data = np.concatenate((mile_diff_data,alt_diff_data),axis=1)

            model_save_path = 'model/NN_' + line_keyId+'_'+label_type
            print (data.shape)

            ## 第一次生成神经网络模型
            with tf.Session() as sess:
                init = tf.global_variables_initializer()
                sess.run(init)
                saver = tf.train.Saver(tf.global_variables())
                saver.restore(sess, save_path=model_save_path)

                pred = sess.run(y_out, feed_dict={x: data, keep_prob: 1.0})

                if label_type == 'lat':
                    dataY_pred = lat_data[:,-1:]+pred
                elif label_type == 'lng':
                    dataY_pred = lng_data[:, -1:] + pred
                else:
                    dataY_pred = alt_data[:, -1:] + pred

                result = np.concatenate((np.around(mile_data[:,-1:],0),np.around(dataY_pred,5)),axis=1)
                np.savetxt(path.split('.')[0]+".res_"+label_type, result, fmt="%s", delimiter=',')


    except Exception:
        print(traceback.format_exc())

