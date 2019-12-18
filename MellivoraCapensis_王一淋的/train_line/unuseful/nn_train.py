# -*- coding: utf-8 -*-

import sys
import random
import traceback
import numpy as np
import tensorflow as tf

np.set_printoptions(suppress=True)

np_data = np.loadtxt('data/xltz_数据.for_train', delimiter=',', dtype=str)
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

lr = 0.0013
x = tf.placeholder(tf.float32, [None, 5])
y = tf.placeholder(tf.float32, [None, 1])
keep_prob = tf.placeholder(tf.float32)

weights = {
    'in': tf.Variable(tf.zeros([5, 128])),
    'hidden': tf.Variable(tf.zeros([128, 64])),
    'out': tf.Variable(tf.zeros([64, 1]))
}
biases = {
    'in': tf.Variable(tf.constant(0.1, shape=[128, ])),
    'hidden': tf.Variable(tf.constant(0.1, shape=[64, ])),
    'out': tf.Variable(tf.constant(0.1, shape=[1, ]))
}

# 输入层的矩阵计算y=Wx+b
y_in = tf.matmul(x, weights['in']) + biases['in']
# 隐藏层的矩阵计算y=Wx+b
y_hidden = tf.matmul(y_in, weights['hidden']) + biases['hidden']
# 对隐藏层进行dropout
y_drop = tf.nn.dropout(y_hidden, keep_prob=keep_prob)
# 输出层的矩阵计算y=Wx+b,得到最后的轴温预测值
y_out = tf.matmul(y_drop, weights['out']) + biases['out']

# 损失函数,采用均方误差作为损失函数
cost = tf.reduce_mean(tf.pow(y_out - y, 2.0))
# 优化器,选择Adam优化器
train_op = tf.train.AdamOptimizer(lr).minimize(cost)

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

            model_save_path = 'model/NN_' + line_keyId + '_' + label_type

            ## 生成神经网络模型
            with tf.Session() as sess:
                # 初始化所有变量
                init = tf.global_variables_initializer()
                sess.run(init)
                # saver用于保存和加载神经网络模型
                saver = tf.train.Saver(tf.global_variables())
                ## 加载模型
                # saver.restore(sess, save_path=model_save_path)
                batch_size = 64
                # 模型训练
                for i in range(10000):
                    # 进行训练数据的抽样,组合成一个批次用于模型训练
                    temp_xs = list()
                    temp_ys = list()
                    for j in range(len(data) // 100 - 1):
                        rand = random.randint(j * 100, (j + 1) * 100)
                        temp_xs.append(data[rand:rand + 32])
                        temp_ys.append(label[rand:rand + 32])
                    batch_xs = np.concatenate(temp_xs)
                    batch_ys = np.concatenate(temp_ys)
                    # 模型训练
                    sess.run(train_op, feed_dict={x: batch_xs, y: batch_ys, keep_prob: 0.8})
                    # 每隔100步,打印一次模型的训练测试结果
                    if i % 100 == 0:
                        rand = int(random.randint(0, len(data) // batch_size))
                        test_xs = data[rand * batch_size:(rand + 1) * batch_size]
                        test_ys = label[rand * batch_size:(rand + 1) * batch_size]
                        print(i, sess.run(cost, feed_dict={x: test_xs, y: test_ys, keep_prob: 1.0}))
                # 模型训练完毕后保存模型
                saver.save(sess, save_path=model_save_path)

    except Exception:
        print(traceback.format_exc())








