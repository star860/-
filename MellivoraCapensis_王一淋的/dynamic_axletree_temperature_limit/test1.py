# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 创建时间：2019年09月16日                                           #
# 创 建 者：wyl                                                      #
# 功能：写入hive测试脚本                                              #
#                                                                    #
######################################################################


import sys, time, datetime
import traceback
from kazoo.client import KazooClient
import hdfs

nameNodeA = [b'phmnn1.bigdata.com', 'phmnn1.bigdata.com']
nameNodeB = [b'phmnn2.bigdata.com', 'phmnn2.bigdata.com']
zooQuorum = '10.73.95.19:2181,10.73.95.20:2181,10.73.95.21:2181'
path = '/hadoop-ha/PHMBIGDATA/ActiveBreadCrumb'

nameNodeADevelop = [b'spark.bigdevelop.com', 'spark.bigdevelop.com']
nameNodeBDevelop = [b'nnredis.bigdevelop.com', 'nnredis.bigdevelop.com']
zooQuorumDevelop = 'datanode1.bigdevelop.com:2181,schedule.bigdevelop.com:2181,datanode2.bigdevelop.com:2181'
pathDevelop = '/hadoop-ha/hanamenode/ActiveBreadCrumb'

zk = KazooClient(hosts=zooQuorum)
zk.start()
if zk.exists(path):
    data = zk.get(path)
    print(data)
