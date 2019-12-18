# -*- coding: utf-8 -*-

import sys
import time
import datetime
import traceback
from kazoo.client import KazooClient
import hdfs
import param_conf


class kazooClient:
    nameNodeA = [b'phmnn1.bigdata.com', 'phmnn1.bigdata.com']
    nameNodeB = [b'phmnn2.bigdata.com', 'phmnn2.bigdata.com']

    Develop_nameNodeA = [b'spark.bigdevelop.com', 'spark.bigdevelop.com']
    Develop_nameNodeB = [b'nnredis.bigdevelop.com', 'nnredis.bigdevelop.com']

    zooQuorum = 'phmkfk1.bigdata.com:2181,phmkfk2.bigdata.com:2181,phmkfk3.bigdata.com:2181'
    zooQuorumDevelop = 'datanode1.bigdevelop.com:2181,schedule.bigdevelop.com:2181,datanode2.bigdevelop.com:2181'

    path = '/hadoop-ha/PHMBIGDATA/ActiveBreadCrumb'
    pathDevelop = '/hadoop-ha/hanamenode/ActiveBreadCrumb'

    client=None
    zk=None
    def __init__(self):
        try:
            self.zk = KazooClient(hosts=self.zooQuorum)
            self.zk.start()
            if self.zk.exists(self.path):
                data = self.zk.get(self.path)
                ip = None
                if self.nameNodeA[0] in data[0]:
                    ip = 'http://' + self.nameNodeA[1] + ':50070'
                elif self.nameNodeB[0] in data[0]:
                    ip = 'http://' + self.nameNodeB[1] + ':50070'

                self.client = hdfs.Client(ip, root='/')
        except Exception as e:
            print('%s kazooClient __init__ ERROR! %s' % (datetime.datetime.now(),traceback.format_exc()))

    def __del__(self):
        if self.zk:
            self.zk.stop()
            self.zk.close()
        if self.client:
            del (self.client)

    def sessionClose(self):
        if self.zk:
            self.zk.stop()
            self.zk.close()
        if self.client:
            del (self.client)


if __name__ == '__main__':
    hdfs=kazooClient()
    hdfs.sessionClose()

