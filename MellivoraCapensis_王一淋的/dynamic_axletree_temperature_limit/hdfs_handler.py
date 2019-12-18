# -*- coding: utf-8 -*-
######################################################################
#                                                                    #
# 创建时间：2019年09月16日                                           #
# 创 建 者：wyl                                                      #
# 功能：写入hive测试脚本                                              #
#                                                                    #
######################################################################

# 如果不是分区表,则直接写入hdfs文件里面就可以了
# 如果是分区表,需要用repair命令,将hdfs里面的分区文件加入到hive的元数据
# 当前hadoop为基于zookeeper的高可用ha架构

from kazoo.client import KazooClient
import hdfs


class HdfsHandler:
    # 两个namenode为active和standby
    name_node_a_prd = [b'phmnn1.bigdata.com', 'phmnn1.bigdata.com']
    name_node_b_prd = [b'phmnn2.bigdata.com', 'phmnn2.bigdata.com']
    # zookeeper的三个节点地址
    zoo_quorum_prd = 'phmkfk1.bigdata.com:2181,phmkfk2.bigdata.com:2181,phmkfk3.bigdata.com:2181'
    # zookeeper的分布式同步目录,在此目录下可以读取到当前active的namenode节点
    path_prd = '/hadoop-ha/PHMBIGDATA/ActiveBreadCrumb'

    name_node_a_dev = [b'hbase.bigdevelop.com', 'hbase.bigdevelop.com']
    name_node_b_dev = [b'nnredis.bigdevelop.com', 'nnredis.bigdevelop.com']
    zoo_quorum_dev = 'datanode1.bigdevelop.com:2181,schedule.bigdevelop.com:2181,datanode2.bigdevelop.com:2181'
    path_dev = '/hadoop-ha/hanamenode/ActiveBreadCrumb'

    def __init__(self):
        # 连接zookeeper
        self.zk = KazooClient(hosts=self.zoo_quorum_dev)
        self.zk.start()
        # 判断是否存在zookeeper同步
        if self.zk.exists(self.path_dev):
            # 获取当前active的namenode
            data = self.zk.get(self.path_dev)
            ip = None
            if self.name_node_a_dev[0] in data[0]:
                ip = 'http://' + self.name_node_a_dev[1] + ':50070'
            elif self.name_node_b_dev[0] in data[0]:
                ip = 'http://' + self.name_node_b_dev[1] + ':50070'

            if ip:
                # 连接active的hdfs namenode
                self.client = hdfs.Client(ip, root='/')
                if not self.client:
                    print('hdfs client 建立连接失败!')
            else:
                print('nameNode IP 获取失败!')

    def test_write_into_hdfs_file(self):
        """
        将数据写入hdfs文件
        :return:
        """
        hdfs_path = '/apps/hive/warehouse/real_time_axletree_temperature/000000_0'
        contents = '京沪线,3002,1,2,3,N,N,N,N,N,N,N,N,N,N,N,N\n'
        contents_d = contents.encode('utf8')
        self.client.write(hdfs_path, contents_d, append=True)
        print('\033[1;33;0m写入成功!!! \033[0m')

    def test_read_hdfs_file(self):
        # cl = self.client.list('/apps/hive/warehouse/real_time_axletree_temperature')
        # print(cl)
        file_path = '/apps/hive/warehouse/real_time_all_axletree_temperature/000000_0'
        with self.client.read(file_path, encoding='utf-8') as fr:
            data_str = fr.read()
            lines = data_str.split('\n')
            for line in lines:
                print(line)

    def write_into_hdfs_file(self, contents):
        """
        将数据写入hdfs文件
        """
        hdfs_path = '/apps/hive/warehouse/real_time_all_axletree_temperature/000000_0'
        # contents = '京沪线,3002,1,2,3,N,N,N,N,N,N,N,N,N,N,N,N\n'
        contents_d = contents.encode('utf8')
        self.client.write(hdfs_path, contents_d, append=True)
        print('\033[1;33;0m写入成功!!! \033[0m')


if __name__ == '__main__':
    hh = HdfsHandler()
    # hh.test_write_into_hdfs_file()
    hh.test_read_hdfs_file()
