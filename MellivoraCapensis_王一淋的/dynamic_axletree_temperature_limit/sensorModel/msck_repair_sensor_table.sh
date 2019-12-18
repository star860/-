#!/bin/bash
######################################################################
#                                                                    #
# 程    序：msck_repair_sensor_table.sh                            #
# 创建时间：2018年11月15日                                           #
# 创 建 者：lzb                                                      #
# 参数:                                                              #
#    参数1：日期+小时[yyyymmdd]                                      #
# 补充说明：按天处理                                                 #
# 功能：  修复轴温数据表和轴温模型表                                #
#                                                                    #
######################################################################
User='phmdm'
PassWord='ebdcea82'

table1='dm_i32_sensor_online_model_dm_20181109'
sql1="msck repair table $table1;"

table2='dm_i32_sensor_online_dm3'
sql2="msck repair table $table2;"



echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>`date`:执行插入语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
beeline -u "jdbc:hive2://phmkfk1.bigdata.com:2181,phmkfk2.bigdata.com:2181,phmkfk3.bigdata.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n $User  -p $PassWord\
 -e "$sql1" \
 -e "$sql2" "!quit"

#hive -e "$sql1; $sql2;" -v

echo 'DONE!'
