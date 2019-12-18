#!/bin/sh
### count=`ps -ef |grep /opt/phmdm/sf_bin/i32/lib/sensor_online_model.py |grep -v "grep" |wc -l`
count=`ps -ef |grep sensor_online_model.py |grep -v "grep" |wc -l`
if [ 0 == $count ];then
nohup python /opt/phmdm/sf_bin/i32/sensorModel/sensor_online_model.py >> /data13/phmdmlog/res.log 2>&1 &
fi


