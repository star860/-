#生产环境
mysql_host = '10.73.95.29'
mysql_port = 3307
mysql_userName = 'phmconf'
mysql_password = 'FPW/JGN9vmml3Q=='
mysql_dbName = 'webdata'
hbase_host = '10.73.95.1'
hive_host = '10.73.95.1'
hive_port = 10000
hive_user = 'phmdm'
hive_passwd = 'ebdcea82'
hive_db = 'default'

## 开发环境
dev_hbase_host='10.73.95.71'


# 本地环境
local_mysql_host = '127.0.0.1'
local_mysql_port = 3306
local_mysql_userName = 'root'
local_mysql_password = '123456'
local_mysql_dbName = 'db'

p_32 = {
    'A06': 'trainsite_id'   # 车次
    ,'A09': 'speed'         # 速度
    ,'A10': 'mileage'       # 运营里程
    ,'C16': 'netVolt'     # 网压
}

cols_32={}


p_37={
    'GPS00':'gps_valid',                #GPS有效位
    'GPS01':'satellite_num',            #定位的卫星数
    'GPS02':'target_type',              #定位方式
    'GPS03':'gps_time',                 #GPS时间
    'GPS04':'longitude',                #经度
    'GPS05':'longitude_direction',      #经度方向
    'GPS06':'latitude',                 #纬度
    'GPS07':'latitude_direction',       #纬度方向
    'GPS08':'speed',                    #速度
    'GPS09':'speed_direction',          #速度方向
    'GPS10':'altitude'                  #高度
}


cols_37={
    'gps_valid':'0',                    #GPS有效位
    'satellite_num':'1',                #定位的卫星数
    'target_type':'2',                  #定位方式
    'gps_time':'3',                     #GPS时间
    'longitude':'4',                    #经度
    'longitude_direction':'5',          #经度方向
    'latitude':'6',                     #纬度
    'latitude_direction':'7',           #纬度方向
    'speed':'8',                        #速度
    'speed_direction':'9',              #速度方向
    'altitude':'10'                     #高度
}

