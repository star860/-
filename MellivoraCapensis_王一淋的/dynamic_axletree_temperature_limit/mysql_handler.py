import pymysql


class MysqlHandler:
    """
    数据库操作类
    """
    def __init__(self):
        # 连接数据库
        self.conn = pymysql.connect(host='localhost',
                                    user='root',
                                    password='rootroot',
                                    database='forge',
                                    port=3306,
                                    # 设置pymysql的字符串格式
                                    charset='utf8')
        self.cursor = self.conn.cursor()

    def get_data_name(self, data_code):
        sql = 'select data_name from forge.dim_phm_trunscate_rules where data_code = "{}"'.format(data_code)
        self.cursor.execute(sql)
        result_tuple = self.cursor.fetchall()
        if len(result_tuple) > 1:
            raise Exception('{}:一个data_code获得多个data_name'.format(data_code))
        elif len(result_tuple) < 1:
            print('{}:未查找到对应的data_name'.format(data_code))
            data_name = data_code
        else:
            data_name = result_tuple[0][0]
        return data_name

    def close_connection(self):
        self.conn.close()
