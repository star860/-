import mysql_handler


class DynamicBearingTemperature:
    def __init__(self):
        self.mq_handler = mysql_handler.MysqlHandler()

    def filter_data(self):
        with open('hbase_data.txt', 'r', encoding='utf8') as hd:
            for line in hd:
                data_list = line.split(' ', maxsplit=1)
                if len(data_list) == 2:
                    data_code = data_list[0].split(':')[1]
                    data_name = self.mq_handler.get_data_name(data_code)
                    data_value = data_list[1]
                    print((data_name+'--'+data_value).strip())
                else:
                    print(data_list[0])


if __name__ == '__main__':
    dynamic_bt = DynamicBearingTemperature()
    dynamic_bt.filter_data()
