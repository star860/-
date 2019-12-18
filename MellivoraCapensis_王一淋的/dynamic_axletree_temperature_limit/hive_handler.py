import subprocess


class HiveHandler:
    def __init__(self):
        self.extract_to_file = '/opt/phmdm/wangyilin/data_files/real_time_all_axletree_temperature.txt'

    def exec_sql(self, sql):
        sql_command = '''hive -e "{}"> {}'''.format(sql, self.extract_to_file)
        subprocess.call(sql_command, shell=True)

    def fetch_data(self):
        result_list = []
        with open(self.extract_to_file, 'r') as ff:
            for line in ff:
                line_sp = line.split(',')
                print(line_sp)
                result_list.append(line_sp)
        return result_list


if __name__ == '__main__':
    hh = HiveHandler()
    hsql = 'select * from real_time_all_axletree_temperature limit 10'
    hh.exec_sql(hsql)
    hh.fetch_data()
