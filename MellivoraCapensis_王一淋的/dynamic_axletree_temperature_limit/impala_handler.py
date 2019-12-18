from impala.dbapi import connect

hive_connection = connect(host='10.73.95.1',
                          port=10000,
                          auth_mechanism='PLAIN',
                          user='phmdm',
                          password='ebdcea82',
                          database='default')
curs = hive_connection.cursor()
curs.execute("""
                    SELECT
                        * 
                    FROM
                        ODS_CUX_PHM_OPENSTRINGINFO_FORSF_V 
                    WHERE
                        TIME >= '2018-06-01' 
                        AND TIME < '2018-06-02'
                        and op_day = '20190601'
""")

result = curs.fetchone()
print(result)
