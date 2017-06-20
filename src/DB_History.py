# -*- coding:utf-8 -*-
'''
目标：将lottery_oracle数据库中的awr_values表当前日期的一星期前数据导入到历史数据库中，维持数据库性能
'''

import pymysql
'''
创建数据库history_data
'''
'''
建立存放历史数据库！(保留全部数据，保证现有数据库只有一周数据）
'''
create_db = '''
CREATE DATABASE history_data;
'''

connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707")

with connection.cursor() as cursor:
    print("------create new database history_data-------")
    cursor.execute(create_db)
    connection.commit()
create_hosts_sql = """\
CREATE TABLE hosts(
Host_ID int PRIMARY KEY,
DB_ID CHAR(20) UNIQUE,
Instance CHAR(20),
Host CHAR(20))
"""

insert_hosts_sql = """\
INSERT INTO hosts(Host_ID,DB_ID,Instance,Host)
 VALUES('{Host_ID}','{DB_ID}','{Instance}','{Host}')
"""
connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707",db="history_data")
with connection.cursor() as cursor:
    print("------create new table history_data(hosts)--------")
    cursor.execute(create_hosts_sql)
    connection.commit()
connection.close()


select_host_sql = '''
    select * from hosts;
'''
connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707",db="lottery_oracle")
cur = connection.cursor()
print("------select data from tables--------")
cur.execute(select_host_sql)
connection.close()
hosts = cur.fetchall()

connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707",db="history_data")
cur = connection.cursor()
for i in range(len(set(hosts))):
    print("------insert 第%d行into history_data(hosts)--------" % (i + 1))
    cur.execute(insert_hosts_sql.format(Host_ID=hosts[i][0], DB_ID=hosts[i][1],
                                               Instance=hosts[i][2],
                                               Host=hosts[i][3]))
    connection.commit()
connection.close()




select_awr_values = """
    Select * from pttdb21 where time between '2017-03-14 01:00:54' and '2017-03-21 00:00:26';
"""

connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707",db="lottery_oracle")
cur = connection.cursor()
print("------select data from lottery_oracle(pttdb21)--------")
cur.execute(select_awr_values)
connection.close()
lst_week = cur.fetchall()

create_awr_values_sql = """\
CREATE TABLE pttdb21(
TIME datetime UNIQUE PRIMARY KEY,
Host_ID int,
Host CHAR(20),
Elapsed FLOAT(2),
DB_Time FLOAT(2),
Redo_Size FLOAT(2),
Logical_Reads FLOAT(2),
Block_Changes FLOAT(2),
Physical_Reads FLOAT(2),
Physical_Writes FLOAT(2),
User_Calls FLOAT(2),
Parses FLOAT(2),
Hard_Parses FLOAT(2),
Sorts FLOAT(2),
Logons FLOAT(2),
Executes FLOAT(2),
Buffer_Hit FLOAT(2),
Top5_Events CHAR(255),
FOREIGN KEY (Host_ID) REFERENCES Hosts(Host_ID)
)
"""
insert_awr_values_sql = """\
INSERT INTO pttdb21(TIME,Host_ID,Host,Elapsed,DB_Time,Redo_Size,Logical_Reads,
Block_Changes,Physical_Reads,Physical_Writes,User_Calls,Parses,Hard_Parses,Sorts,Logons,Executes,
Buffer_Hit,Top5_Events
)
 VALUES('{TIME}','{Host_ID}','{Host}','{Elapsed}','{DB_Time}' ,
'{Redo_Size}','{Logical_Reads}','{Block_Changes}','{Physical_Reads}','{Physical_Writes}',
'{User_Calls}','{Parses}','{Hard_Parses}','{Sorts}','{Logons}','{Executes}','{Buffer_Hit}','{Top5_Events}')
"""

connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707",db="history_data")
cur = connection.cursor()
cur.execute(create_awr_values_sql)
print("------create new table history_data(pttdb21)--------")
for i in range(len(lst_week)):
    cur.execute(insert_awr_values_sql.format(TIME=lst_week[i][0], Host_ID=lst_week[i][1],
                                                Host=lst_week[i][2],
                                                Elapsed=lst_week[i][3],
                                                DB_Time=lst_week[i][4],
                                                Redo_Size=lst_week[i][5],
                                                Logical_Reads=lst_week[i][6],
                                                Block_Changes=lst_week[i][7],
                                                Physical_Reads=lst_week[i][8],
                                                Physical_Writes=lst_week[i][9],
                                                User_Calls=lst_week[i][10],
                                                Parses=lst_week[i][11],
                                                Hard_Parses=lst_week[i][12],
                                                Sorts=lst_week[i][13],
                                                Logons=lst_week[i][14],
                                                Executes=lst_week[i][15],
                                                Buffer_Hit=lst_week[i][16],
                                                Top5_Events=lst_week[i][17]
                                                ))
    print("-------insert %d last week awr_values data to history_data(pttdb21)-------"%(i+1))
    connection.commit()
connection.close()

'''

connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707",db="lottery_oracle")

update_lottery_oracle = """
    delete from awr_values where time between '2017-03-14 01:00:54' and '2017-03-21 00:00:26';
"""
cur = connection.cursor()
cur.execute(update_lottery_oracle)
print("上周数据转移完毕！")
connection.commit()
connection.close()

'''




