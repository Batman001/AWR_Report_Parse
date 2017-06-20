# -*-coding:utf-8 -*-
import codecs
import os
from bs4 import BeautifulSoup as BS
import pandas as pd
import datetime
'''
get_tr方法得到HTML文件中全部table中的每一行tr,并存储在长度为len(table)列表中
'''

def get_tr(file_path):
    f = open(file_path, encoding="gbk")
    count = f.readlines()
    f.close()
    str = count[0:count.index('Main Report\n')]
    html = ''
    for i in str :
        html = html + i
    soup = BS(html,'html.parser')
    t = soup.find_all("table")
    tr = []
    for i in t:
        tr.append(i.find_all("tr"))
    return tr


'''
parse_table方法传入参数tr_n，即为每一个table生成的tr列表
'''


def parse_table(tr_n):
    th = []
    td = []
    for i in range(len(tr_n)):
        if i == 0 and tr_n[i].find("th") != None:
            th = tr_n[i].find_all("th")
        else:
            td.append(tr_n[i].find_all("td"))
        attribute = []
        for i in range(len(th)):
            if th[i].contents == []:
                attribute.append('')
            else:
                attribute.append(th[i].contents[0].strip())
        value = [[] for i in range(len(td))]
        for i in range(len(td)):
            for j in range(len(td[i])):
                value[i].append(td[i][j].contents[0].strip())
    return (attribute, value)


tr_total = []

file_path = 'C:/Users/sunchao/Desktop/AWR_Report_Parse/ttdb21_awr'
file_names = os.listdir(file_path)
for i in range(len(file_names)):
    tr_total.append(get_tr(file_path+"/"+file_names[i]))
    print("第%d篇文档处理完毕"%(i+1)) # 全部表格的每一行（数据结构为[[table1],[table2],..[table n]],table1中为[tr1,tr2....]）

table_total = [[] for i in range(len(tr_total))]  # 创建table列表，table中每一元素为(attribute,value)的元组

for i in range(len(tr_total)):
    for j in range(len(tr_total[i])):
        table_total[i].append(parse_table(tr_total[i][j]))

'''
有列标题的表格最后生成DataFrame
'''

def df_create(t):
    index = []
    for i in t[1]:
        index.append(i[0])
    col = [[] for i in range(len(t[0]) - 1)]
    for i in range(1, len(t[0])):
        for j in range(len(t[1])):
            col[i - 1].append(t[1][j][i])
    s = [[] for i in range(len(col))]
    for i in range(len(col)):
        s[i] = pd.Series(col[i], index=index, name=t[0][i + 1])

    df = pd.DataFrame(s).T
    return df


'''
针对第5个表格（无标题）但出现索引叠加情况，特对此进行处理 例如说[1,2,3,4] >> [[1,2],[3,4]]
'''


def list2split(list):
    l1 = []
    l2 = []
    for i in range(len(list)):
        if i == 0 or i == 1:
            l1.append(list[i])
        else:
            l2.append(list[i])
    return [l1, l2]

'''
没有标题的表格生成Series
'''

def series_create(t):
    if len(t[1][0]) == 4:
        temp = []
        for i in t[1]:
            temp.append(list2split(i))
        result = []
        for i in temp:
            for j in i:
                result.append(j)
    else:
        result = t[1]
    index = []
    for i in result:
        index.append(i[0])
    col = []
    for i in result:
        col.append(i[-1])
    s = pd.Series(col, index=index)
    return s

table_result = [[[] for j in range(len(table_total[0]))] for i in range(len(table_total))] #列表生成式，生成对应文件的表格数据结构
for i in range(len(table_total)):
    for j in range(len(table_total[i])):
        if table_total[i][j][0] == []:
            table_result[i][j] = series_create(table_total[i][j])
        else:
            table_result[i][j] = df_create(table_total[i][j])

temp = [[[] for j in range(len(table_total[0]))] for i in range(len(table_total))]

for i in range(len(table_result)):
    for j in range(len(table_result[i])):
        if isinstance(table_result[i][j], pd.Series):
            table_result[i][j] = table_result[i][j].to_frame()
            temp[i][j] = table_result[i][j]
        else:
            temp[i][j] = table_result[i][j]

table_result = temp


Time = []
'''
str2time方法是将html报告中End Snap对应time时间由str类型更改为datetime类型
'''
def str2time(t):
    t = datetime.datetime.strptime(t,'%d-%b-%y %H:%M:%S')
    return t
for i in range(len(table_result)):
    Time.append(str2time(table_result[i][1]["Snap Time"]["End Snap:"]))



'''
第1张表中选取指标DB ID,Instance,Host(保留Host字段)
'''

DB_ID = [] #DB_ID元素类型为str
for i in range(len(table_result)):
    DB_ID.append(table_result[i][0]["DB Id"][0])

Instance = [] #Instance元素类型为str
for i in range(len(table_result)):
    Instance.append(table_result[i][0]["Instance"][0])


Host = [] #Host元素类型为str
for i in range(len(table_result)):
    Host.append(table_result[i][0]["Host"][0])

'''
第2张表中选取指标Elapsed,DB Time
'''

Elapsed = [] #Elapsed元素类型为float，单位为(mins)

for i in range(len(table_result)):
    Elapsed.append(float(table_result[i][1]["Snap Time"]["Elapsed:"].split("(mins)")[0]))

DB_Time = [] #DB_Time元素类型为float，单位为(mins)

for i in range(len(table_result)):
    DB_Time.append(float(table_result[i][1]["Snap Time"]["DB Time:"].split("(mins)")[0]))

'''
第4张表选取指标 Redo Size,Logical reads,Block changes,Physical reads,Physical writes,User calls,Parses,Hard parses,Sorts,Logons,Executes
'''
Redo_Size = []
for i in range(len(table_result)):
    Redo_Size.append(float(table_result[i][3]["Per Transaction"]["Redo size:"].replace(",", '')))

Logical_Reads = []
for i in range(len(table_result)):
    Logical_Reads.append(float(table_result[i][3]["Per Transaction"]["Logical reads:"].replace(",", '')))

Block_Changes = []
for i in range(len(table_result)):
    Block_Changes.append(float(table_result[i][3]["Per Transaction"]["Block changes:"].replace(",", '')))

Physical_Reads = []
for i in range(len(table_result)):
    Physical_Reads.append(float(table_result[i][3]["Per Transaction"]["Physical reads:"].replace(",", '')))

Physical_Writes = []
for i in range(len(table_result)):
    Physical_Writes.append(float(table_result[i][3]["Per Transaction"]["Physical writes:"].replace(",", '')))

User_Calls = []
for i in range(len(table_result)):
    User_Calls.append(float(table_result[i][3]["Per Transaction"]["User calls:"].replace(",", '')))

Parses = []
for i in range(len(table_result)):
    Parses.append(float(table_result[i][3]["Per Transaction"]["Parses:"].replace(",", '')))

Hard_Parses = []
for i in range(len(table_result)):
    Hard_Parses.append(float(table_result[i][3]["Per Transaction"]["Hard parses:"].replace(",", '')))

Sorts = []
for i in range(len(table_result)):
    Sorts.append(float(table_result[i][3]["Per Transaction"]["Sorts:"].replace(",", '')))

Logons = []
for i in range(len(table_result)):
    Logons.append(float(table_result[i][3]["Per Transaction"]["Logons:"].replace(",", '')))

Executes = []
for i in range(len(table_result)):
    Executes.append(float(table_result[i][3]["Per Transaction"]["Executes:"].replace(",", '')))

'''
第6张表中选取指标Buffer Hit %
'''
Buffer_Hit = []
for i in range(len(table_result)):
    Buffer_Hit.append(float(table_result[i][5][0]["Buffer  Hit   %:"]))
'''
第8张表选取指标Event,%Total Call Time
'''
'''
top5_event传入参数 df 为每个html报告的第8张表的DataFrame格式数据
'''
def top5_event(df):
    d = df["% Total Call Time"].to_dict() #将对应列的Series生成字典形式数据
    #d = sorted(d.items(), key=lambda d:float(d[1]), reverse = True)#对字典按照value值进行排序,d为列表型数据，d中每一个元素为包含事件与value值的元组。
    return d

Top5_Events = []
for i in range(len(table_result)):
    Top5_Events.append(top5_event(table_result[i][7]))




'''
import xlsxwriter

bk = xlsxwriter.Workbook("C:/users/sunchao/Desktop/awr_values.xlsx")
sh = bk.add_worksheet("AWR_Values")


def excel_write(sheet, row, col, txt):
    sheet.write(row, col, txt)


excel_write(sh, 0, 0, "Time")
#excel_write(sh, 0, 1, "DB ID")
#excel_write(sh, 0, 2, "Instance")
excel_write(sh, 0, 3, "Host")
excel_write(sh, 0, 4, "Elapsed")
excel_write(sh, 0, 5, "DB Time")
excel_write(sh, 0, 6, "Redo Size")
excel_write(sh, 0, 7, "Logical Reads")
excel_write(sh, 0, 8, "Block Changes")
excel_write(sh, 0, 9, "Physical Reads")
excel_write(sh, 0, 10, "Physical Writes")
excel_write(sh, 0, 11, "User Calls")
excel_write(sh, 0, 12, "Parses")
excel_write(sh, 0, 13, "Hard Parses")
excel_write(sh, 0, 14, "Sorts")
excel_write(sh, 0, 15, "Logons")
excel_write(sh, 0, 16, "Executes")
excel_write(sh, 0, 17, "Buffer Hit %")
excel_write(sh, 0, 18, "Top 5 Timed Events")



for i in range(len(Time)):
    excel_write(sh, i + 1, 0, Time[i])

#for i in range(len(DB_ID)):
    #excel_write(sh, i + 1, 1, DB_ID[i])

#for i in range(len(Instance)):
    #excel_write(sh, i + 1, 2, Instance[i])

for i in range(len(Host)):
    excel_write(sh, i + 1, 3, Host[i])

for i in range(len(Elapsed)):
    excel_write(sh, i + 1, 4, Elapsed[i])

for i in range(len(DB_Time)):
    excel_write(sh, i + 1, 5, DB_Time[i])

for i in range(len(Redo_Size)):
    excel_write(sh, i + 1, 6, Redo_Size[i])

for i in range(len(Logical_Reads)):
    excel_write(sh, i + 1, 7, Logical_Reads[i])

for i in range(len(Block_changes)):
    excel_write(sh, i + 1, 8, Block_changes[i])

for i in range(len(Physical_Reads)):
    excel_write(sh, i + 1, 9, Physical_Reads[i])

for i in range(len(Physical_writes)):
    excel_write(sh, i + 1, 10, Physical_Writes[i])

for i in range(len(User_calls)):
    excel_write(sh, i + 1, 11, User_Calls[i])

for i in range(len(Parses)):
    excel_write(sh, i + 1, 12, Parses[i])

for i in range(len(Hard_Parses)):
    excel_write(sh, i + 1, 13, Hard_Parses[i])

for i in range(len(Sorts)):
    excel_write(sh, i + 1, 14, Sorts[i])

for i in range(len(Logons)):
    excel_write(sh, i + 1, 15, Logons[i])

for i in range(len(Executes)):
    excel_write(sh, i + 1, 16, Executes[i])

for i in range(len(Buffer_Hit)):
    excel_write(sh, i + 1, 17, Buffer_Hit[i])

for i in range(len(top5_events)):
    excel_write(sh, i + 1, 18, str(Top5_Events[i]))

bk.close()

print("AWR Values into Excel Done!")

'''

import pymysql
import json

create_db = '''
CREATE DATABASE lottery_oracle;
'''

connection = pymysql.connect(host='127.0.0.1',port=3306,user="root",password="Newworld0707")

with connection.cursor() as cursor:
    print("------create new database lottery_oracle-------")
    cursor.execute(create_db)
    connection.commit()

'''
先在数据库中创建Host表，并将host作为主键
'''
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
config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'Newworld0707',
    'db': 'lottery_oracle',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
}

connection = pymysql.connect(**config)

with connection.cursor() as cursor:
    print("------create new table lottery_oracle(hosts)--------")
    cursor.execute(create_hosts_sql)
    connection.commit()

for i in range(len(set(Host))):
    with connection.cursor() as cursor:
        print("------insert 第%d行into lottery_oracle(hosts) tables--------" % (i + 1))
        cursor.execute(insert_hosts_sql.format(Host_ID=i + 1, DB_ID=list(set(DB_ID))[i],
                                               Instance=list(set(Instance))[i],
                                               Host=list(set(Host))[i]))
        connection.commit()

select_host_id = """
    select Host_ID from Hosts where Host="pttdb21";
"""

cur = connection.cursor()
cur.execute(select_host_id)
Host_ID = cur.fetchall()

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

with connection.cursor() as cursor:
    print("------create new table lottery_oracle(pttdb21)--------")
    cursor.execute(create_awr_values_sql)
    connection.commit()

for i in range(len(Time)):
    with connection.cursor() as cursor:
        print("------insert 第%d行into lottery_oracle(pttdb21)--------" % (i + 1))
        cursor.execute(insert_awr_values_sql.format(TIME=Time[i], Host_ID=Host_ID[0]["Host_ID"],
                                               Host=Host[i],
                                               Elapsed=Elapsed[i],
                                               DB_Time= DB_Time[i],
                                               Redo_Size=Redo_Size[i],
                                               Logical_Reads=Logical_Reads[i],
                                               Block_Changes=Block_Changes[i],
                                               Physical_Reads=Physical_Reads[i],
                                               Physical_Writes=Physical_Writes[i],
                                               User_Calls=User_Calls[i],
                                               Parses=Parses[i],
                                               Hard_Parses=Hard_Parses[i],
                                               Sorts=Sorts[i],
                                               Logons=Logons[i],
                                               Executes=Executes[i],
                                               Buffer_Hit=Buffer_Hit[i],
                                               Top5_Events = json.dumps(Top5_Events[i])
                                                ))
        connection.commit()
connection.close()
print("Data write into Mysql Done!")


