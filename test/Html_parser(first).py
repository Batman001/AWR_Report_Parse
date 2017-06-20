# -*-coding:utf-8 -*-
import codecs
import os
from bs4 import BeautifulSoup as BS
import pandas as pd

'''
get_tr方法得到HTML文件中全部table中的每一行tr,并存储在长度为len(table)列表中
'''
def get_tr(file_path):
	count = open(file_path,encoding="gbk").readlines()
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
            th=tr_n[i].find_all("th")
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
    return (attribute,value)

	
tr_total = []

file_path = 'H:/AWR_Report_Parse/ttdb21_awr'
file_names = os.listdir(file_path)

for i in file_names:
    tr_total.append(get_tr(file_path+"/"+i)) #全部表格的每一行（数据结构为[[table1],[table2],..[table n]],table1中为[tr1,tr2....]）

table_total = [[] for i in range(len(tr_total))] #创建table列表，table中每一元素为(attribute,value)的元组


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
    col = [[] for i in range(len(t[0])-1)]
    for i in range(1,len(t[0])):
        for j in range(len(t[1])):
            col[i-1].append(t[1][j][i])
    s = [[] for i in range(len(col))]
    for i in range(len(col)):
        s[i] = pd.Series(col[i],index = index,name=t[0][i+1])
        
    
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
    return [l1,l2]
 

'''
没有标题的表格生成Series
'''

def series_create(t):
	if len(t[1][0]) == 4:
		temp = []
		for i in t[1]:
			temp.append(list2split(i))
		result=  []
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
	s = pd.Series(col,index=index)
	return s

	
table_result = [[[] for j in range(len(table_total[0]))] for i in range(len(table_total))]	
for i in range(len(table_total)):
    for j in range(len(table_total[i])):
        if table_total[i][j][0] == []:
            table_result[i][j] = series_create(table_total[i][j])
        else:
            table_result[i][j] = df_create(table_total[i][j])

temp = [[[] for j in range(len(table_total[0]))] for i in range(len(table_total))]

for i in range(len(table_result)):
    for j in range(len(table_result[i])):
        if isinstance(table_result[i][j],pd.Series):
            table_result[i][j] = table_result[i][j].to_frame()
            temp[i][j] = table_result[i][j]
        else:
            temp[i][j] = table_result[i][j]

table_result = temp


DB_Time = []

for i in range(len(table_result)):
	DB_Time.append(float(table_result[i][1]["Snap Time"]["DB Time:"].split("(mins)")[0]))
                  
AAS = []

for i in range(len(table_result)):
    AAS.append(float(table_result[i][1]["Snap Time"]["DB Time:"].split("(mins)")[0])/float(table_result[i][1]["Snap Time"]["Elapsed:"].split("(mins)")[0]))

Logical_Read = []

for i in range(len(table_result)):
    Logical_Read.append(float(table_result[i][3]["Per Second"]["Logical reads:"].replace(",",'')))

Physical_Read = []

for i in range(len(table_result)):
    Physical_Read.append(float(table_result[i][3]["Per Second"]["Physical reads:"].replace(",",'')))

Hard_Parses = []

for i in range(len(table_result)):
    Hard_Parses.append(float(table_result[i][3]["Per Second"]["Hard parses:"].replace(",",'')))

import xlsxwriter

bk = xlsxwriter.Workbook("C:/users/sunchao/desktop/awr_values.xlsx")
sh = bk.add_worksheet("AWR_Values")

def excel_write(sheet,row,col,txt):
    sheet.write(row,col,txt)

excel_write(sh,0,0,"Time")
excel_write(sh,0,1,"DB_Time")
excel_write(sh,0,2,"AAS")
excel_write(sh,0,3,"Logical_Read")
excel_write(sh,0,4,"Physical_Read")
excel_write(sh,0,5,"Hard_Parses")

Time = []
for i in range(len(file_names)):
	Time.append(file_names[i].split("_")[-1].split(".")[0])

for i in range(len(Time)):
    excel_write(sh,i+1,0,Time[i])

for i in range(len(DB_Time)):
    excel_write(sh,i+1,1,DB_Time[i])

for i in range(len(AAS)):
    excel_write(sh,i+1,2,AAS[i])

for i in range(len(Logical_Read)):
    excel_write(sh,i+1,3,Logical_Read[i])

for i in range(len(Physical_Read)):
    excel_write(sh,i+1,4,Physical_Read[i])

for i in range(len(Hard_Parses)):
    excel_write(sh,i+1,5,Hard_Parses[i])

bk.close()

print("AWR Values into Excel Done!")

import pymysql

create_table_sql = """\
CREATE TABLE AWR_VALUES(
ID int PRIMARY KEY,
TIME CHAR(20) UNIQUE,
DB_Time FLOAT(2)  ,
AAS FLOAT(2)  ,
Logical_Read FLOAT(2),
Physical_Read FLOAT(2),
Hard_Parses FLOAT(2)
)
"""
insert_table_sql = """\
INSERT INTO AWR_VALUES(ID,TIME,DB_Time,AAS,Logical_Read,Physical_Read,Hard_Parses)
 VALUES('{ID}','{TIME}','{DB_Time}','{AAS}','{Logical_Read}','{Physical_Read}','{Hard_Parses}')
"""

config = {
          'host':'127.0.0.1',
          'port':3306,
          'user':'root',
          'password':'Newworld0707',
          'db':'lottery',
          'charset':'utf8mb4',
          'cursorclass':pymysql.cursors.DictCursor,
          }

connection = pymysql.connect(**config)

with connection.cursor() as cursor:
    print("------create new tables--------")
    cursor.execute(create_table_sql)
    connection.commit()


for i in range(len(Time)):
    with connection.cursor() as cursor:
        print("------insert 第%d行into tables--------" %(i+1))
        cursor.execute(insert_table_sql.format(ID=i+1,TIME=Time[i],DB_Time=DB_Time[i],
                                               AAS=AAS[i],Logical_Read=Logical_Read[i],
                                               Physical_Read=Physical_Read[i],
                                               Hard_Parses=Hard_Parses[i]))
        connection.commit()


connection.close()   
print("Data write into Mysql Done!")

