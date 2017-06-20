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

file_path = 'H:/lottery_process/AWR_Report_Parse/ttdb21_awr'
file_names = os.listdir(file_path)

for i in file_names:
	tr_total.append(get_tr(file_path+"/"+i))  # 全部表格的每一行（数据结构为[[table1],[table2],..[table n]],table1中为[tr1,tr2....]）

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
for i in range(len(file_names)):
    Time.append(file_names[i].split("_")[-1].split(".")[0])

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

top5_events = []
for i in range(len(table_result)):
    top5_events.append(top5_event(table_result[i][7]))

