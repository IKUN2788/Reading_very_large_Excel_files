# -*- coding: UTF-8 -*-
'''
@Project ：华东2组-牛亚丽-宝尊透视-0205953313 
@File    ：3-透视汇总.py
@IDE     ：PyCharm 
@Author  ：庄志权(01426442)
@Date    ：2026/2/28 10:16 
'''

import os
import time
import duckdb
import openpyxl

table_name = 'sftable'
wb = openpyxl.Workbook()
ws = wb.active
ws.append(['月结卡号', '总单量', '总计费重量', '总应付金额'])
for db_name in os.listdir("duckdb_output"):
    con = duckdb.connect(f'duckdb_output/{db_name}')
    print(f"加载数据库 {db_name} ing")
    # con.sql("SELECT * FROM excel_data LIMIT 100").show()
    # 方法 1: 使用 fetchall() 一次性获取所有行 (适合小数据量)

    # 显示所有表
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"所有表: {tables}")

    t1 = time.time()
    cursor = con.execute(f"SELECT * FROM '{table_name}' LIMIT 0")
    column_names = [desc[0] for desc in cursor.description]

    运单号码_index = column_names.index('运单号码')
    计费重量_index = column_names.index('计费重量')
    应付金额_index = column_names.index('应付金额')

    print(f"列名列表: {column_names}")
    rows =con.sql(f"SELECT * FROM '{table_name}'").fetchall()

    t2 = time.time()
    print(f"加载数据库 {db_name} 中表 sftable，耗时{t2-t1:.2f}s")
    总单量 = 0
    总计费重量 = 0
    总应付金额 = 0
    for num, row in enumerate(rows):
        if '合 计' in row:
            continue

        if row[运单号码_index]:
            总单量 += 1
            总计费重量 += float(row[计费重量_index])
            总应付金额 += float(row[应付金额_index])
    print(f'总单量: {总单量}', f'总计费重量: {总计费重量:.2f}', f'总应付金额: {总应付金额:.2f}') ,
    ws.append([db_name.split('.')[0], 总单量, 总计费重量, 总应付金额])

os.makedirs('透视结果', exist_ok=True)
wb.save('透视结果/透视汇总.xlsx')
# 方法 2 (更推荐): 使用 fetchone() 逐行获取 (内存友好)
# result = con.sql("SELECT * FROM excel_data")
# num = 0
# while True:
#     row = result.fetchone()
#     if not row:
#         break
#     # print(num, row)
#     print(num)
#     num += 1
