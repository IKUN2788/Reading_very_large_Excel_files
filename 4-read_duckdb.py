import os
import time

import duckdb
for db_name in os.listdir("duckdb_output"):
    con = duckdb.connect(f'duckdb_output/{db_name}')
    # con.sql("SELECT * FROM excel_data LIMIT 100").show()
    # 方法 1: 使用 fetchall() 一次性获取所有行 (适合小数据量)
    table_name = db_name.split('.')[0]
    t1 = time.time()
    rows =con.sql(f"SELECT * FROM {table_name}").fetchall()
    t2 = time.time()
    print(f"加载数据库 {db_name} 中表 {table_name}，耗时{t2-t1:.2f}s")
    for num, row in enumerate(rows):
        print(num,row)

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