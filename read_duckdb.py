import duckdb
con = duckdb.connect('temp_db/excel_data.duckdb')
# con.sql("SELECT * FROM excel_data LIMIT 100").show()
# 方法 1: 使用 fetchall() 一次性获取所有行 (适合小数据量)
rows = con.sql("SELECT * FROM excel_data").fetchall()
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