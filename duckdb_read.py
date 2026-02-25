import os
import time
import duckdb

'''
不会。

在你的代码里 con = duckdb.connect() 没有传入文件路径，DuckDB 默认创建的是 内存数据库 ，
不会在当前目录生成类似 *.duckdb 的临时数据库文件；只有你写成 duckdb.connect("xxx.duckdb") 才会落盘生成数据库文件。

补充：运行时可能会有 扩展下载缓存 或在内存不足时产生 临时溢写文件 （由 DuckDB/系统临时目录管理），
但这不是“生成一个数据库文件”到你的项目目录里。

'''
excel_file = "sample_data.xlsx"
duckdb_output = 'duckdb_output/'
if not os.path.exists(duckdb_output):
    os.makedirs(duckdb_output)

def save_excel_to_duckdb(excel_file, db_path):
    """
    将 Excel 文件保存到 DuckDB 数据库中
    :param excel_file: Excel 文件路径
    :param db_path: DuckDB 数据库文件路径
    """
    # 从文件名生成合法的表名（去掉扩展名，确保没有特殊字符）
    table_name = os.path.splitext(os.path.basename(excel_file))[0]
    
    print(f"--- 方法 B: 使用 spatial 扩展直接读取文件并保存到表 '{table_name}' ---")
    
    try:
        t1 = time.time()
        # 连接到数据库文件
        con = duckdb.connect(db_path)

        # 安装并加载 spatial 扩展 (需要联网一次)
        # spatial 扩展包含 GDAL，支持读取 Excel 等多种格式
        print("正在加载 spatial 扩展...")
        con.install_extension("spatial")
        con.load_extension("spatial")

        # 使用 st_read 函数读取并创建表
        print(f"正在直接查询文件: {excel_file}")
        
        # 使用 CREATE OR REPLACE TABLE 语句真正保存数据
        # 注意：这里我们使用 CTAS (Create Table As Select) 语法
        create_query = f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM st_read('{excel_file}')"
        con.sql(create_query)
        
        print(f"数据已成功写入表: {table_name}")
        
        # 显示前5行结果确认
        con.sql(f"SELECT * FROM {table_name} LIMIT 5").show()
        
        print(f'{excel_file} 已保存至数据库文件: {db_path}')
        t2 = time.time()
        print(f"处理时间: {t2 - t1:.2f} 秒")
        
        # 关闭连接
        con.close()
        return table_name
        
    except Exception as e:
        print(f"⚠️ 无法使用 spatial 扩展 (可能是网络问题或环境限制): {e}")
        print("建议使用方法 A (Pandas) 作为替代。")
        return None

# 定义数据库文件路径
db_file_path = os.path.join(duckdb_output, excel_file.replace(".xlsx", ".duckdb"))

# 执行保存操作，并获取生成的表名
saved_table_name = save_excel_to_duckdb(excel_file, db_file_path)
print('表名：',saved_table_name)

def read_from_duckdb(db_path, table_name):
    """从 DuckDB 读取数据并展示"""
    print(f"\n正在从 DuckDB 读取数据...")
    print(f"数据库路径: {db_path}")
    print(f"查询表名: {table_name}")

    if not os.path.exists(db_path):
        print(f"错误：数据库文件不存在 {db_path}")
        return

    con = duckdb.connect(db_path)

    try:
        # 1. 简单展示前5行
        print(f"--- 表 '{table_name}' 前 5 行预览 ---")
        con.sql(f"SELECT * FROM {table_name} LIMIT 5").show()

        # 2. 遍历数据的示例 (前 10 行)
        print(f"--- 遍历前 10 行数据 ---")
        rows = con.sql(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
        for num, row in enumerate(rows):
            print(f"行 {num}: {row}")

        # 方法 2 (更推荐): 使用 fetchone() 逐行获取 (内存友好)
        # result = con.sql(f"SELECT * FROM {table_name})
        # num = 0
        # while True:
        #     row = result.fetchone()
        #     if not row:
        #         break
        #     # print(num, row)
        #     print(num)
        #     num += 1


    except duckdb.CatalogException as e:
        print(f"错误：表 '{table_name}' 不存在或查询出错。详细信息: {e}")
    except Exception as e:
        print(f"发生错误: {e}")
    finally:
        con.close()
        print("读取完成。")

# 如果保存成功，则读取
if saved_table_name:
    read_from_duckdb(db_file_path, saved_table_name)
else:
    print("保存失败，无法读取。")
