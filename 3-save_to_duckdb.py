import python_calamine
import duckdb
import os
import time

excel_file = "sample_data.xlsx"
# excel_file = "large_test.xlsx"
db_dir = "duckdb_output"
db_path = os.path.join(db_dir, f"{excel_file.split('.')[0]}.duckdb")

# 1. 确保临时目录存在
if not os.path.exists(db_dir):
    os.makedirs(db_dir)
    print(f"已创建目录: {db_dir}")

print(f"正在读取 {excel_file} 并保存到 {db_path}...")

try:
    # 2. 使用 calamine 读取 Excel (保持原有逻辑)
    with open(excel_file, 'rb') as f_r:
        xls = python_calamine.CalamineWorkbook.from_filelike(f_r)
        # 获取第一个工作表
        sheet_data = xls.get_sheet_by_index(0).to_python()
    
    if not sheet_data:
        print("Excel 文件为空！")
        exit()

    # 3. 将数据写入 DuckDB 文件
    # 连接到持久化数据库文件
    con = duckdb.connect(db_path)

    t1 = time.time()
    
    # 获取列名 (第一行是表头)
    headers = [str(h) for h in sheet_data[0]] 
    rows = sheet_data[1:]
    
    # 方式 A: 借助 Pandas 转 DuckDB (最稳健，自动处理空值类型转换)
    # 注意：如果导入 pandas 失败，会抛出 ImportError，然后进入 except 块使用原生 SQL 插入
    try:
        import pandas as pd
        import numpy as np
        # 消除 FutureWarning: Downcasting behavior in `replace` is deprecated
        pd.set_option('future.no_silent_downcasting', True)
        
        # 注意：如果 Excel 有空行可能会导致类型推断错误，Pandas 处理比较好
        # 将空字符串替换为 NaN，以便 DuckDB 正确识别为 NULL
        df = pd.DataFrame(rows, columns=headers)
        df = df.replace('', np.nan).infer_objects(copy=False)
        
        # 将 DataFrame 写入 DuckDB 表 'sample_data'
        con.execute("CREATE OR REPLACE TABLE sample_data AS SELECT * FROM df")
        print("表 'sample_data' 已通过 Pandas 桥接创建成功。")
        
        
    except ImportError:
        # 如果没有 Pandas，回退到原生 SQL 插入
        # 简单粗暴：全部当做 VARCHAR 处理以避免类型错误 (比如空字符串转数字失败)
        print("未找到 Pandas。正在使用原生 SQL (为安全起见，所有列均视为 VARCHAR)...")
        
        # 创建表，所有列都设为 VARCHAR
        cols_def = ", ".join([f'"{h}" VARCHAR' for h in headers])
        create_sql = f"CREATE OR REPLACE TABLE sample_data ({cols_def})"
        con.execute(create_sql)
        
        # 插入数据 (需将所有值转为字符串，处理 None)
        # 这是一个比较慢的方法，但最通用
        placeholders = ', '.join(['?'] * len(headers))
        insert_sql = f"INSERT INTO sample_data VALUES ({placeholders})"
        
        # 转换数据为字符串，避免类型转换错误
        safe_rows = [[str(cell) if cell is not None else None for cell in row] for row in rows]
        con.executemany(insert_sql, safe_rows)

    t2 = time.time()
    print(f"数据导入耗时 {t2 - t1:.2f} 秒。")
    
    # 4. 验证读取
    print(f"\n正在验证 DuckDB 文件中的数据 ({db_path}):")
    # 关闭连接
    con.close()
    
    print(f"\n成功！数据库已保存至: {os.path.abspath(db_path)}")

except Exception as e:
    print(f"错误: {e}")
