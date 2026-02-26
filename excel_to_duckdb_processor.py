import python_calamine
import duckdb
import os
import time
import pandas as pd
import numpy as np

# 配置
EXCEL_FILE = "sample_data.xlsx"
DB_DIR = "duckdb_output"
DB_PATH = os.path.join(DB_DIR, f"{EXCEL_FILE.split('.')[0]}.duckdb")
TABLE_NAME = EXCEL_FILE.split('.')[0]

def save_excel_to_duckdb(excel_path, db_path, table_name):
    """读取 Excel 并保存到 DuckDB"""
    print(f"\n[1/2] 正在读取 {excel_path} 并保存到 {db_path}...")
    
    # 0. 检查文件是否存在
    if not os.path.exists(excel_path):
        print(f"错误: 文件 '{excel_path}' 不存在！")
        return False
    
    # 1. 确保临时目录存在
    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
        print(f"已创建目录: {db_dir}")

    t_start = time.time()
    try:
        # 2. 使用 calamine 读取 Excel
        with open(excel_path, 'rb') as f_r:
            xls = python_calamine.CalamineWorkbook.from_filelike(f_r)
            sheet_data = xls.get_sheet_by_index(0).to_python()
        
        if not sheet_data:
            print("错误: Excel 文件为空！")
            return False

        # 3. 数据处理
        headers = [str(h) for h in sheet_data[0]] 
        rows = sheet_data[1:]

        # 4. 借助 Pandas 处理数据（处理空值等）
        pd.set_option('future.no_silent_downcasting', True)
        df = pd.DataFrame(rows, columns=headers)
        # 将空字符串替换为 NaN，以便 DuckDB 正确识别为 NULL
        df = df.replace('', np.nan).infer_objects(copy=False)

        # 5. 写入 DuckDB
        con = duckdb.connect(db_path)
        # 注册 dataframe 到 duckdb
        con.register('df_view', df)
        
        # 创建表
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df_view")
        con.close()
        
        t_end = time.time()
        print(f"成功: 表 '{table_name}' 已创建。耗时 {t_end - t_start:.2f} 秒。")
        return True

    except Exception as e:
        print(f"错误: {e}")
        return False

def read_from_duckdb(db_path, table_name):
    """从 DuckDB 读取数据并展示"""
    print(f"\n[2/2] 正在从 DuckDB 读取数据...")
    
    if not os.path.exists(db_path):
        print(f"错误: 数据库文件不存在: {db_path}")
        return

    try:
        con = duckdb.connect(db_path)
        
        # 1. 简单展示前5行
        print(f"--- 表 '{table_name}' 前 5 行预览 ---")
        con.sql(f"SELECT * FROM {table_name} LIMIT 5").show()
        
        # 2. 遍历数据的示例 (前 10 行)
        print(f"--- 遍历前 10 行数据 ---")
        rows = con.sql(f"SELECT * FROM {table_name} LIMIT 10").fetchall()
        for num, row in enumerate(rows):
            print(f"行 {num}: {row}")
            
        con.close()
        print("\n读取完成。")

    except Exception as e:
        print(f"读取错误: {e}")

if __name__ == "__main__":
    # 执行保存
    if save_excel_to_duckdb(EXCEL_FILE, DB_PATH, TABLE_NAME):
        # 执行读取
        read_from_duckdb(DB_PATH, TABLE_NAME)
