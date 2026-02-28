import python_calamine
import duckdb
import os
import time
import json

# JSON文件路径
json_file = 'json数据/file.json'

# 读取JSON文件获取月结卡号信息
try:
    with open(json_file, 'r', encoding='utf-8') as f:
        month_id_dict = json.load(f)
    print(f"成功读取JSON文件，包含 {len(month_id_dict)} 个月结卡号")
    print(f"JSON中的月结卡号: {list(month_id_dict.keys())}")
except FileNotFoundError:
    print(f"错误: 找不到JSON文件 {json_file}")
    exit()
except json.JSONDecodeError:
    print(f"错误: JSON文件格式不正确")
    exit()

db_dir = "duckdb_output"
tablie_name = "sftable"  # 统一表名为sftable

# 确保输出目录存在
if not os.path.exists(db_dir):
    os.makedirs(db_dir)
    print(f"已创建目录: {db_dir}")

# 获取系统账单目录
system_bill_dir = './系统账单/'

# 按月份卡号处理文件
total_processed = 0
for month_id, files in month_id_dict.items():
    print(f"\n{'=' * 50}")
    print(f"处理月结卡号: {month_id}")
    print(f"{'=' * 50}")

    # 为该月结卡号创建单独的数据库文件
    db_path = os.path.join(db_dir, f"{month_id}.duckdb")

    # 收集当前月结卡号的所有Excel文件
    excel_files = []
    for file_name in files:
        # 检查是否为Excel文件
        if file_name.lower().endswith(('.xlsx', '.xls')):
            file_path = os.path.join(system_bill_dir, file_name)
            if os.path.exists(file_path):
                excel_files.append(file_path)
                print(f"找到Excel文件: {file_name}")
            else:
                print(f"警告: 文件不存在 {file_path}")

    if not excel_files:
        print(f"月结卡号 {month_id} 没有找到任何Excel文件，跳过")
        continue

    print(f"月结卡号 {month_id} 共有 {len(excel_files)} 个Excel文件")

    try:
        # 连接到该月结卡号对应的数据库文件
        con = duckdb.connect(db_path)

        all_data = []
        headers = None

        # 遍历处理当前月结卡号的所有Excel文件
        for excel_file in excel_files:
            print(f"  正在处理文件: {os.path.basename(excel_file)}")

            # 使用 calamine 读取 Excel
            with open(excel_file, 'rb') as f_r:
                xls = python_calamine.CalamineWorkbook.from_filelike(f_r)
                # 获取第一个工作表
                sheet_data = xls.get_sheet_by_index(0).to_python()

            if not sheet_data:
                print(f"  警告: Excel文件为空！")
                continue

            # 获取列名 (第一行是表头)
            current_headers = [str(h) for h in sheet_data[0]]

            # 检查表头是否一致
            if headers is None:
                headers = current_headers
            elif headers != current_headers:
                print(f"  警告: 文件表头与其他文件不一致")
                print(f"    期望表头: {headers}")
                print(f"    实际表头: {current_headers}")
                # 可以选择跳过或尝试适配，这里选择跳过
                continue

            # 获取数据行
            rows = sheet_data[1:]
            all_data.extend(rows)

            print(f"  从该文件读取了 {len(rows)} 行数据")

        if not all_data:
            print(f"月结卡号 {month_id} 没有读取到任何数据，跳过")
            con.close()
            continue

        print(f"\n月结卡号 {month_id} 总共读取了 {len(all_data)} 行数据")
        print(f"表头: {headers}")

        t1 = time.time()

        # 将数据写入 DuckDB 文件
        try:
            import pandas as pd
            import numpy as np

            # 消除 FutureWarning
            pd.set_option('future.no_silent_downcasting', True)

            df = pd.DataFrame(all_data, columns=headers)
            
            # 解决 "Type DOUBLE does not match with TIMESTAMP" 等类型不匹配问题
            # 将所有数据强制转换为字符串，并处理空值
            # 这样 DuckDB 会将所有列作为 VARCHAR 导入，保证数据完整性
            print("  正在统一数据类型为字符串，以避免DuckDB类型冲突...")
            for col in df.columns:
                # 先转为字符串
                df[col] = df[col].astype(str)
                # 清理 pandas 转换产生的 'nan', 'None' 字符串以及空字符串
                df[col] = df[col].replace({'nan': None, 'None': None, 'NaT': None, '': None})

            # 将 DataFrame 写入 DuckDB 表 'sftable'
            con.execute(f"CREATE OR REPLACE TABLE {tablie_name} AS SELECT * FROM df")
            print(f"表 '{tablie_name}' 已通过 Pandas 桥接创建成功。")

        except ImportError:
            # 如果没有 Pandas，回退到原生 SQL 插入
            print("未找到 Pandas。正在使用原生 SQL (所有列均视为 VARCHAR)...")

            # 创建表，所有列都设为 VARCHAR
            cols_def = ", ".join([f'"{h}" VARCHAR' for h in headers])
            create_sql = f"CREATE OR REPLACE TABLE {tablie_name} ({cols_def})"
            con.execute(create_sql)

            # 插入数据
            placeholders = ', '.join(['?'] * len(headers))
            insert_sql = f"INSERT INTO {tablie_name} VALUES ({placeholders})"

            # 转换数据为字符串，避免类型转换错误
            safe_rows = [[str(cell) if cell is not None else None for cell in row] for row in all_data]
            con.executemany(insert_sql, safe_rows)

        t2 = time.time()
        print(f"数据导入耗时 {t2 - t1:.2f} 秒。")

        # 验证数据
        result = con.execute(f"SELECT COUNT(*) FROM {tablie_name}").fetchone()
        print(f"表 '{tablie_name}' 共有 {result[0]} 行数据")

        # 关闭连接
        con.close()

        print(f"成功保存: {os.path.abspath(db_path)}")
        total_processed += 1

    except Exception as e:
        print(f"处理月结卡号 {month_id} 时出错: {e}")
        continue

print(f"\n{'=' * 50}")
print(f"处理完成！")
print(f"总共处理了 {total_processed} 个月结卡号的数据库文件")
print(f"数据库文件保存在: {os.path.abspath(db_dir)}")

# 列出所有生成的数据库文件
print(f"\n生成的数据库文件:")
for file in os.listdir(db_dir):
    if file.endswith('.duckdb'):
        file_path = os.path.join(db_dir, file)
        size = os.path.getsize(file_path) / (1024 * 1024)  # 转换为MB
        print(f"  {file} ({size:.2f} MB)")