import python_calamine
import time

file_path = "sample_data.xlsx"
# 推荐使用二进制模式读取
with open(file_path, 'rb') as f_r:
    xls = python_calamine.CalamineWorkbook.from_filelike(f_r)
    # 方法一：按名称获取工作表
    # sheet = xls.get_sheet_by_name('Sheet1').to_python()
    t1 = time.time()
    # 方法二：按索引获取工作表
    sheet = xls.get_sheet_by_index(0).to_python()
    # xls.get_sheet_by_name('Sheet1').to_python()
    # 打印前10行
    print("前10行数据:")
    for row in sheet[:10]:
        print(row)
    t2 = time.time()
    print(f"查询时间: {t2 - t1:.2f} 秒")
