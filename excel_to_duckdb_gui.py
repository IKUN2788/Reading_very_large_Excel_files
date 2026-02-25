import sys
import os
import time
import duckdb
import pandas as pd
import numpy as np
import python_calamine
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, 
                             QHBoxLayout, QPushButton, QListWidget, QLabel, 
                             QProgressBar, QTextEdit, QFileDialog, QMessageBox)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QObject

# 核心处理逻辑类
class ExcelProcessor(QObject):
    progress_signal = pyqtSignal(int)
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal()

    def __init__(self, file_paths):
        super().__init__()
        self.file_paths = file_paths
        self.is_running = True

    def run(self):
        total_files = len(self.file_paths)
        if total_files == 0:
            self.log_signal.emit("没有需要处理的文件。")
            self.finished_signal.emit()
            return

        for index, file_path in enumerate(self.file_paths):
            if not self.is_running:
                break
            
            try:
                self.process_file(file_path)
            except Exception as e:
                self.log_signal.emit(f"处理文件 {file_path} 时发生错误: {str(e)}")
            
            # 更新进度
            progress = int((index + 1) / total_files * 100)
            self.progress_signal.emit(progress)
        
        self.log_signal.emit("所有任务处理完成！")
        self.finished_signal.emit()

    def process_file(self, excel_path):
        filename = os.path.basename(excel_path)
        base_name = os.path.splitext(filename)[0]
        
        # 数据库保存在同级目录下的 duckdb_output 文件夹中
        output_dir = os.path.join(os.path.dirname(excel_path), "duckdb_output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        db_path = os.path.join(output_dir, f"{base_name}.duckdb")
        table_name = "excel_data" # 或者使用文件名 base_name

        self.log_signal.emit(f"正在读取: {filename}")
        t_start = time.time()

        try:
            # 使用 calamine 读取 Excel
            with open(excel_path, 'rb') as f_r:
                xls = python_calamine.CalamineWorkbook.from_filelike(f_r)
                if xls.sheet_names:
                    # 默认读取第一个 sheet
                    sheet_data = xls.get_sheet_by_index(0).to_python()
                else:
                    self.log_signal.emit(f"警告: {filename} 没有工作表")
                    return

            if not sheet_data:
                self.log_signal.emit(f"警告: {filename} 内容为空")
                return

            # 数据处理
            headers = [str(h) for h in sheet_data[0]] 
            rows = sheet_data[1:]

            pd.set_option('future.no_silent_downcasting', True)
            df = pd.DataFrame(rows, columns=headers)
            # 将空字符串替换为 NaN
            df = df.replace('', np.nan).infer_objects(copy=False)

            # 写入 DuckDB
            con = duckdb.connect(db_path)
            con.register('df_view', df)
            
            # 创建表，表名使用文件名（清理非法字符）
            safe_table_name = "".join([c if c.isalnum() else "_" for c in base_name])
            if safe_table_name[0].isdigit():
                safe_table_name = "t_" + safe_table_name
                
            con.execute(f"CREATE OR REPLACE TABLE {safe_table_name} AS SELECT * FROM df_view")
            con.close()
            
            t_end = time.time()
            self.log_signal.emit(f"成功: 已保存至 {db_path} (表名: {safe_table_name})，耗时 {t_end - t_start:.2f} 秒")

        except Exception as e:
            raise e

# 拖拽列表组件
class DragDropListWidget(QListWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setSelectionMode(QListWidget.ExtendedSelection)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.accept()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.accept()
        else:
            event.ignore()

    def dropEvent(self, event):
        files = []
        for url in event.mimeData().urls():
            path = url.toLocalFile()
            if os.path.isfile(path):
                if path.lower().endswith(('.xlsx', '.xls')):
                    files.append(path)
            elif os.path.isdir(path):
                for root, dirs, filenames in os.walk(path):
                    for filename in filenames:
                        if filename.lower().endswith(('.xlsx', '.xls')):
                            files.append(os.path.join(root, filename))
        
        self.add_files(files)

    def add_files(self, files):
        existing_items = [self.item(i).text() for i in range(self.count())]
        for file_path in files:
            if file_path not in existing_items:
                self.addItem(file_path)

# 主窗口
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Excel 转 DuckDB 工具")
        self.resize(800, 600)
        
        # 主布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 说明标签
        layout.addWidget(QLabel("请拖拽 Excel 文件 (.xlsx, .xls) 或包含 Excel 的文件夹到下方列表："))

        # 文件列表
        self.file_list = DragDropListWidget()
        layout.addWidget(self.file_list)

        # 按钮区域
        btn_layout = QHBoxLayout()
        
        self.btn_add_files = QPushButton("添加文件")
        self.btn_add_files.clicked.connect(self.add_files_dialog)
        btn_layout.addWidget(self.btn_add_files)

        self.btn_add_dir = QPushButton("添加文件夹")
        self.btn_add_dir.clicked.connect(self.add_dir_dialog)
        btn_layout.addWidget(self.btn_add_dir)

        self.btn_clear = QPushButton("清空列表")
        self.btn_clear.clicked.connect(self.file_list.clear)
        btn_layout.addWidget(self.btn_clear)

        self.btn_start = QPushButton("开始转换")
        self.btn_start.clicked.connect(self.start_processing)
        btn_layout.addWidget(self.btn_start)
        
        layout.addLayout(btn_layout)

        # 进度条
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)

        # 日志输出
        layout.addWidget(QLabel("执行日志："))
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        layout.addWidget(self.log_text)

        # 线程相关
        self.thread = None
        self.worker = None

    def add_files_dialog(self):
        files, _ = QFileDialog.getOpenFileNames(
            self, "选择 Excel 文件", "", "Excel Files (*.xlsx *.xls)")
        if files:
            self.file_list.add_files(files)

    def add_dir_dialog(self):
        directory = QFileDialog.getExistingDirectory(self, "选择文件夹")
        if directory:
            files = []
            for root, dirs, filenames in os.walk(directory):
                for filename in filenames:
                    if filename.lower().endswith(('.xlsx', '.xls')):
                        files.append(os.path.join(root, filename))
            self.file_list.add_files(files)

    def start_processing(self):
        count = self.file_list.count()
        if count == 0:
            QMessageBox.warning(self, "提示", "请先添加文件！")
            return

        file_paths = [self.file_list.item(i).text() for i in range(count)]
        
        self.btn_start.setEnabled(False)
        self.btn_clear.setEnabled(False)
        self.btn_add_files.setEnabled(False)
        self.btn_add_dir.setEnabled(False)
        self.file_list.setEnabled(False)
        self.progress_bar.setValue(0)
        self.log_text.clear()
        self.log("开始处理...")

        # 创建线程和工作对象
        self.thread = QThread()
        self.worker = ExcelProcessor(file_paths)
        self.worker.moveToThread(self.thread)

        # 连接信号
        self.thread.started.connect(self.worker.run)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.log_signal.connect(self.log)
        self.worker.finished_signal.connect(self.thread.quit)
        self.worker.finished_signal.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self.processing_finished)

        self.thread.start()

    def update_progress(self, value):
        self.progress_bar.setValue(value)

    def log(self, message):
        self.log_text.append(message)
        # 滚动到底部
        self.log_text.verticalScrollBar().setValue(self.log_text.verticalScrollBar().maximum())

    def processing_finished(self):
        self.btn_start.setEnabled(True)
        self.btn_clear.setEnabled(True)
        self.btn_add_files.setEnabled(True)
        self.btn_add_dir.setEnabled(True)
        self.file_list.setEnabled(True)
        QMessageBox.information(self, "完成", "处理完成！")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())
