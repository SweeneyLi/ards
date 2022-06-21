import os
import pandas as pd
import numpy as np

__all__ = ['log_time', 'convert_number', 'is_number', 'combine_csvs']


def convert_number(value):
    if is_number(value):
        return np.float(value)
    return None


def is_number(s):
    try:  # 如果能运行float(s)语句，返回True（字符串s是浮点数）
        float(s)
        return True
    except ValueError:  # ValueError为Python的一种标准异常，表示"传入无效的参数"
        pass  # 如果引发了ValueError这种异常，不做任何事情（pass：不做任何事情，一般用做占位语句）
    try:
        import unicodedata  # 处理ASCii码的包
        unicodedata.numeric(s)  # 把一个表示数字的字符串转换为浮点数返回的函数
        return True
    except (TypeError, ValueError):
        pass
    return False


def log_time(func):
    def wrapper(*args, **kw):
        import time
        start_time = time.time()
        print("\n[%30s] startTime: %s" % (
            (' ' + func.__name__).rjust(30, '>'), time_format(start_time),))
        # print("args: ", args, kw)
        try:
            return func(*args, **kw)
        finally:
            end_time = time.time()
            print("\n[%30s] endTime: %s,  cost: %s\n\n" % (
                (' ' + func.__name__).rjust(30, '<'), time_format(end_time), time_format(end_time - start_time)))

    def time_format(seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        h = h % 24
        return '%02d:%02d:%02d' % (h, m, s)

    return wrapper


def combine_csvs(data_path=None, data_name=None):
    files = os.listdir(data_path)
    files = list(filter(lambda x: x.endswith('.csv'), files))
    if len(files) <= 1:
        return

    if data_name is None:
        data_name = '_'.join(files[0].split('_')[:-1]) + '.csv'
    if data_name in files:
        files.remove(data_name)
    print('There are %d files' % len(files))

    df_columns = pd.read_csv(os.path.join(data_path, files[0])).columns
    df = pd.DataFrame(columns=df_columns)

    for file_name in files:
        df2 = pd.read_csv(os.path.join(data_path, file_name))
        df = pd.concat([df, df2])

    print(df)
    print(df.columns)

    df.to_csv(os.path.join(data_path, data_name), index=False)
