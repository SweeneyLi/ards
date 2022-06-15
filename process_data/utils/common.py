__all__ = ['log_time']

import os
import pandas as pd


def log_time(func):
    def wrapper(*args, **kw):
        import time
        start_time = time.time()
        print("\n[%30s] startTime: %s" % (
            (' ' + func.__name__).rjust(30, '>'), time_format(start_time),))
        print("args: ", args, kw)
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
    if len(files) <= 1:
        return

    if data_name is None:
        data_name = '_'.join(files[0].split('_')[:-1]) + '.csv'
    if data_name in files:
        files.remove(data_name)
    print('There are %d files' % len(files))

    df_columns = pd.read_csv(os.path.join(data_path, files[0]), index_col=0).columns
    df = pd.DataFrame(columns=df_columns)

    for file_name in files:
        df2 = pd.read_csv(os.path.join(data_path, file_name), index_col=0)
        df = pd.concat([df, df2])

    print(df)
    print(df.columns)

    df.to_csv(os.path.join(data_path, data_name))


if __name__ == '__main__':
    combine_csvs('/process_data/dataset/ards_data')
