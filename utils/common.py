import os
import pandas as pd
from data_validator import SectionValidator

__all__ = ['log_time', 'combine_csvs', 'generate_pf_list', 'get_continuous_offset',
           'reformat_data_from_dataframe_to_dict']


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


def reformat_data_from_dataframe_to_dict(df):
    result = {}
    # change to dict
    for index, row in df.iterrows():
        name = row['label']
        value = row['value']
        offset = row['offset']

        if result.get(name, None) is None:
            result[name] = {}
        result[name][offset] = value

    return result


def generate_pf_list(pao2_list, fio2_list):
    pf_list = []
    if len(pao2_list) < 2 or len(fio2_list) < 2:
        return pf_list

    time_list = sorted(list(map(lambda x: x[0], pao2_list)) + list(map(lambda x: x[0], fio2_list)))
    start_time = max(pao2_list[0][0], fio2_list[0][0])

    len_pao2 = len(pao2_list)
    len_fio2 = len(fio2_list)
    p_index = 0
    f_index = 0

    for time_value in time_list:
        if time_value < start_time:
            continue
        while p_index < len_pao2 and pao2_list[p_index][0] <= time_value:
            p_index += 1
        while f_index < len_fio2 and fio2_list[f_index][0] <= time_value:
            f_index += 1
        last_p_v = pao2_list[p_index - 1][1]
        last_f_v = fio2_list[f_index - 1][1]
        pf_list.append([time_value, last_p_v / last_f_v])
        # print('%d: %f / %f = %f' % (time_value, last_p_v, last_f_v, last_p_v / last_f_v))

    return pf_list


assert generate_pf_list(
    [[0, 300], [1, 300], [10, 200], [15, 100]],
    [[5, 10], [20, 5]]
) == [[5, 30.0], [10, 20.0], [15, 10.0], [20, 20.0]]


def get_continuous_offset(continue_offset, section_type, points):
    section_type = section_type + ' filter'
    if len(points) < 2:
        return None
    len_points = len(points)

    start_index = -1
    for i in range(len_points):
        if SectionValidator.is_valid(section_type, points[i][1]):
            start_index = i
            break

    if start_index == -1:
        return None

    end_index = start_index

    while end_index < len_points:
        if SectionValidator.is_valid(section_type, points[end_index][1]):
            if points[end_index][0] - points[start_index][0] >= continue_offset:
                return points[end_index][0]

            end_index += 1
        else:
            while end_index < len_points and not SectionValidator.is_valid(section_type, points[end_index][1]):
                end_index += 1
            start_index = end_index

    return None


assert get_continuous_offset(8, 'PEEP', [
    [-1, 5], [10, 3], [11, 2], [19, 5]
]) is None
assert get_continuous_offset(8, 'PEEP', [
    [-1, 2], [10, 8], [11, 8], [19, 10], [20, 3]
]) == 19

if __name__ == '__main__':
    combine_csvs('../output/')
