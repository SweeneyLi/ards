import pandas as pd

from data_process.common_utils import is_number
from data_process.data_validator import SectionValidator

__all__ = [
    'reformat_data_from_dataframe_to_dict_and_remove_outlier', 'generate_pf_list', 'get_continuous_offset',
    'reformat_feature_from_column_to_line', 'change_dataframe_bool_and_round'
]


def reformat_data_from_dataframe_to_dict_and_remove_outlier(df):
    result = {}
    # change to dict
    for index, row in df.iterrows():
        name = row['label']
        value = row['value']
        offset = row['time_offset']

        if not is_number(value):
            print('not a number! -> name: %s, value:%s, offset:%s' % (name, value, offset))
            continue
        value = float(value)

        if result.get(name, None) is None:
            result[name] = {}
        if SectionValidator.is_valid(name, value):
            result[name][offset] = value

    return result


def generate_pf_list(pao2_dict, fio2_dict):
    pao2_list = sorted(pao2_dict.items(), key=lambda x: x[0])
    fio2_list = sorted(fio2_dict.items(), key=lambda x: x[0])

    pf_list = []
    if len(pao2_list) < 1 or len(fio2_list) < 1:
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
        # The unit of fio2 is percentage.
        pf_list.append([time_value, last_p_v / (last_f_v / 100)])
        # print('%d: %f / %f = %f' % (time_value, last_p_v, last_f_v, last_p_v / last_f_v))

    return pf_list


assert generate_pf_list(
    {0: 300, 1: 300, 10: 200, 15: 100},
    {5: 100, 20: 50}
    # [[0, 300], [1, 300], [10, 200], [15, 100]],
    # [[5, 10], [20, 5]]
) == [[5, 300], [10, 200], [15, 100], [20, 200]]


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


def reformat_feature_from_column_to_line(data):
    label_list = list(data.columns).copy()
    label_list.remove('time_offset')

    new_data = pd.DataFrame(columns=['time_offset', 'label', 'value'])
    for index, row in data.iterrows():
        offset = row['time_offset']
        for l in label_list:
            if row[l]:
                new_data = new_data.append({'time_offset': offset, 'label': l, 'value': row[l]}, ignore_index=True)
    return new_data


def change_dataframe_bool_and_round(df, round_number=2):
    df = df.round(round_number)

    df.replace('TRUE', 1, inplace=True)
    df.replace('FALSE', 0, inplace=True)
    df.replace('True', 1, inplace=True)
    df.replace('False', 0, inplace=True)

    for col in df.columns:
        if df[col].dtype == bool:
            df[col] = df[col].astype('int')

    return df
