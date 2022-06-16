import pandas as pd

from utils.postgres_sql import PostgresSqlConnector
from tqdm import tqdm
import datetime
import os
import threading

base_ards_data_path = './dataset/valid_id_and_identification_offset.csv'

output_path = './output'
output_data_path = './output/ards_data'
assert os.path.exists(output_path) is False
os.mkdir(output_data_path)


def get_ards_data(mult_thread=True):
    base_ards_data = pd.read_csv(base_ards_data_path, index_col=0)

    print('There are %d base data' % len(base_ards_data))

    if not mult_thread:
        save_ards_data(base_ards_data)
    else:
        mult_thread_save_ards_data(base_ards_data, 4)


def mult_thread_save_ards_data(thread_data, thread_number):
    split_data_number = len(thread_data) // (thread_number - 1)

    thread_data_split_list = [thread_data[i: i + split_data_number] for i in
                              range(0, len(thread_data), split_data_number)]

    # 线程池
    threads = []
    print("程序开始%s" % datetime.datetime.now())

    for thread_index, part_thread_data in enumerate(thread_data_split_list):  # 将需要执行的linux命令列表 放入for循环
        th = threading.Thread(target=save_ards_data,
                              args=(part_thread_data, thread_index + 1))  # 调用函数,引入线程参数
        th.start()  # 开始执行
        threads.append(th)

    # 等待线程运行完毕
    for th in threads:
        th.join()  # 循环 join()方法可以让主线程等待所有的线程都执行完毕

    print("程序结束%s" % datetime.datetime.now())


def add_feature_of_ards_data(sql_connector, ards_info, icu_stay_id, identification_offset):
    # extra feature is mortality_28d, pf_8h_min, recovery_offset, ards_group

    def fun_of_28_da_mortality(x):
        offset_28 = 28 * 24 * 60
        if (x['unitdischargestatus'] == 'Expired' and x[
            'unitdischargeoffset'] <= offset_28) or (x['hospitaldischargestatus'] == 'Expired' and x[
            'hospitaldischargeoffset'] <= offset_28):
            return True
        else:
            return False

    # def fun_of_group_label(x):
    #     offset_1 = 24 * 60
    #     # died
    #     if x['unitdischargestatus'] == 'Expired'or x['hospitaldischargestatus'] == 'Expired':
    #        expired_offset = min(x['unitdischargeoffset'], x['hospitaladmitoffset'])
    #        if expired_offset - identification_offset <= offset_1:
    #            return 'Rapid Death'
    #
    #     if x['unitdischargestatus'] == 'Alive' and x['hospitaldischargestatus'] == 'Alive':
    #         alive_offset = max(x['unitdischargeoffset'], x['hospitaladmitoffset'])
    #         if alive_offset - identification_offset >= offset_1:
    #             return ''
    #
    #     if (x['unitdischargestatus'] == 'Expired' and x[
    #         'unitdischargeoffset'] <= offset_1) or (x['hospitaldischargestatus'] == 'Expired' and x[
    #         'hospitaladmitoffset'] <= offset_1):
    #         return
    #     else:
    #         return False

    ards_info['icu_stay_id'] = icu_stay_id
    ards_info['identification_offset'] = identification_offset

    ards_info['mortality_28d'] = ards_info.apply(fun_of_28_da_mortality, axis=1)

    # ards_info['group_label'] = ards_info.apply(fun_of_group_label, axis=1)
    return ards_info


def save_ards_data(base_ards_data, thread_number=0):
    sql_connector = PostgresSqlConnector()

    # prepare dataframe
    ards_data_column = ['icu_stay_id', 'identification_offset']
    ards_data_column.extend(['mortality_28d', 'ards_group'])
    ards_data_column.extend(sql_connector.get_feature(base_ards_data.iloc[0]['icu_stay_id']).columns)
    ards_data = pd.DataFrame(columns=ards_data_column)

    for index, row in tqdm(base_ards_data.iterrows(), total=base_ards_data.shape[0]):
        icu_stay_id = row['icu_stay_id']
        identification_offset = row['identification_offset']
        # get origin feature
        ards_info = sql_connector.get_feature(icu_stay_id)

        # add extra feature
        ards_info = add_feature_of_ards_data(sql_connector, ards_info, icu_stay_id, identification_offset)

        # combine
        ards_data = pd.concat([ards_data, ards_info], axis=0)

    # print(ards_data.columns)
    # print(ards_data.iloc[:1].to_json())
    # print(ards_data)
    ards_data.to_csv(os.path.join('/process_data/dataset/ards_data',
                                  'ards_data_%d.csv' % thread_number))
    sql_connector.close()


if __name__ == '__main__':
    get_ards_data(mult_thread=True)
