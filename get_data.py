import os
import pandas as pd
from tqdm import tqdm
import datetime
import threading
from utils.init_config import dynamic_feature_name_list
from utils.postgres_sql import PostgresSqlConnector
from utils.feature_extractor import FeatureExtractor


def get_ards_data(base_ards_data, mult_thread=1):
    print('There are %d records' % len(base_ards_data))

    if mult_thread == 1:
        save_ards_data(base_ards_data)
    else:
        mult_thread_save_ards_data(base_ards_data, mult_thread)


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


def save_ards_data(base_ards_data, thread_number=0):
    global data_name
    global static_feature
    global dynamic_feature

    sql_connector = PostgresSqlConnector()
    offset_24h = 24 * 60

    # prepare dataframe
    ards_data_column = ['icu_stay_id', 'identification_offset']

    if static_feature:
        icu_stay_id_example = base_ards_data.iloc[0]['icu_stay_id']
        ards_data_column.extend(sql_connector.get_static_feature(icu_stay_id_example).columns)
        ards_data_column.extend(['28d_death_status', 'ards_group', 'pf_8h_min', 'admission_diagnosis'])
    if dynamic_feature:
        ards_data_column.extend(dynamic_feature_name_list)

    ards_data = pd.DataFrame(columns=ards_data_column)

    for index, row in tqdm(base_ards_data.iterrows(), total=base_ards_data.shape[0]):
        icu_stay_id = row['icu_stay_id']
        identification_offset = row['identification_offset']
        temp = pd.DataFrame([[icu_stay_id, identification_offset]], columns=['icu_stay_id', 'identification_offset'])

        if static_feature:
            # get static feature
            a_ards_static_feature = sql_connector.get_static_feature(icu_stay_id)
            assert a_ards_static_feature.shape[0] == 1

            a_ards_static_feature['icu_stay_id'] = icu_stay_id
            a_ards_static_feature['identification_offset'] = identification_offset

            # add extra static feature
            a_ards_static_feature = FeatureExtractor.add_static_feature_of_ards_data(sql_connector,
                                                                                     a_ards_static_feature,
                                                                                     icu_stay_id,
                                                                                     identification_offset)
            temp = pd.merge(temp, a_ards_static_feature)
        if dynamic_feature:
            # get dynamic feature
            a_ards_dynamic_feature_dict = sql_connector.get_dynamic_feature(icu_stay_id, identification_offset,
                                                                            identification_offset + offset_24h)
            # reformat  dynamic feature
            a_ards_dynamic_feature = FeatureExtractor.reformat_dynamic_feature_of_ards_data(a_ards_dynamic_feature_dict)

            a_ards_dynamic_feature.loc[0, 'icu_stay_id'] = icu_stay_id

            # print(temp, a_ards_dynamic_feature)
            temp = pd.merge(temp, a_ards_dynamic_feature, on='icu_stay_id')

        ards_data = pd.concat([ards_data, temp])

    if static_feature:
        data_name = data_name + '_static'
    if dynamic_feature:
        data_name = data_name + '_dynamic'

    ards_data.to_csv(os.path.join(output_data_path,
                                  data_name + '_%d.csv' % thread_number), index=False)
    sql_connector.close()


base_ards_data_path = 'dataset/ards_data/valid_ards_data_with_base_info.csv'
output_path = './output'
output_data_path = 'output/ards_data_dynamic'
if os.path.exists(output_data_path) is False:
    os.mkdir(output_data_path)

test_mode = False
static_feature = False
dynamic_feature = True
mult_thread = 1

start_index = 250
end_index = 300

data_name = 'ards_data'
if start_index:
    data_name += '_%d_to_%d' % (start_index, end_index)

if __name__ == '__main__':
    # base_ards_data = base_ards_data.iloc[[7984, 7986], :]
    base_ards_data = pd.read_csv(base_ards_data_path)
    base_ards_data = base_ards_data.iloc[start_index:end_index, :]
    if test_mode:
        base_ards_data = base_ards_data.iloc[:1]

    get_ards_data(base_ards_data=base_ards_data, mult_thread=mult_thread)
    print('End')
