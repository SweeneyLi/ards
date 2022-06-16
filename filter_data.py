import pandas as pd

from utils.validator import DataValidator
from utils.postgres_sql import PostgresSqlConnector
from tqdm import tqdm
import datetime
import os
import threading


def get_base_ards_data(mult_thread=True):
    sql_connector = PostgresSqlConnector()
    ards_data_id_list = sql_connector.get_ards_data_icu_stay_id()
    sql_connector.close()

    print('There are %d ids' % len(ards_data_id_list))

    if not mult_thread:
        save_valid_id_and_identification_offset(ards_data_id_list)
    else:
        mult_thread_save_valid_id_and_identification_offset(ards_data_id_list, 16)


def mult_thread_save_valid_id_and_identification_offset(thread_data, thread_number):
    split_data_number = len(thread_data) // thread_number

    thread_data_split_list = [thread_data[i: i + split_data_number] for i in
                              range(0, len(thread_data), split_data_number)]

    # 线程池
    threads = []
    print("程序开始%s" % datetime.datetime.now())

    for thread_index, part_thread_data in enumerate(thread_data_split_list):  # 将需要执行的linux命令列表 放入for循环
        th = threading.Thread(target=save_valid_id_and_identification_offset,
                              args=(part_thread_data, thread_index))  # 调用函数,引入线程参数
        th.start()  # 开始执行
        threads.append(th)

    # 等待线程运行完毕
    for th in threads:
        th.join()  # 循环 join()方法可以让主线程等待所有的线程都执行完毕

    print("程序结束%s" % datetime.datetime.now())


def save_valid_id_and_identification_offset(ards_data_id_list, thread_number=0):
    sql_connector = PostgresSqlConnector()

    valid_id_list = []
    for ards_data_id in tqdm(ards_data_id_list):
        pao2_fio2_peep_info = sql_connector.get_pao2_fio2_peep_info_by_icu_stay_id(ards_data_id)
        identification_offset = DataValidator.get_identification_offset(pao2_fio2_peep_info)
        if identification_offset is None:
            continue
        valid_id_list.append({'icu_stay_id': ards_data_id, 'identification_offset': identification_offset})
    df = pd.DataFrame(valid_id_list, columns=['icu_stay_id', 'identification_offset'])

    df.to_csv(os.path.join('/process_data/dataset/base_data',
                           'valid_id_%d.csv' % thread_number))

    sql_connector.close()


if __name__ == '__main__':
    get_base_ards_data(mult_thread=True)
