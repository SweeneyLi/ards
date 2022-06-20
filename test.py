from utils.postgres_sql import PostgresSqlConnector
import pandas as pd

if __name__ == '__main__':
    a = pd.read_csv('output/ards_data_dynamic/ards_data_0_to_100_dynamic.csv')
    print(a)
    # test_icu_stay_id = 914123
    # test_identification_start_offset = 722
    # test_identification_end_offset = test_identification_start_offset + 24 * 60
    # sql = PostgresSqlConnector()
    # print(sql.get_patient_table_feature(test_icu_stay_id))
    # test = sql.get_nurseCharting_feature(test_icu_stay_id, test_identification_start_offset, test_identification_end_offset)
    # test = test[(test['label'] == 'Temperature') | (test['label'] == 'Respiratory Rate')]
    # print(test)
    # test.to_csv('test.csv', index=False)
    # print('end')
