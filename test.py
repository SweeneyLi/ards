from utils.postgres_sql import PostgresSqlConnector

if __name__ == '__main__':
    test_icu_stay_id = 914123
    test_identification_start_offset = 722
    test_identification_end_offset = test_identification_start_offset + 24 * 60
    sql = PostgresSqlConnector()
    # print(sql.get_patient_table_feature(test_icu_stay_id))
    # print(sql.get_apachePatientResult_table_feature(test_icu_stay_id))
    # print(sql.get_static_feature(test_icu_stay_id))
    # print(sql.get_indicator_feature(test_icu_stay_id))
    # print(sql.get_lab_feature(test_icu_stay_id, test_identification_start_offset, test_identification_end_offset))
    # print(sql.get_pao2_fio2_in_first_8h_after_ards_identification(test_icu_stay_id, test_identification_offset))
    # print(sql.get_vitalPeriodic_feature(test_icu_stay_id, test_identification_start_offset,test_identification_end_offset))
    # print(sql.get_dynamic_feature(test_icu_stay_id, test_identification_start_offset,test_identification_end_offset))
    # print(sql.get_respiratoryCharting_feature(test_icu_stay_id, test_identification_offset,
    #                                           test_identification_offset + 24 * 60))
    # sql.set_ards_data_valid_tag(351515)
    test = sql.get_nurseCharting_feature(test_icu_stay_id, test_identification_start_offset, test_identification_end_offset)
    test = test[(test['label'] == 'Temperature') | (test['label'] == 'Respiratory Rate')]
    print(test)
    test.to_csv('test.csv')
    print('end')
