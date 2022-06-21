import os
import numpy as np
import pandas as pd
from tqdm import tqdm
from utils.common import change_dataframe_bool_and_round

output_path = './output'
indicator_feature_list = [
    'vasopressor_indicator',
    'dobutamine_indicator',
    'dopamine_indicator',
    'epinephrine_indicator',
    'norepinephrine_indicator',
    'phenylephrine_indicator',
    'vasopressin_indicator',
    'warfarin_indicator',
    'heparin_indicator',
    'milrinone_indicator']

admission_diagnosis_list = [
    'Sepsis',
    'Respiratory (Medical/Other)',
    'Pneumonia',
    'Coronary Artery Bypass Graft',
    'Trauma',
    'Asthma or Emphysema',
    'Gastrointestinal Bleed',
    'Cerebrovascular Accident/Stroke',
    'Valve Disease',
    'Overdose',
    'Acute Myocardial Infarction',
    'Coma',
    'Cardiovascular (Other)',
    'Neurologic',
    'Gastrointestinal Obstruction',
    'Arrhythmia',
    'Cancer'
]


def change_admission_diagnosis(diagnosis):
    if diagnosis in admission_diagnosis_list:
        return diagnosis
    return 'Other'


def get_fig3_data():
    output_data_path = os.path.join(output_path, 'fig3_data.csv')
    # get static feature
    ards_data_with_static_feature_path = 'dataset/ards_data/valid_ards_data_with_static_feature_9399.csv'
    ards_data_with_static_feature = pd.read_csv(ards_data_with_static_feature_path)
    # reformat feature
    ards_data_with_static_feature['admission_diagnosis'] = ards_data_with_static_feature['admission_diagnosis'].map(
        change_admission_diagnosis)
    ards_data_with_static_feature['icu_death_status'] = ards_data_with_static_feature['unitdischargestatus'].map(
        lambda x: x == 'Expired')
    ards_data_with_static_feature['hospital_dead_status'] = ards_data_with_static_feature[
        'hospitaldischargestatus'].map(
        lambda x: x == 'Expired')

    ards_data_with_static_feature['icu_los'] = ards_data_with_static_feature['unitdischargeoffset'].map(
        lambda x: x / 1440)
    ards_data_with_static_feature['hospital_los'] = ards_data_with_static_feature.apply(
        lambda x: (x['hospitaldischargeoffset'] - x['hospitaladmitoffset']) / 1440, axis=1)

    ards_data_with_static_feature['ards_severity'] = ards_data_with_static_feature['pf_8h_min'].map(
        lambda x: {0: 'Severe', 100: 'Moderate', 200: 'Mild'}.get(np.floor(min(x, 299))) if x else None)

    ards_data_with_static_feature['age'] = ards_data_with_static_feature['age'].replace('> 89', '90')
    ards_data_with_static_feature['age'] = ards_data_with_static_feature['age'].astype('int', errors='ignore')

    ards_data_with_static_feature = change_dataframe_bool_and_round(ards_data_with_static_feature)
    # save data
    ards_data_with_static_feature = ards_data_with_static_feature.loc[:,
                                    ['icu_stay_id', 'ards_group', 'ards_severity', 'hospital_dead_status',
                                     '28d_death_status',
                                     'icu_death_status', 'age', 'apachescore', 'hospital_los',
                                     'icu_los', 'admission_diagnosis']
                                    ]
    ards_data_with_static_feature.to_csv(output_data_path, index=False)


def get_fig4_data():
    from utils.init_config import diagnoses_dict, dynamic_feature_name_list
    column_list = ['ards_group'] + ['gender', 'age', 'admission_diagnosis', 'BMI',
                                    'apachescore'] + indicator_feature_list + dynamic_feature_name_list
    output_data_path = os.path.join(output_path, 'fig4_data.csv')

    # get data
    ards_data_with_static_feature_path = 'dataset/ards_data/valid_ards_data_with_static_feature_9399.csv'
    ards_data_with_dynamic_feature_path = 'dataset/ards_data/valid_ards_data_with_dynamic_feature_9399.csv'

    ards_data_with_static_feature = pd.read_csv(ards_data_with_static_feature_path)
    print('static', ards_data_with_static_feature.shape)
    ards_data_with_dynamic_feature = pd.read_csv(ards_data_with_dynamic_feature_path)
    print('dynamic shape', ards_data_with_dynamic_feature.shape)
    ards_data_with_dynamic_feature.drop(columns=['identification_offset'], inplace=True)
    ards_data = pd.merge(ards_data_with_static_feature, ards_data_with_dynamic_feature,
                         on='icu_stay_id')
    print('combine shape', ards_data.shape)

    # reformat
    ards_data['BMI'] = ards_data.apply(
        lambda x: x['admissionweight'] / (x['admissionheight'] * x['admissionheight'] * 0.01) if x[
            'admissionheight'] else None, axis=1)
    ards_data['age'].replace('> 89', '90', inplace=True)
    ards_data['age'] = ards_data['age'].astype('int', errors='ignore')

    ards_data = ards_data.loc[:, column_list]

    # change diagnose
    diagnose_list = list(set(diagnoses_dict.values()))
    diagnose_list.append('Other')
    for diagnose in diagnose_list:
        ards_data['diagnose_' + diagnose] = ards_data['admission_diagnosis'].map(lambda x: 1 if x == diagnose else 0)
    ards_data.drop(columns=['admission_diagnosis'], inplace=True)

    ards_data = change_dataframe_bool_and_round(ards_data)

    print('final shape', ards_data.shape)
    ards_data.to_csv(output_data_path, index=False)


def get_lab_feature(lab_feature_list=None):
    if lab_feature_list is None:
        lab_feature_list = ['pH', 'lactate', 'bicarbonate']
    fig5_lab_data = 'dataset/figure_data/fig5_lab_data.csv'
    if os.path.exists(fig5_lab_data):
        return pd.read_csv(fig5_lab_data)

    global output_path
    output_lab_data_path = os.path.join(output_path, 'fig5_lab_data.csv')

    from utils.postgres_sql import PostgresSqlConnector
    sql_connector = PostgresSqlConnector()
    offset_24h = 24 * 60
    ards_data_static_feature_path = 'dataset/ards_data/valid_ards_data_with_static_feature.csv'
    ards_data_static_feature = pd.read_csv(ards_data_static_feature_path)

    lab_data = pd.DataFrame(columns=['icu_stay_id', 'ards_group', 'time_offset', 'label', 'value'])
    for index, row in tqdm(ards_data_static_feature.iterrows(), total=ards_data_static_feature.shape[0]):
        icu_stay_id = row['icu_stay_id']
        identification_offset = row['identification_offset']
        ards_group = row['ards_group']

        temp = sql_connector.get_special_lab_feature(icu_stay_id, identification_offset,
                                                     identification_offset + offset_24h, lab_feature_list)

        temp['icu_stay_id'] = icu_stay_id
        temp['ards_group'] = ards_group
        temp['ards_offset'] = temp['time_offset'].map(lambda x: x - identification_offset)
        lab_data = pd.concat([lab_data, temp])
        lab_data.to_csv(output_lab_data_path, index=False)
    return lab_data


def get_fig5_data():
    global output_path
    output_data_path = os.path.join(output_path, 'fig5_data.csv')
    lab_feature_list = ['pH', 'lactate', 'bicarbonate']

    # get lab data
    lab_data = get_lab_feature(lab_feature_list)

    # reformat data
    lab_data['offset_15m'] = lab_data.apply(lambda x: int((x['time_offset'] - x['identification_offset']) / 15) / 4,
                                            axis=1)
    lab_data = lab_data.loc[:, ['ards_group', 'label', 'offset_15m', 'value']]

    # group data
    lab_group_data = lab_data.groupby(['label', 'ards_group', 'offset_15m']).median('value')
    lab_group_data.to_csv(output_data_path)


if __name__ == '__main__':
    # get_fig3_data()
    get_fig4_data()
    # get_fig5_data()
    print('End')
