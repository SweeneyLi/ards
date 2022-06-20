import os

import pandas as pd

output_path = './output'
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
    ards_data_with_static_feature_path = 'dataset/ards_data/valid_ards_data_with_static_feature.csv'
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
    # save data
    ards_data_with_static_feature = ards_data_with_static_feature.loc[:,
                                    ['ards_group', 'pf_8h_min', 'hospital_dead_status', '28d_death_status',
                                     'icu_death_status', 'age', 'apachescore', 'hospital_los',
                                     'icu_los', 'admission_diagnosis']
                                    ]
    ards_data_with_static_feature.to_csv(output_data_path, index=False)


def get_fig4_data():
    from utils.init_config import diagnoses_dict, dynamic_feature_name_list
    column_list = ['gender', 'age', 'admission_diagnosis', 'BMI', 'apacescore'] + dynamic_feature_name_list
    output_data_path = os.path.join(output_path, 'fig4_data.csv')

    # get data
    ards_data_with_static_feature_path = 'dataset/ards_data/valid_ards_data_with_static_feature.csv'
    # ards_data_with_dynamic_feature_path = 'dataset/valid_ards_data_with_dynamic_feature.csv'
    ards_data_with_dynamic_feature_path = 'output/ards_data_dynamic/ards_data_100_to_150_dynamic_0.csv'
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
    ards_data['age'] = ards_data['age'].replace('>89', '90')
    ards_data['age'] = ards_data['age'].astype('int')

    ards_data = ards_data.loc[: column_list]

    ards_data['ards_group'] = ards_data['ards_group'].replace('rapid death', 0)
    ards_data['ards_group'] = ards_data['ards_group'].replace('long stay', 2)
    ards_data['ards_group'] = ards_data['ards_group'].replace('spontaneous recovery', 3)

    ards_data['gender'] = ards_data['gender'].replace('Female', 0)
    ards_data['gender'] = ards_data['gender'].replace('Male', 1)

    diagnose_list = list(set(diagnoses_dict.values()))
    diagnose_list.append('Other')

    for diagnose in diagnose_list:
        ards_data['diagnose_' + diagnose] = ards_data['admission_diagnosis'].map(lambda x: x == diagnose)
    ards_data.drop(columns=['admission_diagnosis'], inplace=True)
    ards_data.to_csv(output_data_path, index=False)

if __name__ == '__main__':
    get_fig3_data()