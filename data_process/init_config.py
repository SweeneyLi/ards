import pandas as pd
import numpy as np
import yaml
import os

__all__ = [
    'read_config', 'db_config', 'diagnoses_dict', 'dynamic_range_dict', 'dynamic_feature_list'
]
db_config_path = 'config/db_config.yaml'
data_config_path = 'config/data_config.xlsx'

db_config = {}
diagnoses_dict = {}
dynamic_range_dict = {}
dynamic_feature_list = []
dynamic_feature_name_list = []


def read_config():
    if db_config == {}:
        read_db_config()
        read_data_config()
        print('The config was inited.')


def read_db_config():
    global db_config
    assert os.path.isfile(db_config_path) is True

    with open(db_config_path, 'r') as f:
        db_config = yaml.load(f.read(), Loader=yaml.Loader)['database']


def read_data_config():
    global diagnoses_dict
    global dynamic_range_dict
    global dynamic_feature_list
    global dynamic_feature_name_list

    assert os.path.isfile(data_config_path) is True

    # diagnose dict
    data = pd.read_excel(data_config_path, sheet_name='diagnosis groupings')
    data['Group'].fillna(method='ffill', inplace=True)
    for index, row in data.iterrows():
        diagnoses_dict[row['Diagnoses']] = row['Group']

    # dynamic feature
    data = pd.read_excel(data_config_path, sheet_name='dynamic features range')
    dynamic_feature_list = data['Name'].tolist()

    for feature_name in dynamic_feature_list:
        dynamic_feature_name_list.extend([
            feature_name + '_median',
            feature_name + '_variance',
            feature_name + '_rate_change'
        ])

    # dynamic feature range
    for index, row in data.iterrows():
        dynamic_range_dict[row['Name']] = row['Limit'] if row['Limit'] is not np.nan else None

    # filter of ards
    dynamic_range_dict['PEEP filter'] = '[5, 25]'
    dynamic_range_dict['P/F ratio filter'] = '(0, 300]'


read_config()
