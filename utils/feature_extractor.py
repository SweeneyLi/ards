import json
import operator
import pandas as pd
import numpy as np
from utils.data_validator import SectionValidator
from utils.common import *
from utils.init_config import diagnoses_dict, dynamic_feature_name_list
from utils.postgres_sql import PostgresSqlConnector
import operator

__all__ = ['FeatureExtractor']


class FeatureExtractor:
    sql_connector = PostgresSqlConnector()
    valid_type = ['P/F ratio', 'PEEP']
    # continue offset of valid section
    continue_offset = 8 * 60

    @staticmethod
    def get_identification_offset(pao2_fio2_peep_info):
        # assert set(pao2_fio2_peep_info.keys()) == {'paO2', 'FiO2', 'PEEP'}
        if set(pao2_fio2_peep_info.keys()) != {'paO2', 'FiO2', 'PEEP'}:
            print('[get_identification_offset] keys of pao2_fio2_peep_info',
                  pao2_fio2_peep_info.keys())
            return None

        # prepare data
        pao2_fio2_peep_info['P/F ratio'] = generate_pf_list(pao2_fio2_peep_info['paO2'],
                                                            pao2_fio2_peep_info['FiO2'])
        pao2_fio2_peep_info['PEEP'] = sorted(pao2_fio2_peep_info['PEEP'].items(), key=lambda x: x[0])

        identification_offset = -100000
        for section_type in FeatureExtractor.valid_type:
            continuous_offset = get_continuous_offset(FeatureExtractor.continue_offset,
                                                      section_type,
                                                      pao2_fio2_peep_info[section_type])
            if continuous_offset is None:
                return None
            identification_offset = max(identification_offset, continuous_offset)
        return identification_offset

    @staticmethod
    def add_static_feature_of_ards_data(sql_connector, a_ards_info, icu_stay_id, identification_offset):
        # extra feature is admission_diagnosis, 28d_death_status, pf_8h_min, ards_group

        def get_death_28d(x):
            offset_28 = 28 * 24 * 60
            if (x['unitdischargestatus'] == 'Expired' and x[
                'unitdischargeoffset'] <= offset_28) or (
                    x['hospitaldischargestatus'] == 'Expired' and x[
                'hospitaldischargeoffset'] <= offset_28):
                return True
            else:
                return False

        def get_pf_8h_min(x):
            pf_8h_data = sql_connector.get_pao2_fio2_in_first_8h_after_ards_identification(x['icu_stay_id'],
                                                                                           x['identification_offset'])
            pf_8h_info = reformat_data_from_dataframe_to_dict_and_remove_outlier(pf_8h_data)
            # assert set(pf_8h_info.keys()) == {'paO2', 'FiO2'}
            if set(pf_8h_info.keys()) != {'paO2', 'FiO2'}:
                print('[get_pf_8h_min] keys of pf_8h_info', pf_8h_info.keys())
                return None
            pf_8h_list = generate_pf_list(pf_8h_info['paO2'], pf_8h_info['FiO2'])
            if len(pf_8h_list) == 0:
                return None
            return min(list(map(lambda x: x[1], pf_8h_list)))

        def get_ards_group(x):
            """
            All ARDS patients were stratified according to the following outcomes:
            “rapid death” if a patient expired within 24 h after time of enrollment (48 h after ARDS identification),
            “spontaneous recovery” if a patient fully recovered from ARDS within 24 h after time of enrollment,
            and “long stay” if a patient continued to have ARDS for more than 24 h of post- enrollment.
            Only one ICU stay was included for each patient.
            :param x:
            :return:
            """
            offset_24h = 24 * 60

            if x['unitdischargeoffset'] - x['identification_offset'] <= offset_24h:
                if x['unitdischargestatus'] == 'Expired':
                    return 'rapid death'
                else:
                    return 'spontaneous recovery'

            pao2_fio2_peep_data = sql_connector.get_pao2_fio2_peep_info_by_icu_stay_id_and_offset(x['icu_stay_id'], x[
                'identification_offset'] + 16 * 60)
            pao2_fio2_peep_info = reformat_data_from_dataframe_to_dict_and_remove_outlier(pao2_fio2_peep_data)
            if FeatureExtractor.get_identification_offset(pao2_fio2_peep_info):
                return 'long stay'

            return 'spontaneous recovery'

        a_ards_dict = json.loads(a_ards_info.to_json(orient='records'))[0]
        # base info
        a_ards_info.loc[0, 'icu_stay_id'] = icu_stay_id
        a_ards_info.loc[0, 'identification_offset'] = identification_offset

        a_ards_info.loc[0, 'admission_diagnosis'] = diagnoses_dict.get(a_ards_dict['apacheadmissiondx'], 'Other')

        if a_ards_dict['unitdischargestatus'] == 'Expired':
            a_ards_dict['hospitaldischargestatus'] = 'Expired'
            a_ards_info.loc[0, 'hospitaldischargeoffset'] = a_ards_dict['unitdischargeoffset']

        # 28d_death_status
        a_ards_info.loc[0, '28d_death_status'] = get_death_28d(a_ards_dict)

        # ph_8h_min
        a_ards_info.loc[0, 'pf_8h_min'] = get_pf_8h_min(a_ards_dict)

        # ards_group
        a_ards_info.loc[0, 'ards_group'] = get_ards_group(a_ards_dict)

        return a_ards_info

    @staticmethod
    # @log_time
    def reformat_dynamic_feature_of_ards_data(a_ards_dynamic_feature_dict):
        dynamic_feature_data = pd.DataFrame(columns=dynamic_feature_name_list)

        # p/f ratio
        lab_feature = a_ards_dynamic_feature_dict['lab']
        p_f_info = reformat_data_from_dataframe_to_dict_and_remove_outlier(
            lab_feature[(lab_feature['label'] == 'paO2') | (lab_feature['label'] == 'FiO2')])
        # generate p/f
        if set(p_f_info.keys()) == {'paO2', 'FiO2'} and \
                len(p_f_info['paO2'].keys()) >= 2 and len(p_f_info['FiO2'].keys()) >= 2:
            p_f_info = pd.DataFrame(generate_pf_list(p_f_info['paO2'], p_f_info['FiO2']),
                                    columns=['time_offset', 'value'])
            p_f_info.loc[:, 'label'] = 'P/F ratio'
            a_ards_dynamic_feature_dict['P/F ratio'] = p_f_info
        # print('pf:', p_f_info)

        for a_ards_dynamic_feature in a_ards_dynamic_feature_dict.values():
            # feature_format: time_offset, label, value
            # float value
            a_ards_dynamic_feature['value'] = a_ards_dynamic_feature['value'].astype('float')
            # drop no valid data
            delete_list = a_ards_dynamic_feature[
                a_ards_dynamic_feature.apply(lambda x: not SectionValidator.is_valid(x['label'], x['value']), axis=1)]

            a_ards_dynamic_feature.drop(delete_list.index, inplace=True)

            # get extra feature
            # mean and var
            group_feature = a_ards_dynamic_feature.groupby(['label'])
            extra_feature = pd.merge(group_feature['value'].mean(), group_feature['value'].var(), on='label')
            extra_feature.columns = ['mean_value', 'var_value']
            # rate_change
            temp = pd.merge(group_feature.min('time_offset'), group_feature.max('time_offset'), on='label')
            temp = temp.apply(lambda x: x['value_y'] - x['value_x'], axis=1)
            temp.name = 'rate_change_value'
            extra_feature = pd.merge(extra_feature, temp, on='label')

            for index, row in extra_feature.iterrows():
                dynamic_feature_data.loc[0, str(index) + '_median'] = row['mean_value']
                dynamic_feature_data.loc[0, str(index) + '_variance'] = row['var_value']
                dynamic_feature_data.loc[0, str(index) + '_rate_change'] = row['rate_change_value']
        return dynamic_feature_data
