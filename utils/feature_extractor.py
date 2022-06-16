import operator
import pandas as pd
import numpy as np
from utils.common import *
from utils.init_config import diagnoses_dict, dynamic_feature_list
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
        assert set(pao2_fio2_peep_info.keys()) == {'paO2', 'FiO2', 'PEEP'}

        # prepare data
        pao2_fio2_peep_info['P/F ratio'] = generate_pf_list(pao2_fio2_peep_info['paO2'],
                                                            pao2_fio2_peep_info['FiO2'])

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
            assert set(pf_8h_info.keys()) == {'paO2', 'FiO2'}
            pf_8h_list = generate_pf_list(pf_8h_info['paO2'], pf_8h_info['FiO2'])
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

        # base info
        a_ards_info['icu_stay_id'] = icu_stay_id
        a_ards_info['identification_offset'] = identification_offset

        a_ards_info['admission_diagnosis'] = diagnoses_dict.get(a_ards_info['apacheadmissiondx'], 'Other')

        if a_ards_info['unitdischargestatus'] == 'Expired':
            a_ards_info['hospitaldischargestatus'] = 'Expired'
            a_ards_info['hospitaldischargeoffset'] = a_ards_info['unitdischargeoffset']

        # 28d_death_status
        a_ards_info['28d_death_status'] = get_death_28d(a_ards_info)

        # ph_8h_min
        a_ards_info['pf_8h_min'] = get_pf_8h_min(a_ards_info)

        # ards_group
        a_ards_info['ards_group'] = get_ards_group(a_ards_info)

        return a_ards_info

    @staticmethod
    def add_dynamic_feature_of_ards_data(a_ards_dynamic_feature_list):
        dynamic_feature_column = []
        for feature_name in dynamic_feature_list:
            dynamic_feature_column.append([
                feature_name + '_median',
                feature_name + '_variance',
                feature_name + '_rate_change'
            ])
        dynamic_feature_data = pd.DataFrame(columns=dynamic_feature_column)
        for a_ards_dynamic_feature in a_ards_dynamic_feature_list:
            feature_info_dict = reformat_data_from_dataframe_to_dict_and_remove_outlier(a_ards_dynamic_feature)
            for label, record_dict in feature_info_dict.items():
                record_list = sorted(record_dict.items(), key=lambda x: x[0])

                if len(record_list) == 0:
                    continue

                record_value_list = list(map(record_list, lambda x: x[1]))
                dynamic_feature_data[label + '_media'] = np.mean(record_value_list)
                dynamic_feature_data[label + '_variance'] = np.var(record_value_list)
                dynamic_feature_data[label + '_rate_change'] = record_value_list[-1] - record_value_list[0] if len(
                    record_value_list) > 1 else None
        return dynamic_feature_data
