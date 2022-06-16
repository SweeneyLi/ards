from .common import *
from init_config import diagnoses_dict
from utils.postgres_sql import PostgresSqlConnector

__all__ = ['FeatureExtractor']


class FeatureExtractor:
    sql_connector = PostgresSqlConnector()
    valid_type = ['P/F ratio', 'PEEP']
    # continue offset of valid section
    continue_offset = 8 * 60

    @staticmethod
    def get_identification_offset(pao2_fio2_peep_info):
        assert list(pao2_fio2_peep_info.keys()) == ['paO2', 'FiO2', 'PEEP']

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
    def add_feature_of_ards_data(sql_connector, a_ards_info, icu_stay_id, identification_offset):
        # extra feature is admission_diagnosis, mortality_28d, pf_8h_min, ards_group

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
            pf_8h_info = reformat_data_from_dataframe_to_dict(pf_8h_data)
            assert list(pf_8h_info.keys()) == ['paO2', 'FiO2']
            pf_8h_list = generate_pf_list(pf_8h_info['paO2', 'FiO2'])
            return min(list(map(lambda x: x[1], pf_8h_list)))

        def get_ards_group(x):
            pass

        # base info
        a_ards_info['icu_stay_id'] = icu_stay_id
        a_ards_info['identification_offset'] = identification_offset
        a_ards_info['admission_diagnosis'] = diagnoses_dict.get(a_ards_info['apacheadmissiondx'], 'Other')

        # 28d_death_status
        a_ards_info['28d_death_status'] = get_death_28d(a_ards_info)

        # ph_8h_min
        a_ards_info['pf_8h_min'] = get_pf_8h_min(a_ards_info)

        # ards_group
        a_ards_info['ards_group'] = get_ards_group(a_ards_info)

        return a_ards_info