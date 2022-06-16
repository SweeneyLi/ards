from .common import *

__all__ = ['FeatureExtractor']


class FeatureExtractor:
    valid_type = ['P/F ratio', 'PEEP']
    # continue offset of valid section
    continue_offset = 8 * 60

    @staticmethod
    def get_identification_offset(pao2_fio2_peep_info):
        assert list(pao2_fio2_peep_info.keys()) == ['paO2', 'FiO2', 'PEEP']

        # prepare data
        for k, v in pao2_fio2_peep_info.items():
            pao2_fio2_peep_info[k] = sorted(pao2_fio2_peep_info[k].items(), key=lambda x: x[0])

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
