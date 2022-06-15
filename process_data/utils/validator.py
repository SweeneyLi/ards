import os
import yaml

__all__ = ['DataValidator']

config_path = '/Users/sweeney/WorkSpace/WorkCode/aids/process_data/config.yaml'
assert os.path.isfile(config_path)
with open(config_path, 'r') as f:
    config = yaml.load(f.read(), Loader=yaml.Loader)
SECTION_RANGE_RICT = config['section_range']


class BaseSectionValidator:
    def __init__(self, min_value, min_open, max_value, max_open):
        self.min_value = float(min_value) if min_value.lower() != '-inf' else '-inf'
        self.min_open = min_open
        self.max_value = float(max_value)
        self.max_open = max_open

    def is_valid(self, input_value):
        if self.min_value != '-inf' and self.min_open and input_value < self.min_value:
            return False
        elif self.min_value != '-inf' and not self.min_open and input_value <= self.min_value:
            return False
        elif self.max_open and input_value > self.max_value:
            return False
        elif not self.max_open and input_value >= self.max_value:
            return False
        return True


class SectionValidator:
    section_validator_dict = {}

    @staticmethod
    def base_section_validator_constructor(input_value):
        assert len(input_value) >= 4 and ',' in input_value
        min_open = input_value[0] == '['
        max_open = input_value[-1] == ']'
        min_v, max_v = input_value[1:-1].split(',')
        return BaseSectionValidator(min_v, min_open, max_v, max_open)

    @staticmethod
    def is_valid(name, value):
        if not SectionValidator.section_validator_dict.get(name, None):
            # print(name)
            assert name in SECTION_RANGE_RICT.keys()
            SectionValidator.section_validator_dict[name] = SectionValidator.base_section_validator_constructor(
                SECTION_RANGE_RICT[name])
        return SectionValidator.section_validator_dict[name].is_valid(value)


assert SectionValidator.is_valid('PEEP filter', -1) is False
assert SectionValidator.is_valid('PEEP filter', 5) is True
assert SectionValidator.is_valid('PEEP filter', 10) is True
assert SectionValidator.is_valid('PEEP filter', 25) is True
assert SectionValidator.is_valid('PEEP filter', 26) is False


class DataValidator:
    # need to valid type
    valid_type = ['P/F ratio', 'PEEP']
    # continue offset of valid section
    continue_offset = 8 * 60

    @staticmethod
    def get_identification_offset(pao2_fio2_peep_info):
        pao2_fio2_peep_info = DataValidator.reformat_data(pao2_fio2_peep_info)

        identification_offset = -100000
        for section_type in DataValidator.valid_type:
            continuous_offset = DataValidator.get_continuous_offset(DataValidator.continue_offset,
                                                      section_type,
                                                      pao2_fio2_peep_info[section_type])
            if continuous_offset is None:
                return None
            identification_offset = max(identification_offset, continuous_offset)
        return identification_offset

    @staticmethod
    def reformat_data(db_data):
        # change to dict
        pao2_fio2_peep_info = {'FiO2': {}, 'paO2': {}, 'PEEP': {}}
        for index, row in db_data.iterrows():
            name = row['label']
            value = row['value']
            offset = row['offset']
            if SectionValidator.is_valid(name, value):
                pao2_fio2_peep_info[name][offset] = value

        # prepare data
        for k, v in pao2_fio2_peep_info.items():
            pao2_fio2_peep_info[k] = sorted(pao2_fio2_peep_info[k].items(), key=lambda x: x[0])

        pao2_fio2_peep_info['P/F ratio'] = DataValidator.generate_pf_list(pao2_fio2_peep_info['paO2'],
                                                                   pao2_fio2_peep_info['FiO2'])

        return pao2_fio2_peep_info

    @staticmethod
    def generate_pf_list(pao2_list, fio2_list):
        pf_list = []
        if len(pao2_list) < 2 or len(fio2_list) < 2:
            return pf_list

        time_list = sorted(list(map(lambda x: x[0], pao2_list)) + list(map(lambda x: x[0], fio2_list)))
        start_time = max(pao2_list[0][0], fio2_list[0][0])

        len_pao2 = len(pao2_list)
        len_fio2 = len(fio2_list)
        p_index = 0
        f_index = 0

        for time_value in time_list:
            if time_value < start_time:
                continue
            while p_index < len_pao2 and pao2_list[p_index][0] <= time_value:
                p_index += 1
            while f_index < len_fio2 and fio2_list[f_index][0] <= time_value:
                f_index += 1
            last_p_v = pao2_list[p_index - 1][1]
            last_f_v = fio2_list[f_index - 1][1]
            pf_list.append([time_value, last_p_v / last_f_v])
            # print('%d: %f / %f = %f' % (time_value, last_p_v, last_f_v, last_p_v / last_f_v))

        return pf_list

    @staticmethod
    def get_continuous_offset(continue_offset, section_type, points):
        section_type = section_type + ' filter'
        if len(points) < 2:
            return None
        len_points = len(points)

        start_index = -1
        for i in range(len_points):
            if SectionValidator.is_valid(section_type, points[i][1]):
                start_index = i
                break

        if start_index == -1:
            return None

        end_index = start_index

        while end_index < len_points:
            if SectionValidator.is_valid(section_type, points[end_index][1]):
                if points[end_index][0] - points[start_index][0] >= continue_offset:
                    return points[end_index][0]

                end_index += 1
            else:
                while end_index < len_points and not SectionValidator.is_valid(section_type, points[end_index][1]):
                    end_index += 1
                start_index = end_index

        return None


assert DataValidator.generate_pf_list(
    [[0, 300], [1, 300], [10, 200], [15, 100]],
    [[5, 10], [20, 5]]
) == [[5, 30.0], [10, 20.0], [15, 10.0], [20, 20.0]]

assert DataValidator.get_continuous_offset(8, 'PEEP', [
    [-1, 5], [10, 3], [11, 2], [19, 5]
]) is None
assert DataValidator.get_continuous_offset(8, 'PEEP', [
    [-1, 2], [10, 8], [11, 8], [19, 10], [20, 3]
]) == 19
