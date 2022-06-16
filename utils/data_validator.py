from utils.init_config import dynamic_range_dict

__all__ = ['SectionValidator']


class BaseSectionValidator:
    def __init__(self, min_value=None, min_open=None, max_value=None, max_open=None):
        if min_value is None:
            self.min_value = None
            self.min_open = None
            self.max_value = None
            self.max_open = None
            return

        self.min_value = float(min_value) if min_value.lower() != '-inf' else '-inf'
        self.min_open = min_open
        self.max_value = float(max_value)
        self.max_open = max_open

    def is_valid(self, input_value):
        if self.min_value is None:
            return True

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
        if input_value is None:
            return BaseSectionValidator()

        assert len(input_value) >= 4 and ',' in input_value
        min_open = input_value[0] == '['
        max_open = input_value[-1] == ']'
        min_v, max_v = input_value[1:-1].split(',')
        return BaseSectionValidator(min_v, min_open, max_v, max_open)

    @staticmethod
    def is_valid(name, value):
        if not SectionValidator.section_validator_dict.get(name, None):
            # print(name, dynamic_range_dict.keys())
            if name not in dynamic_range_dict.keys():
                print(name, dynamic_range_dict.keys())
            assert name in dynamic_range_dict.keys()
            SectionValidator.section_validator_dict[name] = SectionValidator.base_section_validator_constructor(
                dynamic_range_dict[name])
        return SectionValidator.section_validator_dict[name].is_valid(value)


assert SectionValidator.is_valid('PEEP filter', -1) is False
assert SectionValidator.is_valid('PEEP filter', 5) is True
assert SectionValidator.is_valid('PEEP filter', 10) is True
assert SectionValidator.is_valid('PEEP filter', 25) is True
assert SectionValidator.is_valid('PEEP filter', 26) is False
