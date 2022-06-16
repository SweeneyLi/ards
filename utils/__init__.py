from . import common
from . import postgres_sql
from . import validator

__all__ = []
__all__.extend(common.__all__)
__all__.extend(postgres_sql.__all__)
__all__.extend(validator.__all__)

config_path = '/Users/sweeney/WorkSpace/WorkCode/aids/process_data/config.yaml'
common.init_config(config_path)
