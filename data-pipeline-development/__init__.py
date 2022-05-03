"""Initial Module.

Source repository: https://github.com/datapipeline


"""

import logging
logging.basicConfig(level=logging.INFO ,format='%(asctime)s : %(levelname)s : %(name)s : %(message)s')
from datapipeline._config import config  # noqa
from datapipeline._properties import execution_step_property  # noqa
__all__ = [
    "config",
]



logging.getLogger("datapipeline").addHandler(logging.NullHandler())
