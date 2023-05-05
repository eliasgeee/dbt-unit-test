from enum import Enum

class DbtRunType(Enum):
    FULLREFRESH = 1
    INCREMENTAL = 2
