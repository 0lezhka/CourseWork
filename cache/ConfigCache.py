import json

from ApplicationConstants import CONFIGS_FILE_PATH

PREPROCESS_EXCLUDE_COLUMNS = "preprocess_exclude_columns"
DATA_PREPROCESSORS_PARAM = "data_preprocessors"
DATA_POSTROCESSORS_PARAM = "data_postprocessors"
JOIN_COLUMN = "join_column"
DATA_AGGREGATION = "data_aggregation"
DATA_NORMALIZATION = "data_normalization"


class ConfigCache:
    __configs = None

    def __init__(self):
        with open(CONFIGS_FILE_PATH) as f:
            self.__configs = json.load(f)

    def get_preprocess_exclude_columns(self):
        return self.__configs[PREPROCESS_EXCLUDE_COLUMNS]

    def get_preprocessors(self):
        return self.__configs[DATA_PREPROCESSORS_PARAM]

    def get_postprocessors(self):
        return self.__configs[DATA_POSTROCESSORS_PARAM]

    def get_join_column(self):
        return self.__configs[JOIN_COLUMN]

    def get_data_aggregation(self):
        return self.__configs[DATA_AGGREGATION]

    def get_data_normalization(self):
        return self.__configs[DATA_NORMALIZATION]


config_cache = ConfigCache()
