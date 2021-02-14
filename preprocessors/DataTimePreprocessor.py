import re

from ApplicationConstants import DATE_TIME_REGEX

DATE_TIME_COLUMNS = [
    "transactionYear",
    "transactionMonth",
    "transactionDay",
    "transactionHour",
    "transactionMinute",
    "transactionSecond"
]

TIME_COLUMN_NAME = "time"


class DataTimePreprocessor:
    def process(self, df):
        for column_name in DATE_TIME_COLUMNS:
            self.__insert_date_column(df, column_name)

        del df[TIME_COLUMN_NAME]

    def __insert_date_column(self, df, column_name):
        df.insert(1, column_name, df[TIME_COLUMN_NAME].to_list())
        df[column_name] = \
            df[column_name].apply(
                lambda date_time_str: re.search(DATE_TIME_REGEX, date_time_str).group(column_name))
