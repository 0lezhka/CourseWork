from cache.ConfigCache import config_cache

SINGLE_VALUE_COLUMN_REPLACE_VALUE = 0


class MinMaxNormalizerPostprocessor:
    def postprocess(self, df):
        for column in df.columns:
            if column in config_cache.get_data_normalization()['skip_columns']:
                continue

            if len(df[column].unique()) == 1:
                continue

            min_column_value = df[column].min()
            max_column_value = df[column].max()

            df[column] = df[column].apply(lambda x: (x - min_column_value) / (max_column_value - min_column_value))
