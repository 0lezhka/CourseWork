from scipy.stats import zscore

from cache.ConfigCache import config_cache


class ZScoreNormalizerPostprocessor:
    def postprocess(self, df):
        for column in df.columns:
            if column in config_cache.get_data_normalization()['skip_columns']:
                continue

            df[column] = zscore(df[column])
