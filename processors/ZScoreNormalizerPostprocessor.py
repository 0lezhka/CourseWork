from scipy.stats import zscore


class ZScoreNormalizerPostprocessor:
    def postprocess(self, df):
        for column in df.columns:
            df[column] = zscore(df[column])
