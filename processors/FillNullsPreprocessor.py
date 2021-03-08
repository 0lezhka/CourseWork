class FillNullsPreprocessor:
    def process(self, df):
        for column in df.columns:
            df[column] = df[column].fillna('Null')
