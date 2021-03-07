from cache.ConfigCache import config_cache


class AggregationPostprocessor:
    def postprocess(self, df):
        aggregated_df = df.groupby(config_cache.get_data_aggregation()["group_by_columns"])[
            config_cache.get_data_aggregation()["sum_column"]].sum().reset_index()

        df.drop(df.index, inplace=True)

        for column in aggregated_df.columns:
            df[column] = aggregated_df[column]

