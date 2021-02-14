import pandas as pandas

from ApplicationConstants import CSV_FILE_SEPARATOR, INFOS_FILE_TYPE, ITEMS_FILE_TYPE, ORDERS_FILE_TYPE
from cache.ConfigCache import config_cache
from preprocessors.PreprocessorLoader import preprocessor_loader


class DataProcessor:
    def process_data(self, infos_file_path, items_file_path, orders_file_path, out_file_path):
        """
            Downloads data from the given scv files, merge it together and saves as a single file.
            Makes 'time' column atomic breaking it on few columns.
            :param infos_file_path: path to a csv file that contains item prices and promotion dates.
            :param items_file_path: path to a csv file that contains items info.
            :param orders_file_path: path to a csv file that contains orders info.
            :param out_file_path: output file path.
        """

        infos_df = self.download_file(infos_file_path)
        items_df = self.download_file(items_file_path)
        orders_df = self.download_file(orders_file_path)

        self.preprocess_exclude_columns(
            (infos_df, INFOS_FILE_TYPE), (items_df, ITEMS_FILE_TYPE), (orders_df, ORDERS_FILE_TYPE))
        self.preprocess_data((infos_df, INFOS_FILE_TYPE), (items_df, ITEMS_FILE_TYPE), (orders_df, ORDERS_FILE_TYPE))

        result_df = self.join_dataframes(infos_df, items_df, orders_df)

        self.save_result_df(out_file_path, result_df)

    def preprocess_data(self, *dfs_tuples):
        for df_tuple in dfs_tuples:
            preprocessor_loader.preprocess(df_tuple[0], df_tuple[1])

    def download_file(self, file_path):
        return pandas.read_csv(file_path, sep=CSV_FILE_SEPARATOR)

    def preprocess_exclude_columns(self, *dfs_tuples):
        exclude_columns = config_cache.get_preprocess_exclude_columns()

        for df_tuple in dfs_tuples:
            file_excluded_columns = next(
                filter(lambda ec: ec['target_file'] == df_tuple[1], exclude_columns), None)

            if file_excluded_columns is not None:
                file_excluded_columns = file_excluded_columns['exclude_columns']

                for column in file_excluded_columns:
                    del df_tuple[0][column]

    def join_dataframes(self, *dfs):
        result_df = dfs[0]

        for i in range(1, len(dfs)):
            result_df = pandas.merge(result_df, dfs[i], on=config_cache.get_join_column())

        return result_df

    def save_result_df(self, filepath, df):
        df.to_csv(filepath, index=False)
