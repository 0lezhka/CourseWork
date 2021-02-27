from pydoc import locate

from cache.ConfigCache import config_cache


class PreprocessorLoader:
    __preprocessors = []

    def __init__(self):
        self.__register_preprocessors()

    def preprocess(self, df, file_type):
        for preprocessor in self.__preprocessors:
            if file_type in preprocessor[1]:
                preprocessor[0].process(df)

    def __register_preprocessors(self):
        preprocessors_pairs = config_cache.get_preprocessors()

        preprocessors_pairs.sort(key=lambda o: o.get("order"))

        for preprocessor_pair in preprocessors_pairs:
            processor_class = preprocessor_pair["processor"]

            self.__preprocessors.append((self.__create_preprocessor_instance(processor_class, processor_class),
                                         preprocessor_pair['target_files']))

    def __create_preprocessor_instance(self, module_name, class_name):
        preprocessor_class = locate(f'preprocessors.{module_name}.{class_name}')

        return preprocessor_class()


preprocessor_loader = PreprocessorLoader()
