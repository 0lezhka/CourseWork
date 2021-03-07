from pydoc import locate

from Logger import rootLogger
from cache.ConfigCache import config_cache


class ProcessorLoader:
    __preprocessors = []
    __postprocessors = []

    def __init__(self):
        self.__register_preprocessors()
        self.__register_postprocessors()

    def preprocess(self, df, file_type):
        for preprocessor in self.__preprocessors:
            if file_type in preprocessor[1]:
                rootLogger.info(f"{type(preprocessor[0]).__name__} preprocessor execution started...")

                preprocessor[0].process(df)

                rootLogger.info(f"{type(preprocessor[0]).__name__} preprocessor execution finished")

    def postprocess(self, df):
        for postprocessor in self.__postprocessors:
            rootLogger.info(f"{type(postprocessor).__name__} postprocessor execution started... ")

            postprocessor.postprocess(df)

            rootLogger.info(f"{type(postprocessor).__name__} postprocessor execution finished")

    def __register_preprocessors(self):
        preprocessors_pairs = config_cache.get_preprocessors()

        preprocessors_pairs.sort(key=lambda o: o.get("order"))

        for preprocessor_pair in preprocessors_pairs:
            processor_class = preprocessor_pair["processor"]

            self.__preprocessors.append((self.__create_processor_instance(processor_class, processor_class),
                                         preprocessor_pair['target_files']))

    def __register_postprocessors(self):
        postprocessors = config_cache.get_postprocessors()

        postprocessors.sort(key=lambda o: o.get("order"))

        for postprocessor in postprocessors:
            processor_class = postprocessor["processor"]

            self.__postprocessors.append(self.__create_processor_instance(processor_class, processor_class))

    def __create_processor_instance(self, module_name, class_name):
        preprocessor_class = locate(f'processors.{module_name}.{class_name}')

        return preprocessor_class()


processor_loader = ProcessorLoader()
