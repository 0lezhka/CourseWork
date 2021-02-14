import fire

from DataProcessor import DataProcessor


class CommandHandler:
    @staticmethod
    def register_commands():
        fire.Fire({
            "process_data": DataProcessor().process_data
        })


CommandHandler().register_commands()
