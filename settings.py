import json
import os


class Settings():
    def __init__(self) -> None:
        self.settings_path = os.path.join(os.getcwd(), "settings.json")
        settings_file = open(self.settings_path)
        settings_data = json.load(settings_file)
        self.__dict__.update(settings_data)
        # for s in settings_data:
        #     setattr[self, s, settings_data[s]]
