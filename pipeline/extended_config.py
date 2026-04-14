import os
import yaml

class ExtendedConfig:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            config_path = os.environ.get("EXTENDED_CONFIG", "/data/config/extended_config.yaml")
            with open(config_path, 'r') as file:
                cls._instance.EXTENDED_CONFIG = yaml.safe_load(file)
        return cls._instance

    def get(self, path: str, default=None):
        keys = path.split('.')
        config = self.EXTENDED_CONFIG
        for key in keys:
            if key in config:
                config = config[key]
            else:
                return default
        return config