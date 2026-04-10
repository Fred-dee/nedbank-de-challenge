import os
import yaml

class PipelineConfig:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            config_path = os.environ.get("PIPELINE_CONFIG", "/data/config/pipeline_config.yaml")
            with open(config_path, 'r') as file:
                cls._instance.PIPELINE_CONFIG = yaml.safe_load(file)
        return cls._instance

    def get(self, path: str, default=None):
        keys = path.split('.')
        config = self.PIPELINE_CONFIG
        for key in keys:
            if key in config:
                config = config[key]
            else:
                return default
        return config
