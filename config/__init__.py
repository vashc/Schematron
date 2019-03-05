import os
import yaml

ROOT = os.path.dirname(os.path.abspath(__file__))
# config_path = os.path.join(ROOT, 'config')

with open(os.path.join(ROOT, 'config.yaml'), 'r') as handler:
    CONFIG = yaml.load(handler.read())

DB_POOL_SETTINGS = CONFIG['DB_POOL']
DB_SETTINGS = CONFIG['DB']
