from starlette.config import Config

try:
    config = Config('.env')
except:
    config = Config()


data_base_url = config("DATABASE_URL" ,cast=str)