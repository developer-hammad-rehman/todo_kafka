from starlette.config import Config

try:
    config = Config(".env")

except:
    config = Config()


db_url = config("DATABASE_URL" , cast=str)