import configparser

config = configparser.ConfigParser()
config.read('app.config.props')

class Props():
    # DATABASE CONFIGURATION
    DB_HOST=config.get('DATABASE', 'database.host')
    DB_USER=config.get('DATABASE', 'database.user')
    DB_PASSWORD = config.get('DATABASE', 'database.password')
    DB_SCHEMA = config.get('DATABASE', 'database.schema')
    # FILE PATH CONFIGURATION
    PATH_CONTRACT_MASTER = config.get('PATH', 'path.contract_master')
    PATH_BHAV_ZIP = config.get('PATH', 'path.bhav_copy_zip')
    PATH_BHAV_CSV = config.get('PATH', 'path.bhav_copy_csv')
    # GDFL CONFIGURATION
    GDFL_REST_URL = config.get('GDFL', 'gdfl.base_url')+'accessKey='+config.get('GDFL', 'gdfl.params_accessKey')+'&exchange='+config.get('GDFL', 'gdfl.params_exchange')+'&periodicity='+config.get('GDFL', 'gdfl.params_periodicity')+'&period='+config.get('GDFL', 'gdfl.params_period')
