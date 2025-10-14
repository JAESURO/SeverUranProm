import os
from configparser import ConfigParser
from dotenv import load_dotenv

load_dotenv()

def config(filename='database.ini', section='postgresql'):
    db = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    if not all(db.values()):
        parser = ConfigParser()
        parser.read(filename)

        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    
    db = {k: v for k, v in db.items() if v is not None}
    
    return db