import os
from dotenv import load_dotenv

load_dotenv()

def config():
    """Load database configuration from .env file"""
    db = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    missing = [k for k, v in db.items() if not v]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}. "
                        f"Please set them in .env file (DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)")
    
    if db['port']:
        db['port'] = int(db['port'])
    
    return db