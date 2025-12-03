import os
from dotenv import load_dotenv

load_dotenv()

class DatabaseConfig:
    """Configuration for MySQL database connection"""
    
    # Staging database
    STAGING_HOST = os.getenv('STAGING_DB_HOST', 'localhost')
    STAGING_PORT = int(os.getenv('STAGING_DB_PORT', 3306))
    STAGING_USER = os.getenv('STAGING_DB_USER', 'etl_user')
    STAGING_PASSWORD = os.getenv('STAGING_DB_PASSWORD', 'etl_password')
    STAGING_DATABASE = os.getenv('STAGING_DB_NAME', 'staging_sales')
    
    # Data Warehouse database
    DW_HOST = os.getenv('DW_DB_HOST', 'localhost')
    DW_PORT = int(os.getenv('DW_DB_PORT', 3306))
    DW_USER = os.getenv('DW_DB_USER', 'etl_user')
    DW_PASSWORD = os.getenv('DW_DB_PASSWORD', 'etl_password')
    DW_DATABASE = os.getenv('DW_DB_NAME', 'sales_dw')
    
    # Connection settings
    CHUNK_SIZE = 10000
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    
    @classmethod
    def get_staging_connection_string(cls):
        return f"mysql+mysqlconnector://{cls.STAGING_USER}:{cls.STAGING_PASSWORD}@{cls.STAGING_HOST}:{cls.STAGING_PORT}/{cls.STAGING_DATABASE}"
    
    @classmethod
    def get_dw_connection_string(cls):
        return f"mysql+mysqlconnector://{cls.DW_USER}:{cls.DW_PASSWORD}@{cls.DW_HOST}:{cls.DW_PORT}/{cls.DW_DATABASE}"