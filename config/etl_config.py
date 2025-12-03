class ETLConfig:
    """Configuration for ETL processes"""
    
    # File paths
    DATA_DIR = "data"
    SALES_FILE = "sales.csv"
    CUSTOMERS_FILE = "customers.csv"
    PRODUCTS_FILE = "products.csv"
    
    # ETL settings
    BATCH_SIZE = 50000
    LOG_FILE = "logs/etl.log"
    
    # Validation rules
    MIN_UNIT_PRICE = 0.01
    MAX_UNIT_PRICE = 10000.00
    MIN_QUANTITY = 1
    MAX_QUANTITY = 1000
    
    # Date range
    START_DATE = "2020-01-01"
    END_DATE = "2025-12-31"