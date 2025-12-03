#!/usr/bin/env python3
"""
Main ETL Pipeline Script
Coordinates the entire ETL process
"""
import sys
import os

# Thêm thư mục gốc vào đường dẫn Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from extract_sales import DataExtractor
from transform_sales import DataTransformer
from load_sales import DataLoader
from config.database_config import DatabaseConfig
from config.etl_config import ETLConfig

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(ETLConfig.LOG_FILE),
        logging.StreamHandler()
    ]
)

class ETLPipeline:
    def __init__(self):
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader()
        self.data_dir = ETLConfig.DATA_DIR
        
    def run_full_pipeline(self):
        """Execute complete ETL pipeline"""
        try:
            logging.info("=" * 60)
            logging.info("STARTING ETL PIPELINE")
            logging.info("=" * 60)
            
            start_time = datetime.now()
            
            # Phase 1: Extraction
            logging.info("\nPHASE 1: EXTRACTION")
            logging.info("-" * 40)
            
            self.extractor.extract_customers_data(
                f"{self.data_dir}/{ETLConfig.CUSTOMERS_FILE}"
            )
            self.extractor.extract_products_data(
                f"{self.data_dir}/{ETLConfig.PRODUCTS_FILE}"
            )
            self.extractor.extract_sales_data(
                f"{self.data_dir}/{ETLConfig.SALES_FILE}"
            )
            
            # Phase 2: Transformation
            logging.info("\nPHASE 2: TRANSFORMATION")
            logging.info("-" * 40)
            
            self.transformer.transform_customers()
            self.transformer.transform_products()
            self.transformer.validate_and_clean_sales()
            self.transformer.populate_date_dimension()
            
            # Phase 3: Loading
            logging.info("\nPHASE 3: LOADING")
            logging.info("-" * 40)
            
            self.loader.load_dim_customers()
            self.loader.load_dim_products()
            self.loader.load_fact_sales()
            self.loader.create_aggregates()
            
            # Calculate statistics
            end_time = datetime.now()
            duration = end_time - start_time
            
            logging.info("=" * 60)
            logging.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logging.info(f"Total Duration: {duration}")
            logging.info("=" * 60)
            
            return True
            
        except Exception as e:
            logging.error(f"ETL Pipeline failed: {e}")
            logging.error("=" * 60)
            logging.error("ETL PIPELINE FAILED")
            logging.error("=" * 60)
            raise
    
    def run_incremental(self, date_filter=None):
        """Run incremental ETL for new data"""
        try:
            logging.info("Running incremental ETL")
            
            # This would typically check for new files or date ranges
            # For now, we'll run the full pipeline
            return self.run_full_pipeline()
            
        except Exception as e:
            logging.error(f"Incremental ETL failed: {e}")
            raise
    
    def validate_results(self):
        """Validate ETL results"""
        try:
            logging.info("Validating ETL results...")
            
            # Connect to DW database
            import mysql.connector
            from mysql.connector import Error
            
            config = DatabaseConfig()
            
            connection = mysql.connector.connect(
                host=config.DW_HOST,
                port=config.DW_PORT,
                user=config.DW_USER,
                password=config.DW_PASSWORD,
                database=config.DW_DATABASE
            )
            cursor = connection.cursor()
            
            validation_queries = [
                ("Total Customers", "SELECT COUNT(*) FROM dim_customer WHERE is_current = TRUE"),
                ("Total Products", "SELECT COUNT(*) FROM dim_product WHERE is_current = TRUE"),
                ("Total Sales Records", "SELECT COUNT(*) FROM fact_sales"),
                ("Total Sales Amount", "SELECT SUM(total_amount) FROM fact_sales"),
                ("Date Range", "SELECT MIN(full_date), MAX(full_date) FROM dim_date"),
                ("Unique Orders", "SELECT COUNT(DISTINCT order_id) FROM fact_sales")
            ]
            
            results = {}
            for label, query in validation_queries:
                cursor.execute(query)
                result = cursor.fetchone()[0]
                results[label] = result
                logging.info(f"{label}: {result}")
            
            cursor.close()
            connection.close()
            
            return results
            
        except Error as e:
            logging.error(f"Validation failed: {e}")
            return {}

if __name__ == "__main__":
    # Create pipeline and run
    pipeline = ETLPipeline()
    
    # Run full ETL
    success = pipeline.run_full_pipeline()
    
    if success:
        # Validate results
        pipeline.validate_results()