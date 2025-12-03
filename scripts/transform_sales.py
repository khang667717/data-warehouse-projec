import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
import logging
from config.database_config import DatabaseConfig
from config.etl_config import ETLConfig
import sys
import os

# Thêm thư mục gốc vào đường dẫn Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    filename=ETLConfig.LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataTransformer:
    def __init__(self):
        self.staging_config = DatabaseConfig()
        self.dw_config = DatabaseConfig()
        
    def create_connection(self, database='staging'):
        """Create database connection"""
        try:
            if database == 'staging':
                config = self.staging_config
                db_name = config.STAGING_DATABASE
            else:
                config = self.dw_config
                db_name = config.DW_DATABASE
            
            connection = mysql.connector.connect(
                host=config.STAGING_HOST,
                port=config.STAGING_PORT,
                user=config.STAGING_USER,
                password=config.STAGING_PASSWORD,
                database=db_name
            )
            return connection
        except Error as e:
            logging.error(f"Error connecting to {database} database: {e}")
            raise
    
    def validate_and_clean_sales(self):
        """Validate and clean sales data in staging"""
        try:
            logging.info("Starting sales data validation and cleaning")
            
            connection = self.create_connection('staging')
            cursor = connection.cursor()
            
            # Start metadata tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, start_time, status)
                VALUES (%s, %s, %s)
            """
            cursor.execute(metadata_query, ('TRANSFORM_SALES', start_time, 'RUNNING'))
            process_id = cursor.lastrowid
            connection.commit()
            
            # Step 1: Mark invalid records
            validation_query = """
                UPDATE staging_sales 
                SET processed_flag = FALSE,
                    error_message = CASE
                        WHEN quantity < %s THEN 'Quantity below minimum'
                        WHEN quantity > %s THEN 'Quantity above maximum'
                        WHEN unit_price < %s THEN 'Unit price below minimum'
                        WHEN unit_price > %s THEN 'Unit price above maximum'
                        WHEN total_amount != (quantity * unit_price) THEN 'Total amount mismatch'
                        WHEN order_date < %s OR order_date > %s THEN 'Order date out of range'
                        ELSE NULL
                    END
                WHERE processed_flag = FALSE 
                AND error_message IS NULL
            """
            
            cursor.execute(validation_query, (
                ETLConfig.MIN_QUANTITY,
                ETLConfig.MAX_QUANTITY,
                ETLConfig.MIN_UNIT_PRICE,
                ETLConfig.MAX_UNIT_PRICE,
                ETLConfig.START_DATE,
                ETLConfig.END_DATE
            ))
            connection.commit()
            
            invalid_count = cursor.rowcount
            logging.info(f"Marked {invalid_count} invalid records")
            
            # Step 2: Mark valid records
            mark_valid_query = """
                UPDATE staging_sales 
                SET processed_flag = TRUE
                WHERE error_message IS NULL 
                AND processed_flag = FALSE
            """
            cursor.execute(mark_valid_query)
            connection.commit()
            
            valid_count = cursor.rowcount
            logging.info(f"Marked {valid_count} valid records")
            
            # Step 3: Remove duplicates (keep latest)
            deduplicate_query = """
                DELETE ss1 FROM staging_sales ss1
                INNER JOIN staging_sales ss2 
                WHERE ss1.staging_id < ss2.staging_id 
                AND ss1.order_id = ss2.order_id 
                AND ss1.product_id = ss2.product_id
                AND ss1.processed_flag = TRUE
            """
            cursor.execute(deduplicate_query)
            connection.commit()
            
            duplicate_count = cursor.rowcount
            logging.info(f"Removed {duplicate_count} duplicate records")
            
            # Step 4: Fill missing customer/product references
            # This would typically involve matching with external systems
            # For now, we'll just mark them for review
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED',
                    records_transformed = %s
                WHERE process_id = %s
            """
            cursor.execute(update_query, (end_time, valid_count, process_id))
            connection.commit()
            
            logging.info("Sales data transformation completed")
            
        except Exception as e:
            logging.error(f"Error in validate_and_clean_sales: {e}")
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
    
    def transform_customers(self):
        """Transform customer data"""
        try:
            logging.info("Starting customer data transformation")
            
            connection = self.create_connection('staging')
            cursor = connection.cursor()
            
            # Start metadata tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, start_time, status)
                VALUES (%s, %s, %s)
            """
            cursor.execute(metadata_query, ('TRANSFORM_CUSTOMERS', start_time, 'RUNNING'))
            process_id = cursor.lastrowid
            connection.commit()
            
            # Clean customer data
            transform_queries = [
                # Trim whitespace
                """
                UPDATE staging_customers 
                SET customer_name = TRIM(customer_name),
                    email = LOWER(TRIM(email)),
                    phone = TRIM(phone),
                    address = TRIM(address),
                    city = TRIM(city),
                    country = TRIM(country)
                WHERE processed_flag = FALSE
                """,
                
                # Validate email format
                """
                UPDATE staging_customers 
                SET error_message = CASE
                        WHEN email NOT LIKE '%@%.%' THEN 'Invalid email format'
                        ELSE NULL
                    END
                WHERE processed_flag = FALSE
                """,
                
                # Mark valid records
                """
                UPDATE staging_customers 
                SET processed_flag = TRUE
                WHERE error_message IS NULL 
                AND processed_flag = FALSE
                """
            ]
            
            total_transformed = 0
            for query in transform_queries:
                cursor.execute(query)
                connection.commit()
                total_transformed += cursor.rowcount
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED',
                    records_transformed = %s
                WHERE process_id = %s
            """
            cursor.execute(update_query, (end_time, total_transformed, process_id))
            connection.commit()
            
            logging.info(f"Customer transformation completed: {total_transformed} records")
            
        except Exception as e:
            logging.error(f"Error in transform_customers: {e}")
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
    
    def transform_products(self):
        """Transform product data"""
        try:
            logging.info("Starting product data transformation")
            
            connection = self.create_connection('staging')
            cursor = connection.cursor()
            
            # Start metadata tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, start_time, status)
                VALUES (%s, %s, %s)
            """
            cursor.execute(metadata_query, ('TRANSFORM_PRODUCTS', start_time, 'RUNNING'))
            process_id = cursor.lastrowid
            connection.commit()
            
            # Clean product data
            transform_queries = [
                # Trim whitespace and standardize
                """
                UPDATE staging_products 
                SET product_name = TRIM(product_name),
                    category = UPPER(TRIM(category)),
                    subcategory = CONCAT(
                        UPPER(SUBSTRING(TRIM(subcategory), 1, 1)),
                        LOWER(SUBSTRING(TRIM(subcategory), 2))
                    ),
                    supplier = TRIM(supplier)
                WHERE processed_flag = FALSE
                """,
                
                # Validate prices
                """
                UPDATE staging_products 
                SET error_message = CASE
                        WHEN cost_price <= 0 THEN 'Invalid cost price'
                        WHEN msrp <= 0 THEN 'Invalid MSRP'
                        WHEN msrp < cost_price THEN 'MSRP lower than cost'
                        ELSE NULL
                    END
                WHERE processed_flag = FALSE
                """,
                
                # Calculate profit margin
                """
                UPDATE staging_products 
                SET processed_flag = TRUE
                WHERE error_message IS NULL 
                AND processed_flag = FALSE
                """
            ]
            
            total_transformed = 0
            for query in transform_queries:
                cursor.execute(query)
                connection.commit()
                total_transformed += cursor.rowcount
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED',
                    records_transformed = %s
                WHERE process_id = %s
            """
            cursor.execute(update_query, (end_time, total_transformed, process_id))
            connection.commit()
            
            logging.info(f"Product transformation completed: {total_transformed} records")
            
        except Exception as e:
            logging.error(f"Error in transform_products: {e}")
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
    
    def populate_date_dimension(self, start_date='2020-01-01', end_date='2025-12-31'):
        """Populate date dimension table"""
        try:
            logging.info("Populating date dimension")
            
            connection = self.create_connection('dw')
            cursor = connection.cursor()
            
            # Generate date range
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            
            current_date = start
            dates_to_insert = []
            
            while current_date <= end:
                date_key = int(current_date.strftime('%Y%m%d'))
                dates_to_insert.append((
                    date_key,
                    current_date.date(),
                    current_date.day,
                    current_date.month,
                    (current_date.month - 1) // 3 + 1,
                    current_date.year,
                    current_date.weekday() + 1,
                    current_date.strftime('%A'),
                    current_date.strftime('%B'),
                    1 if current_date.weekday() >= 5 else 0,
                    0  # is_holiday
                ))
                current_date += pd.Timedelta(days=1)
            
            # Insert dates
            insert_query = """
                INSERT IGNORE INTO dim_date 
                (date_key, full_date, day, month, quarter, year, 
                 day_of_week, day_name, month_name, is_weekend, is_holiday)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, dates_to_insert)
            connection.commit()
            
            logging.info(f"Date dimension populated: {len(dates_to_insert)} dates")
            
        except Exception as e:
            logging.error(f"Error populating date dimension: {e}")
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

if __name__ == "__main__":
    transformer = DataTransformer()
    
    # Run all transformations
    transformer.transform_customers()
    transformer.transform_products()
    transformer.validate_and_clean_sales()
    transformer.populate_date_dimension()