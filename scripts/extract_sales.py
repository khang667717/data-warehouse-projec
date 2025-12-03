import pandas as pd
import mysql.connector
from mysql.connector import Error
from config.database_config import DatabaseConfig
from config.etl_config import ETLConfig
import logging
from datetime import datetime
import os
import sys

# Thêm thư mục gốc vào đường dẫn Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    filename=ETLConfig.LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class DataExtractor:
    def __init__(self):
        self.staging_config = DatabaseConfig()
        self.batch_size = ETLConfig.BATCH_SIZE
        
    def create_staging_connection(self):
        """Create connection to staging database"""
        try:
            connection = mysql.connector.connect(
                host=self.staging_config.STAGING_HOST,
                port=self.staging_config.STAGING_PORT,
                user=self.staging_config.STAGING_USER,
                password=self.staging_config.STAGING_PASSWORD,
                database=self.staging_config.STAGING_DATABASE
            )
            return connection
        except Error as e:
            logging.error(f"Error connecting to staging database: {e}")
            raise
    
    def extract_sales_data(self, file_path):
        """Extract sales data from CSV and load to staging"""
        try:
            logging.info(f"Starting extraction from {file_path}")
            
            # Read CSV in chunks
            chunk_count = 0
            total_records = 0
            file_name = os.path.basename(file_path)
            
            connection = self.create_staging_connection()
            cursor = connection.cursor()
            
            # Check if file already processed
            check_query = """
                SELECT COUNT(*) FROM etl_metadata 
                WHERE process_name = 'EXTRACT_SALES' 
                AND source_file = %s 
                AND status = 'COMPLETED'
            """
            cursor.execute(check_query, (file_name,))
            if cursor.fetchone()[0] > 0:
                logging.warning(f"File {file_name} already processed")
                return
            
            # Start tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, source_file, start_time, status)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(metadata_query, ('EXTRACT_SALES', file_name, start_time, 'RUNNING'))
            process_id = cursor.lastrowid
            connection.commit()
            
            # Process CSV in chunks
            for chunk in pd.read_csv(file_path, chunksize=self.batch_size):
                chunk_count += 1
                records_in_chunk = len(chunk)
                total_records += records_in_chunk
                
                logging.info(f"Processing chunk {chunk_count}: {records_in_chunk} records")
                
                # Prepare data for insertion
                data_to_insert = []
                for _, row in chunk.iterrows():
                    data_to_insert.append((
                        row['order_id'],
                        row['order_date'],
                        row['customer_id'],
                        row['product_id'],
                        row['quantity'],
                        row['unit_price'],
                        row['total_amount'],
                        file_name
                    ))
                
                # Insert into staging
                insert_query = """
                    INSERT INTO staging_sales 
                    (order_id, order_date, customer_id, product_id, 
                     quantity, unit_price, total_amount, file_name)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.executemany(insert_query, data_to_insert)
                connection.commit()
                
                logging.info(f"Chunk {chunk_count} loaded: {records_in_chunk} records")
            
            # Update metadata
            end_time = datetime.now()
            update_metadata_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED', 
                    records_extracted = %s
                WHERE process_id = %s
            """
            cursor.execute(update_metadata_query, (end_time, total_records, process_id))
            connection.commit()
            
            logging.info(f"Extraction completed: {total_records} records loaded")
            
        except Exception as e:
            logging.error(f"Error in extract_sales_data: {e}")
            
            # Update metadata with error
            if 'connection' in locals():
                error_query = """
                    UPDATE etl_metadata 
                    SET end_time = %s, status = 'FAILED', 
                        error_message = %s
                    WHERE process_id = %s
                """
                cursor.execute(error_query, (datetime.now(), str(e), process_id))
                connection.commit()
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
    
    def extract_customers_data(self, file_path):
        """Extract customers data from CSV"""
        try:
            logging.info(f"Extracting customers from {file_path}")
            
            df = pd.read_csv(file_path)
            file_name = os.path.basename(file_path)
            
            connection = self.create_staging_connection()
            cursor = connection.cursor()
            
            # Check if file already processed
            check_query = """
                SELECT COUNT(*) FROM etl_metadata 
                WHERE process_name = 'EXTRACT_CUSTOMERS' 
                AND source_file = %s 
                AND status = 'COMPLETED'
            """
            cursor.execute(check_query, (file_name,))
            if cursor.fetchone()[0] > 0:
                logging.warning(f"File {file_name} already processed")
                return
            
            # Insert metadata
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, source_file, start_time, status)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(metadata_query, ('EXTRACT_CUSTOMERS', file_name, start_time, 'RUNNING'))
            process_id = cursor.lastrowid
            connection.commit()
            
            # Prepare data
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append((
                    row['customer_id'],
                    row['customer_name'],
                    row['email'],
                    row['phone'],
                    row['address'],
                    row['city'],
                    row['country'],
                    row['registration_date'],
                    file_name
                ))
            
            # Insert into staging
            insert_query = """
                INSERT INTO staging_customers 
                (customer_id, customer_name, email, phone, address, 
                 city, country, registration_date, file_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED', 
                    records_extracted = %s
                WHERE process_id = %s
            """
            cursor.execute(update_query, (end_time, len(df), process_id))
            connection.commit()
            
            logging.info(f"Customers extracted: {len(df)} records")
            
        except Exception as e:
            logging.error(f"Error extracting customers: {e}")
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()
    
    def extract_products_data(self, file_path):
        """Extract products data from CSV"""
        try:
            logging.info(f"Extracting products from {file_path}")
            
            df = pd.read_csv(file_path)
            file_name = os.path.basename(file_path)
            
            connection = self.create_staging_connection()
            cursor = connection.cursor()
            
            # Check if file already processed
            check_query = """
                SELECT COUNT(*) FROM etl_metadata 
                WHERE process_name = 'EXTRACT_PRODUCTS' 
                AND source_file = %s 
                AND status = 'COMPLETED'
            """
            cursor.execute(check_query, (file_name,))
            if cursor.fetchone()[0] > 0:
                logging.warning(f"File {file_name} already processed")
                return
            
            # Insert metadata
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, source_file, start_time, status)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(metadata_query, ('EXTRACT_PRODUCTS', file_name, start_time, 'RUNNING'))
            process_id = cursor.lastrowid
            connection.commit()
            
            # Prepare data
            data_to_insert = []
            for _, row in df.iterrows():
                data_to_insert.append((
                    row['product_id'],
                    row['product_name'],
                    row['category'],
                    row['subcategory'],
                    row['supplier'],
                    row['cost_price'],
                    row['msrp'],
                    file_name
                ))
            
            # Insert into staging
            insert_query = """
                INSERT INTO staging_products 
                (product_id, product_name, category, subcategory, 
                 supplier, cost_price, msrp, file_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, data_to_insert)
            connection.commit()
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED', 
                    records_extracted = %s
                WHERE process_id = %s
            """
            cursor.execute(update_query, (end_time, len(df), process_id))
            connection.commit()
            
            logging.info(f"Products extracted: {len(df)} records")
            
        except Exception as e:
            logging.error(f"Error extracting products: {e}")
            raise
            
        finally:
            if 'connection' in locals() and connection.is_connected():
                cursor.close()
                connection.close()

if __name__ == "__main__":
    extractor = DataExtractor()
    
    # Extract all data
    data_dir = ETLConfig.DATA_DIR
    extractor.extract_customers_data(f"{data_dir}/{ETLConfig.CUSTOMERS_FILE}")
    extractor.extract_products_data(f"{data_dir}/{ETLConfig.PRODUCTS_FILE}")
    extractor.extract_sales_data(f"{data_dir}/{ETLConfig.SALES_FILE}")