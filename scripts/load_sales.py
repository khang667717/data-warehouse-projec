import mysql.connector
from mysql.connector import Error
import logging
from datetime import datetime
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

class DataLoader:
    def __init__(self):
        self.staging_config = DatabaseConfig()
        self.dw_config = DatabaseConfig()
        self.batch_size = 5000
        
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
                database=db_name,
                allow_local_infile=True  # Allow LOAD DATA LOCAL INFILE
            )
            return connection
        except Error as e:
            logging.error(f"Error connecting to {database} database: {e}")
            raise
    
    def load_dim_customers(self):
        """Load data into dim_customer"""
        try:
            logging.info("Loading dim_customer")
            
            staging_conn = self.create_connection('staging')
            dw_conn = self.create_connection('dw')
            staging_cursor = staging_conn.cursor()
            dw_cursor = dw_conn.cursor()
            
            # Start metadata tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, start_time, status)
                VALUES (%s, %s, %s)
            """
            dw_cursor.execute(metadata_query, ('LOAD_DIM_CUSTOMERS', start_time, 'RUNNING'))
            process_id = dw_cursor.lastrowid
            dw_conn.commit()
            
            # Get valid customer records from staging
            select_query = """
                SELECT DISTINCT 
                    customer_id,
                    customer_name,
                    email,
                    phone,
                    address,
                    city,
                    country,
                    registration_date,
                    CASE 
                        WHEN country IN ('USA', 'UK', 'Australia') THEN 'INTERNATIONAL'
                        WHEN country = 'Vietnam' AND city IN ('Hanoi', 'Ho Chi Minh') THEN 'MAJOR_CITY'
                        ELSE 'OTHER'
                    END as customer_segment
                FROM staging_customers 
                WHERE processed_flag = TRUE
                AND error_message IS NULL
            """
            
            staging_cursor.execute(select_query)
            customers = staging_cursor.fetchall()
            
            # Insert into dimension with SCD Type 2 logic
            insert_query = """
                INSERT INTO dim_customer 
                (customer_id, customer_name, email, phone, address, 
                 city, country, registration_date, customer_segment, valid_from)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURDATE())
                ON DUPLICATE KEY UPDATE
                    customer_name = VALUES(customer_name),
                    email = VALUES(email),
                    phone = VALUES(phone),
                    address = VALUES(address),
                    city = VALUES(city),
                    country = VALUES(country),
                    customer_segment = VALUES(customer_segment),
                    valid_to = CASE 
                        WHEN customer_name != VALUES(customer_name) 
                        OR email != VALUES(email)
                        OR phone != VALUES(phone)
                        OR address != VALUES(address)
                        OR city != VALUES(city)
                        OR country != VALUES(country)
                        THEN CURDATE() - INTERVAL 1 DAY
                        ELSE valid_to
                    END,
                    is_current = CASE 
                        WHEN customer_name != VALUES(customer_name) 
                        OR email != VALUES(email)
                        OR phone != VALUES(phone)
                        OR address != VALUES(address)
                        OR city != VALUES(city)
                        OR country != VALUES(country)
                        THEN FALSE
                        ELSE TRUE
                    END
            """
            
            dw_cursor.executemany(insert_query, customers)
            dw_conn.commit()
            
            loaded_count = dw_cursor.rowcount
            logging.info(f"Loaded {loaded_count} customers")
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED',
                    records_loaded = %s
                WHERE process_id = %s
            """
            dw_cursor.execute(update_query, (end_time, loaded_count, process_id))
            dw_conn.commit()
            
        except Exception as e:
            logging.error(f"Error loading dim_customers: {e}")
            raise
            
        finally:
            if 'staging_conn' in locals() and staging_conn.is_connected():
                staging_cursor.close()
                staging_conn.close()
            if 'dw_conn' in locals() and dw_conn.is_connected():
                dw_cursor.close()
                dw_conn.close()
    
    def load_dim_products(self):
        """Load data into dim_product"""
        try:
            logging.info("Loading dim_product")
            
            staging_conn = self.create_connection('staging')
            dw_conn = self.create_connection('dw')
            staging_cursor = staging_conn.cursor()
            dw_cursor = dw_conn.cursor()
            
            # Start metadata tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, start_time, status)
                VALUES (%s, %s, %s)
            """
            dw_cursor.execute(metadata_query, ('LOAD_DIM_PRODUCTS', start_time, 'RUNNING'))
            process_id = dw_cursor.lastrowid
            dw_conn.commit()
            
            # Get valid product records from staging
            select_query = """
                SELECT DISTINCT 
                    product_id,
                    product_name,
                    category,
                    subcategory,
                    supplier,
                    cost_price,
                    msrp,
                    ROUND(((msrp - cost_price) / msrp) * 100, 2) as profit_margin
                FROM staging_products 
                WHERE processed_flag = TRUE
                AND error_message IS NULL
            """
            
            staging_cursor.execute(select_query)
            products = staging_cursor.fetchall()
            
            # Insert into dimension with SCD Type 2 logic
            insert_query = """
                INSERT INTO dim_product 
                (product_id, product_name, category, subcategory, 
                 supplier, cost_price, msrp, profit_margin, valid_from)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURDATE())
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    category = VALUES(category),
                    subcategory = VALUES(subcategory),
                    supplier = VALUES(supplier),
                    cost_price = VALUES(cost_price),
                    msrp = VALUES(msrp),
                    profit_margin = VALUES(profit_margin),
                    valid_to = CASE 
                        WHEN product_name != VALUES(product_name) 
                        OR category != VALUES(category)
                        OR subcategory != VALUES(subcategory)
                        OR supplier != VALUES(supplier)
                        OR cost_price != VALUES(cost_price)
                        OR msrp != VALUES(msrp)
                        THEN CURDATE() - INTERVAL 1 DAY
                        ELSE valid_to
                    END,
                    is_current = CASE 
                        WHEN product_name != VALUES(product_name) 
                        OR category != VALUES(category)
                        OR subcategory != VALUES(subcategory)
                        OR supplier != VALUES(supplier)
                        OR cost_price != VALUES(cost_price)
                        OR msrp != VALUES(msrp)
                        THEN FALSE
                        ELSE TRUE
                    END
            """
            
            dw_cursor.executemany(insert_query, products)
            dw_conn.commit()
            
            loaded_count = dw_cursor.rowcount
            logging.info(f"Loaded {loaded_count} products")
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED',
                    records_loaded = %s
                WHERE process_id = %s
            """
            dw_cursor.execute(update_query, (end_time, loaded_count, process_id))
            dw_conn.commit()
            
        except Exception as e:
            logging.error(f"Error loading dim_products: {e}")
            raise
            
        finally:
            if 'staging_conn' in locals() and staging_conn.is_connected():
                staging_cursor.close()
                staging_conn.close()
            if 'dw_conn' in locals() and dw_conn.is_connected():
                dw_cursor.close()
                dw_conn.close()
    
    def load_fact_sales(self):
        """Load data into fact_sales"""
        try:
            logging.info("Loading fact_sales")
            
            staging_conn = self.create_connection('staging')
            dw_conn = self.create_connection('dw')
            staging_cursor = staging_conn.cursor()
            dw_cursor = dw_conn.cursor()
            
            # Start metadata tracking
            start_time = datetime.now()
            metadata_query = """
                INSERT INTO etl_metadata 
                (process_name, start_time, status)
                VALUES (%s, %s, %s)
            """
            dw_cursor.execute(metadata_query, ('LOAD_FACT_SALES', start_time, 'RUNNING'))
            process_id = dw_cursor.lastrowid
            dw_conn.commit()
            
            # Get valid sales records in batches
            offset = 0
            total_loaded = 0
            
            while True:
                select_query = f"""
                    SELECT 
                        s.order_id,
                        s.order_date,
                        s.customer_id,
                        s.product_id,
                        s.quantity,
                        s.unit_price,
                        s.total_amount,
                        p.cost_price
                    FROM staging_sales s
                    JOIN staging_products p ON s.product_id = p.product_id
                    WHERE s.processed_flag = TRUE
                    AND s.error_message IS NULL
                    AND p.processed_flag = TRUE
                    AND p.error_message IS NULL
                    ORDER BY s.order_date
                    LIMIT {self.batch_size} OFFSET {offset}
                """
                
                staging_cursor.execute(select_query)
                sales_batch = staging_cursor.fetchall()
                
                if not sales_batch:
                    break
                
                # Prepare data for insertion
                data_to_insert = []
                for row in sales_batch:
                    order_id, order_date, customer_id, product_id, quantity, unit_price, total_amount, cost_price = row
                    
                    # Get dimension keys
                    date_key = int(order_date.strftime('%Y%m%d'))
                    
                    # Get customer key (latest valid version)
                    customer_key_query = """
                        SELECT customer_key 
                        FROM dim_customer 
                        WHERE customer_id = %s 
                        AND is_current = TRUE
                        LIMIT 1
                    """
                    dw_cursor.execute(customer_key_query, (customer_id,))
                    customer_result = dw_cursor.fetchone()
                    customer_key = customer_result[0] if customer_result else None
                    
                    # Get product key (latest valid version)
                    product_key_query = """
                        SELECT product_key 
                        FROM dim_product 
                        WHERE product_id = %s 
                        AND is_current = TRUE
                        LIMIT 1
                    """
                    dw_cursor.execute(product_key_query, (product_id,))
                    product_result = dw_cursor.fetchone()
                    product_key = product_result[0] if product_result else None
                    
                    if customer_key and product_key:
                        cost_amount = quantity * cost_price
                        profit_amount = total_amount - cost_amount
                        profit_margin = (profit_amount / total_amount * 100) if total_amount > 0 else 0
                        
                        data_to_insert.append((
                            date_key,
                            customer_key,
                            product_key,
                            order_id,
                            quantity,
                            unit_price,
                            total_amount,
                            round(cost_amount, 2),
                            round(profit_amount, 2),
                            round(profit_margin, 2),
                            order_date
                        ))
                
                # Insert into fact table
                if data_to_insert:
                    insert_query = """
                        INSERT IGNORE INTO fact_sales 
                        (date_key, customer_key, product_key, order_id, 
                         quantity, unit_price, total_amount, cost_amount, 
                         profit_amount, profit_margin, order_timestamp)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    
                    dw_cursor.executemany(insert_query, data_to_insert)
                    dw_conn.commit()
                    
                    batch_loaded = dw_cursor.rowcount
                    total_loaded += batch_loaded
                    logging.info(f"Loaded batch: {batch_loaded} records (Total: {total_loaded})")
                
                offset += self.batch_size
            
            # Update metadata
            end_time = datetime.now()
            update_query = """
                UPDATE etl_metadata 
                SET end_time = %s, status = 'COMPLETED',
                    records_loaded = %s
                WHERE process_id = %s
            """
            dw_cursor.execute(update_query, (end_time, total_loaded, process_id))
            dw_conn.commit()
            
            logging.info(f"Fact sales loading completed: {total_loaded} records")
            
        except Exception as e:
            logging.error(f"Error loading fact_sales: {e}")
            raise
            
        finally:
            if 'staging_conn' in locals() and staging_conn.is_connected():
                staging_cursor.close()
                staging_conn.close()
            if 'dw_conn' in locals() and dw_conn.is_connected():
                dw_cursor.close()
                dw_conn.close()
    
    def create_aggregates(self):
        """Create aggregate tables for reporting"""
        try:
            logging.info("Creating aggregate tables")
            
            dw_conn = self.create_connection('dw')
            dw_cursor = dw_conn.cursor()
            
            # Clear existing aggregates
            truncate_query = "TRUNCATE TABLE agg_sales_daily"
            dw_cursor.execute(truncate_query)
            
            # Create daily aggregates
            aggregate_query = """
                INSERT INTO agg_sales_daily 
                (date_key, customer_key, product_key, 
                 total_quantity, total_amount, avg_unit_price, 
                 order_count, unique_customers)
                SELECT 
                    fs.date_key,
                    NULL as customer_key,
                    NULL as product_key,
                    SUM(fs.quantity) as total_quantity,
                    SUM(fs.total_amount) as total_amount,
                    AVG(fs.unit_price) as avg_unit_price,
                    COUNT(DISTINCT fs.order_id) as order_count,
                    COUNT(DISTINCT fs.customer_key) as unique_customers
                FROM fact_sales fs
                GROUP BY fs.date_key
                
                UNION ALL
                
                SELECT 
                    fs.date_key,
                    fs.customer_key,
                    NULL as product_key,
                    SUM(fs.quantity) as total_quantity,
                    SUM(fs.total_amount) as total_amount,
                    AVG(fs.unit_price) as avg_unit_price,
                    COUNT(DISTINCT fs.order_id) as order_count,
                    1 as unique_customers
                FROM fact_sales fs
                GROUP BY fs.date_key, fs.customer_key
                
                UNION ALL
                
                SELECT 
                    fs.date_key,
                    NULL as customer_key,
                    fs.product_key,
                    SUM(fs.quantity) as total_quantity,
                    SUM(fs.total_amount) as total_amount,
                    AVG(fs.unit_price) as avg_unit_price,
                    COUNT(DISTINCT fs.order_id) as order_count,
                    COUNT(DISTINCT fs.customer_key) as unique_customers
                FROM fact_sales fs
                GROUP BY fs.date_key, fs.product_key
            """
            
            dw_cursor.execute(aggregate_query)
            dw_conn.commit()
            
            aggregate_count = dw_cursor.rowcount
            logging.info(f"Aggregates created: {aggregate_count} rows")
            
        except Exception as e:
            logging.error(f"Error creating aggregates: {e}")
            raise
            
        finally:
            if 'dw_conn' in locals() and dw_conn.is_connected():
                dw_cursor.close()
                dw_conn.close()

if __name__ == "__main__":
    loader = DataLoader()
    
    # Run all loading processes
    loader.load_dim_customers()
    loader.load_dim_products()
    loader.load_fact_sales()
    loader.create_aggregates()