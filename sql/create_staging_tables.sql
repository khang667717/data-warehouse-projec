-- Create staging database
CREATE DATABASE IF NOT EXISTS staging_sales;
USE staging_sales;

-- Table for raw sales data
CREATE TABLE IF NOT EXISTS staging_sales (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(50),
    order_date DATE,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INT,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(12, 2),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_flag BOOLEAN DEFAULT FALSE,
    error_message TEXT
);

-- Table for raw customers data
CREATE TABLE IF NOT EXISTS staging_customers (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50),
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    registration_date DATE,
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_flag BOOLEAN DEFAULT FALSE
);

-- Table for raw products data
CREATE TABLE IF NOT EXISTS staging_products (
    staging_id INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    supplier VARCHAR(255),
    cost_price DECIMAL(10, 2),
    msrp DECIMAL(10, 2),
    file_name VARCHAR(255),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_flag BOOLEAN DEFAULT FALSE
);

-- Create indexes for better performance
CREATE INDEX idx_staging_sales_order_date ON staging_sales(order_date);
CREATE INDEX idx_staging_sales_customer_id ON staging_sales(customer_id);
CREATE INDEX idx_staging_sales_product_id ON staging_sales(product_id);
CREATE INDEX idx_staging_customers_customer_id ON staging_customers(customer_id);
CREATE INDEX idx_staging_products_product_id ON staging_products(product_id);

-- Create metadata table for tracking ETL processes
CREATE TABLE IF NOT EXISTS etl_metadata (
    process_id INT AUTO_INCREMENT PRIMARY KEY,
    process_name VARCHAR(100),
    source_file VARCHAR(255),
    records_extracted INT,
    records_transformed INT,
    records_loaded INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(50),
    error_message TEXT
);