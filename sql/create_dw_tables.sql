-- Create Data Warehouse database
CREATE DATABASE IF NOT EXISTS sales_dw;
USE sales_dw;

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    year INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE,
    UNIQUE KEY unique_full_date (full_date)
);

-- Dimension: Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100),
    registration_date DATE,
    customer_segment VARCHAR(50),
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE KEY unique_customer_id (customer_id, valid_from)
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    supplier VARCHAR(255),
    cost_price DECIMAL(10, 2),
    msrp DECIMAL(10, 2),
    profit_margin DECIMAL(5, 2),
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE KEY unique_product_id (product_id, valid_from)
);

-- Fact: Sales
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT NOT NULL,
    product_key INT NOT NULL,
    order_id VARCHAR(50) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    cost_amount DECIMAL(12, 2),
    profit_amount DECIMAL(12, 2),
    profit_margin DECIMAL(5, 2),
    order_timestamp TIMESTAMP,
    
    -- Foreign keys
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    
    -- Business keys
    UNIQUE KEY unique_order (order_id, product_key)
);

-- Create aggregate table for performance
CREATE TABLE IF NOT EXISTS agg_sales_daily (
    agg_key INT AUTO_INCREMENT PRIMARY KEY,
    date_key INT NOT NULL,
    customer_key INT,
    product_key INT,
    total_quantity INT,
    total_amount DECIMAL(12, 2),
    avg_unit_price DECIMAL(10, 2),
    order_count INT,
    unique_customers INT,
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key)
);