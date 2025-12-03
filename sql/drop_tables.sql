-- Script to drop all tables (for cleanup)

-- Drop staging tables
DROP TABLE IF EXISTS staging_sales;
DROP TABLE IF EXISTS staging_customers;
DROP TABLE IF EXISTS staging_products;
DROP TABLE IF EXISTS etl_metadata;

-- Drop DW tables
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS agg_sales_daily;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_date;

-- Drop databases
DROP DATABASE IF EXISTS staging_sales;
DROP DATABASE IF EXISTS sales_dw;