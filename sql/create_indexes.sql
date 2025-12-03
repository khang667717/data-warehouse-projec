USE sales_dw;

-- Indexes for fact_sales
CREATE INDEX idx_fact_sales_date_key ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_order_id ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_order_timestamp ON fact_sales(order_timestamp);

-- Indexes for dim_customer
CREATE INDEX idx_dim_customer_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_customer_country ON dim_customer(country);
CREATE INDEX idx_dim_customer_city ON dim_customer(city);

-- Indexes for dim_product
CREATE INDEX idx_dim_product_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(category);
CREATE INDEX idx_dim_product_supplier ON dim_product(supplier);

-- Indexes for dim_date
CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Indexes for agg_sales_daily
CREATE INDEX idx_agg_sales_daily_date_key ON agg_sales_daily(date_key);
CREATE INDEX idx_agg_sales_daily_customer_key ON agg_sales_daily(customer_key);
CREATE INDEX idx_agg_sales_daily_product_key ON agg_sales_daily(product_key);