-- 1. Main Sales Fact View (for Tableau)
CREATE OR REPLACE VIEW tableau_sales_view AS
SELECT 
    fs.sales_key,
    fs.order_id,
    fs.order_timestamp,
    fs.quantity,
    fs.unit_price,
    fs.total_amount,
    fs.cost_amount,
    fs.profit_amount,
    fs.profit_margin,
    
    -- Date dimensions
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.quarter,
    d.day_of_week,
    d.day_name,
    d.is_weekend,
    
    -- Customer dimensions
    c.customer_id,
    c.customer_name,
    c.email,
    c.phone,
    c.city,
    c.country,
    c.customer_segment,
    c.registration_date,
    
    -- Product dimensions
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.supplier,
    p.cost_price,
    p.msrp,
    p.profit_margin as product_profit_margin
    
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_product p ON fs.product_key = p.product_key
WHERE c.is_current = TRUE AND p.is_current = TRUE;

-- 2. Daily Aggregates View
CREATE OR REPLACE VIEW tableau_daily_aggregates AS
SELECT 
    d.full_date,
    d.year,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    
    COUNT(DISTINCT fs.order_id) as order_count,
    COUNT(DISTINCT fs.customer_key) as customer_count,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as total_sales,
    SUM(fs.cost_amount) as total_cost,
    SUM(fs.profit_amount) as total_profit,
    AVG(fs.profit_margin) as avg_profit_margin,
    
    -- Moving averages
    AVG(SUM(fs.total_amount)) OVER (
        ORDER BY d.full_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as weekly_moving_avg,
    
    -- Month-to-date
    SUM(CASE 
        WHEN d.full_date >= DATE_FORMAT(d.full_date, '%Y-%m-01') 
        THEN fs.total_amount 
        ELSE 0 
    END) as mtd_sales,
    
    -- Year-to-date
    SUM(CASE 
        WHEN d.full_date >= DATE_FORMAT(d.full_date, '%Y-01-01') 
        THEN fs.total_amount 
        ELSE 0 
    END) as ytd_sales
    
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
GROUP BY d.full_date, d.year, d.month, d.month_name, d.day_name, d.is_weekend;

-- 3. Customer Analysis View
CREATE OR REPLACE VIEW tableau_customer_analysis AS
SELECT 
    c.customer_key,
    c.customer_id,
    c.customer_name,
    c.city,
    c.country,
    c.customer_segment,
    c.registration_date,
    
    COUNT(DISTINCT fs.order_id) as total_orders,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as lifetime_value,
    AVG(fs.total_amount) as avg_order_value,
    
    -- Recency (days since last order)
    DATEDIFF(
        CURDATE(), 
        MAX(d.full_date)
    ) as days_since_last_order,
    
    -- Frequency (orders per month)
    COUNT(DISTINCT fs.order_id) / 
    NULLIF(DATEDIFF(CURDATE(), MIN(d.full_date)) / 30.44, 0) as monthly_frequency,
    
    -- Monetary segments
    CASE 
        WHEN SUM(fs.total_amount) > 10000 THEN 'VIP'
        WHEN SUM(fs.total_amount) > 5000 THEN 'Premium'
        WHEN SUM(fs.total_amount) > 1000 THEN 'Regular'
        ELSE 'Basic'
    END as customer_tier
    
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_date d ON fs.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.customer_key, c.customer_id, c.customer_name, c.city, c.country, 
         c.customer_segment, c.registration_date;

-- 4. Product Performance View
CREATE OR REPLACE VIEW tableau_product_performance AS
SELECT 
    p.product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.supplier,
    p.cost_price,
    p.msrp,
    p.profit_margin as expected_margin,
    
    COUNT(DISTINCT fs.order_id) as times_ordered,
    SUM(fs.quantity) as total_quantity_sold,
    SUM(fs.total_amount) as total_revenue,
    SUM(fs.cost_amount) as total_cost,
    SUM(fs.profit_amount) as total_profit,
    AVG(fs.profit_margin) as actual_margin,
    
    -- Inventory turnover (simulated)
    SUM(fs.quantity) * p.cost_price as inventory_cost,
    
    -- Product ranking
    RANK() OVER (ORDER BY SUM(fs.total_amount) DESC) as revenue_rank,
    RANK() OVER (ORDER BY SUM(fs.profit_amount) DESC) as profit_rank,
    
    -- Category performance
    SUM(SUM(fs.total_amount)) OVER (PARTITION BY p.category) as category_revenue,
    SUM(COUNT(DISTINCT fs.order_id)) OVER (PARTITION BY p.category) as category_orders
    
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_key, p.product_id, p.product_name, p.category, p.subcategory, 
         p.supplier, p.cost_price, p.msrp, p.profit_margin;

-- 5. Geographic Analysis View
CREATE OR REPLACE VIEW tableau_geographic_analysis AS
SELECT 
    c.country,
    c.city,
    
    COUNT(DISTINCT c.customer_key) as customer_count,
    COUNT(DISTINCT fs.order_id) as order_count,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as total_sales,
    SUM(fs.profit_amount) as total_profit,
    
    -- Per capita metrics
    SUM(fs.total_amount) / NULLIF(COUNT(DISTINCT c.customer_key), 0) as sales_per_customer,
    SUM(fs.quantity) / NULLIF(COUNT(DISTINCT c.customer_key), 0) as quantity_per_customer,
    
    -- Growth metrics
    SUM(CASE 
        WHEN d.full_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY) 
        THEN fs.total_amount 
        ELSE 0 
    END) as last_30_days_sales,
    
    SUM(CASE 
        WHEN d.full_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY) 
        THEN fs.total_amount 
        ELSE 0 
    END) as last_90_days_sales
    
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_date d ON fs.date_key = d.date_key
WHERE c.is_current = TRUE
GROUP BY c.country, c.city;