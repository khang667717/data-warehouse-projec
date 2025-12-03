-- Sample analytical queries for the data warehouse

USE sales_dw;

-- 1. Daily Sales Report
SELECT 
    d.full_date,
    d.day_name,
    SUM(fs.total_amount) as daily_sales,
    SUM(fs.quantity) as total_quantity,
    COUNT(DISTINCT fs.order_id) as order_count,
    COUNT(DISTINCT fs.customer_key) as customer_count
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
GROUP BY d.full_date, d.day_name
ORDER BY d.full_date DESC
LIMIT 30;

-- 2. Top 10 Customers by Sales
SELECT 
    c.customer_name,
    c.city,
    c.country,
    SUM(fs.total_amount) as total_spent,
    COUNT(DISTINCT fs.order_id) as order_count,
    AVG(fs.total_amount) as avg_order_value
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_name, c.city, c.country
ORDER BY total_spent DESC
LIMIT 10;

-- 3. Product Performance Analysis
SELECT 
    p.product_name,
    p.category,
    p.subcategory,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as revenue,
    SUM(fs.cost_amount) as cost,
    SUM(fs.profit_amount) as profit,
    AVG(fs.profit_margin) as avg_margin
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.product_name, p.category, p.subcategory
ORDER BY profit DESC
LIMIT 20;

-- 4. Monthly Sales Trend
SELECT 
    d.year,
    d.month,
    d.month_name,
    SUM(fs.total_amount) as monthly_sales,
    SUM(fs.profit_amount) as monthly_profit,
    COUNT(DISTINCT fs.order_id) as order_count,
    COUNT(DISTINCT fs.customer_key) as customer_count
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year DESC, d.month DESC;

-- 5. Customer Segmentation Analysis
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(fs.total_amount) as segment_revenue,
    AVG(fs.total_amount) as avg_customer_value,
    COUNT(DISTINCT fs.order_id) / COUNT(DISTINCT c.customer_key) as avg_orders_per_customer
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.customer_segment
ORDER BY segment_revenue DESC;

-- 6. Hourly Sales Pattern (if timestamp available)
SELECT 
    HOUR(order_timestamp) as hour_of_day,
    COUNT(DISTINCT order_id) as order_count,
    SUM(total_amount) as hourly_sales,
    AVG(total_amount) as avg_order_value
FROM fact_sales
WHERE order_timestamp IS NOT NULL
GROUP BY HOUR(order_timestamp)
ORDER BY hour_of_day;

-- 7. Year-over-Year Growth
SELECT 
    current.year,
    current.month,
    current.month_name,
    current.monthly_sales,
    previous.monthly_sales as prev_year_sales,
    ROUND(((current.monthly_sales - previous.monthly_sales) / previous.monthly_sales) * 100, 2) as yoy_growth_percent
FROM (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        SUM(fs.total_amount) as monthly_sales
    FROM fact_sales fs
    JOIN dim_date d ON fs.date_key = d.date_key
    WHERE d.year = YEAR(CURDATE())
    GROUP BY d.year, d.month, d.month_name
) current
LEFT JOIN (
    SELECT 
        d.year,
        d.month,
        SUM(fs.total_amount) as monthly_sales
    FROM fact_sales fs
    JOIN dim_date d ON fs.date_key = d.date_key
    WHERE d.year = YEAR(CURDATE()) - 1
    GROUP BY d.year, d.month
) previous ON current.month = previous.month
ORDER BY current.year DESC, current.month DESC;

-- 8. Top Performing Categories
SELECT 
    p.category,
    COUNT(DISTINCT p.product_key) as product_count,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.total_amount) as category_revenue,
    SUM(fs.profit_amount) as category_profit,
    ROUND((SUM(fs.profit_amount) / SUM(fs.total_amount)) * 100, 2) as category_margin
FROM fact_sales fs
JOIN dim_product p ON fs.product_key = p.product_key
WHERE p.is_current = TRUE
GROUP BY p.category
ORDER BY category_profit DESC;

-- 9. Customer Retention Analysis
SELECT 
    registration_year,
    COUNT(customer_key) as total_customers,
    SUM(CASE WHEN last_order_year >= registration_year THEN 1 ELSE 0 END) as retained_customers,
    ROUND(SUM(CASE WHEN last_order_year >= registration_year THEN 1 ELSE 0 END) / COUNT(customer_key) * 100, 2) as retention_rate
FROM (
    SELECT 
        c.customer_key,
        YEAR(c.registration_date) as registration_year,
        YEAR(MAX(fs.order_timestamp)) as last_order_year
    FROM dim_customer c
    LEFT JOIN fact_sales fs ON c.customer_key = fs.customer_key
    WHERE c.is_current = TRUE
    GROUP BY c.customer_key, c.registration_date
) customer_data
GROUP BY registration_year
ORDER BY registration_year DESC;

-- 10. Sales by Geography
SELECT 
    c.country,
    c.city,
    COUNT(DISTINCT c.customer_key) as customer_count,
    SUM(fs.total_amount) as regional_sales,
    COUNT(DISTINCT fs.order_id) as order_count,
    AVG(fs.total_amount) as avg_order_value
FROM fact_sales fs
JOIN dim_customer c ON fs.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.country, c.city
ORDER BY regional_sales DESC;