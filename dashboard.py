import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector
from mysql.connector import Error
import warnings
warnings.filterwarnings('ignore')

# Page config
st.set_page_config(
    page_title="Sales Data Warehouse Dashboard",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Sales Data Warehouse Dashboard")
st.markdown("---")

# Database connection
@st.cache_resource
def get_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            port=3306,
            user="etl_user",
            password="etl_password",
            database="sales_dw"
        )
        return connection
    except Error as e:
        st.error(f"Database connection error: {e}")
        return None

# Load data
@st.cache_data(ttl=300)
def load_data():
    conn = get_connection()
    if conn is None:
        return {}
    
    data = {}
    
    try:
        # Sales summary
        query = """
        SELECT 
            SUM(total_amount) as total_sales,
            SUM(profit_amount) as total_profit,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT order_id) as order_count,
            AVG(profit_margin) as avg_margin
        FROM fact_sales
        """
        data['summary'] = pd.read_sql(query, conn)
        
        # Daily sales trend
        query = """
        SELECT 
            d.full_date,
            d.day_name,
            d.month_name,
            d.year,
            SUM(fs.total_amount) as daily_sales,
            SUM(fs.profit_amount) as daily_profit,
            SUM(fs.quantity) as daily_quantity,
            COUNT(DISTINCT fs.order_id) as daily_orders
        FROM fact_sales fs
        JOIN dim_date d ON fs.date_key = d.date_key
        GROUP BY d.full_date, d.day_name, d.month_name, d.year
        ORDER BY d.full_date
        """
        data['daily'] = pd.read_sql(query, conn)
        
        # Top products
        query = """
        SELECT 
            p.product_name,
            p.category,
            SUM(fs.quantity) as total_quantity,
            SUM(fs.total_amount) as revenue,
            SUM(fs.profit_amount) as profit,
            AVG(fs.profit_margin) as avg_margin
        FROM fact_sales fs
        JOIN dim_product p ON fs.product_key = p.product_key
        GROUP BY p.product_name, p.category
        ORDER BY revenue DESC
        LIMIT 20
        """
        data['products'] = pd.read_sql(query, conn)
        
        # Customer analysis
        query = """
        SELECT 
            c.city,
            c.country,
            c.customer_segment,
            COUNT(DISTINCT c.customer_key) as customer_count,
            SUM(fs.total_amount) as total_sales,
            COUNT(DISTINCT fs.order_id) as order_count,
            AVG(fs.total_amount) as avg_order_value
        FROM fact_sales fs
        JOIN dim_customer c ON fs.customer_key = c.customer_key
        GROUP BY c.city, c.country, c.customer_segment
        """
        data['customers'] = pd.read_sql(query, conn)
        
        # Monthly trends
        query = """
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
        ORDER BY d.year, d.month
        """
        data['monthly'] = pd.read_sql(query, conn)
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
    finally:
        conn.close()
    
    return data

# Load all data
data = load_data()

if not data:
    st.stop()

# KPI Metrics
st.subheader("📈 Key Performance Indicators")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_sales = data['summary']['total_sales'].iloc[0]
    st.metric("Total Sales", f"${total_sales:,.2f}")

with col2:
    total_profit = data['summary']['total_profit'].iloc[0]
    st.metric("Total Profit", f"${total_profit:,.2f}")

with col3:
    total_qty = data['summary']['total_quantity'].iloc[0]
    st.metric("Quantity Sold", f"{total_qty:,.0f}")

with col4:
    order_count = data['summary']['order_count'].iloc[0]
    st.metric("Total Orders", f"{order_count:,.0f}")

with col5:
    avg_margin = data['summary']['avg_margin'].iloc[0]
    st.metric("Avg Profit Margin", f"{avg_margin:.1f}%")

st.markdown("---")

# Charts Section
col1, col2 = st.columns(2)

with col1:
    st.subheader("📅 Daily Sales Trend")
    if not data['daily'].empty:
        fig = px.line(data['daily'], x='full_date', y='daily_sales',
                     title='Daily Sales Over Time',
                     labels={'full_date': 'Date', 'daily_sales': 'Sales ($)'})
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

with col2:
    st.subheader("📦 Top Products by Revenue")
    if not data['products'].empty:
        top_products = data['products'].head(10)
        fig = px.bar(top_products, x='product_name', y='revenue',
                    title='Top 10 Products',
                    labels={'product_name': 'Product', 'revenue': 'Revenue ($)'},
                    color='revenue')
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

col3, col4 = st.columns(2)

with col3:
    st.subheader("🌍 Sales by Customer Segment")
    if not data['customers'].empty:
        segment_sales = data['customers'].groupby('customer_segment')['total_sales'].sum().reset_index()
        fig = px.pie(segment_sales, values='total_sales', names='customer_segment',
                    title='Sales Distribution by Customer Segment')
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

with col4:
    st.subheader("📊 Monthly Performance")
    if not data['monthly'].empty:
        data['monthly']['month_year'] = data['monthly']['month_name'] + ' ' + data['monthly']['year'].astype(str)
        fig = px.bar(data['monthly'], x='month_year', y='monthly_sales',
                    title='Monthly Sales',
                    labels={'month_year': 'Month', 'monthly_sales': 'Sales ($)'})
        fig.update_xaxes(tickangle=45)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

# Data Tables Section
st.markdown("---")
st.subheader("📋 Detailed Data")

tab1, tab2, tab3 = st.tabs(["📦 Products", "👥 Customers", "📈 Sales"])

with tab1:
    st.dataframe(data['products'], use_container_width=True)

with tab2:
    st.dataframe(data['customers'], use_container_width=True)

with tab3:
    st.dataframe(data['daily'], use_container_width=True)

# Filters
st.sidebar.title("🔍 Filters")
st.sidebar.markdown("---")

if not data['daily'].empty:
    date_range = st.sidebar.date_input(
        "Date Range",
        [data['daily']['full_date'].min(), data['daily']['full_date'].max()]
    )

if not data['products'].empty:
    categories = ['All'] + list(data['products']['category'].unique())
    selected_category = st.sidebar.selectbox("Product Category", categories)

# Download options
st.sidebar.markdown("---")
st.sidebar.subheader("📥 Export Data")

if st.sidebar.button("Export Summary Report"):
    summary_df = pd.DataFrame({
        'Metric': ['Total Sales', 'Total Profit', 'Quantity Sold', 'Total Orders', 'Avg Margin'],
        'Value': [total_sales, total_profit, total_qty, order_count, avg_margin]
    })
    csv = summary_df.to_csv(index=False)
    st.sidebar.download_button(
        label="Download CSV",
        data=csv,
        file_name="sales_summary.csv",
        mime="text/csv"
    )

# About section
st.sidebar.markdown("---")
st.sidebar.info("""
**Data Warehouse Dashboard**
- Data Source: MySQL Sales DW
- Last Updated: Real-time
- Total Records: {:,} sales
""".format(len(data['daily'])))

st.markdown("---")
st.caption("Dashboard created with Streamlit | Data Warehouse Project")
