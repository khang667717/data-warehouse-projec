#!/bin/bash

# Setup script for Data Warehouse Project

echo "Setting up Data Warehouse Project..."

# Create directories
mkdir -p logs
mkdir -p data

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "Python3 is not installed. Please install Python 3.8 or higher."
    exit 1
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "pip3 is not installed. Installing pip..."
    python3 -m ensurepip --upgrade
fi

# Upgrade pip first
echo "Upgrading pip..."
pip3 install --upgrade pip

# Install Python dependencies with retry
echo "Installing Python dependencies..."
MAX_RETRIES=3
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
    pip3 install -r requirements.txt
    if [ $? -eq 0 ]; then
        echo "Dependencies installed successfully!"
        break
    else
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "Installation failed. Retry $RETRY_COUNT of $MAX_RETRIES..."
        if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
            sleep 5
        fi
    fi
done

if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "Failed to install dependencies after $MAX_RETRIES attempts."
    echo "Trying alternative installation method..."
    
    # Install dependencies individually
    pip3 install pandas==2.1.4
    pip3 install mysql-connector-python==8.2.0
    pip3 install python-dotenv==1.0.0
    pip3 install sqlalchemy==2.0.23
    pip3 install numpy==1.26.0
    pip3 install python-dateutil==2.8.2
    pip3 install tqdm==4.66.1
    # Skip Airflow for now as it's optional
fi

# Check MySQL connection
if ! command -v mysql &> /dev/null; then
    echo "Warning: MySQL client is not installed or not in PATH."
    echo "Please ensure MySQL server is running."
else
    echo "MySQL client found."
fi

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# MySQL Database Configuration
STAGING_DB_HOST=localhost
STAGING_DB_PORT=3306
STAGING_DB_USER=root
STAGING_DB_PASSWORD=
STAGING_DB_NAME=staging_sales

DW_DB_HOST=localhost
DW_DB_PORT=3306
DW_DB_USER=root
DW_DB_PASSWORD=
DW_DB_NAME=sales_dw

# ETL Configuration
BATCH_SIZE=50000
LOG_FILE=logs/etl.log
EOF
    echo ".env file created. Please update with your database credentials."
    echo "Note: Default user is 'root' with empty password. Update as needed."
fi

# Generate sample data
echo "Generating sample data..."
if python3 data/generate_sample_data.py; then
    echo "Sample data generated successfully!"
else
    echo "Warning: Could not generate sample data. Creating minimal sample files..."
    
    # Create minimal customer data
    cat > data/customers.csv << EOF
customer_id,customer_name,email,phone,address,city,country,registration_date
CUST000001,Customer 1,customer1@example.com,0987123456,123 Street 1,Hanoi,Vietnam,2023-01-15
CUST000002,Customer 2,customer2@example.com,0987234567,456 Street 2,Ho Chi Minh,Vietnam,2023-02-20
CUST000003,Customer 3,customer3@example.com,0987345678,789 Street 3,Da Nang,Vietnam,2023-03-10
EOF
    
    # Create minimal product data
    cat > data/products.csv << EOF
product_id,product_name,category,subcategory,supplier,cost_price,msrp
PROD000001,Product 1 Electronics,Electronics,Phone,Supplier 1,150.00,199.99
PROD000002,Product 2 Clothing,Clothing,Men,Supplier 2,25.00,39.99
PROD000003,Product 3 Food,Food,Snacks,Supplier 3,5.00,8.99
EOF
    
    # Create minimal sales data
    cat > data/sales.csv << EOF
order_id,order_date,customer_id,product_id,quantity,unit_price,total_amount
ORD00000001,2023-01-20,CUST000001,PROD000001,1,199.99,199.99
ORD00000002,2023-02-25,CUST000002,PROD000002,2,39.99,79.98
ORD00000003,2023-03-15,CUST000003,PROD000003,5,8.99,44.95
ORD00000004,2023-04-10,CUST000001,PROD000002,1,39.99,39.99
ORD00000005,2023-05-05,CUST000002,PROD000001,1,199.99,199.99
EOF
    
    echo "Created minimal sample data files."
fi

# Create database tables
echo "Creating database tables..."
if python3 scripts/create_tables.py; then
    echo "Database tables created successfully!"
else
    echo "Warning: Could not create database tables via script."
    echo "You may need to create them manually using SQL files in sql/ directory."
fi

echo "Setup completed!"
echo ""
echo "IMPORTANT: Before running ETL, please:"
echo "1. Update .env file with your MySQL credentials"
echo "2. Ensure MySQL server is running"
echo "3. Create databases manually if needed:"
echo "   CREATE DATABASE staging_sales;"
echo "   CREATE DATABASE sales_dw;"
echo ""
echo "To run ETL pipeline: bash run_etl.sh"
echo "Or use Docker: docker-compose up -d"