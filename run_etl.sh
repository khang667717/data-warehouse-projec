#!/bin/bash

# Script to run the ETL pipeline

echo "Starting ETL Pipeline..."

# Check if required files exist
if [ ! -f "data/customers.csv" ]; then
    echo "Error: customers.csv not found in data directory"
    echo "Run setup_database.sh first or place your data files in the data directory"
    exit 1
fi

if [ ! -f "data/products.csv" ]; then
    echo "Error: products.csv not found in data directory"
    echo "Run setup_database.sh first or place your data files in the data directory"
    exit 1
fi

if [ ! -f "data/sales.csv" ]; then
    echo "Error: sales.csv not found in data directory"
    echo "Run setup_database.sh first or place your data files in the data directory"
    exit 1
fi

# Run the ETL pipeline
echo "Running ETL pipeline..."
python scripts/etl_pipeline.py

if [ $? -eq 0 ]; then
    echo "ETL pipeline completed successfully!"
    echo "Check logs/etl.log for details"
else
    echo "ETL pipeline failed!"
    exit 1
fi