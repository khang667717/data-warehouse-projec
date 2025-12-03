import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

def generate_customers(num_customers=1000):
    """Generate sample customer data"""
    cities = ['Hanoi', 'Ho Chi Minh', 'Da Nang', 'Hai Phong', 'Can Tho', 'Nha Trang', 'Hue', 'Vung Tau']
    countries = ['Vietnam', 'USA', 'UK', 'Japan', 'Korea', 'Singapore', 'Australia']
    
    customers = []
    for i in range(1, num_customers + 1):
        customer = {
            'customer_id': f'CUST{str(i).zfill(6)}',
            'customer_name': f'Customer {i}',
            'email': f'customer{i}@example.com',
            'phone': f'0987{str(random.randint(100000, 999999))}',
            'address': f'{random.randint(1, 999)} Street',
            'city': random.choice(cities),
            'country': random.choice(countries),
            'registration_date': (datetime.now() - timedelta(days=random.randint(1, 1000))).strftime('%Y-%m-%d')
        }
        customers.append(customer)
    
    df = pd.DataFrame(customers)
    df.to_csv('data/customers.csv', index=False)
    print(f"Generated {len(customers)} customers")

def generate_products(num_products=100):
    """Generate sample product data"""
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home', 'Sports', 'Beauty']
    
    products = []
    for i in range(1, num_products + 1):
        category = random.choice(categories)
        subcategories = {
            'Electronics': ['Phone', 'Laptop', 'Tablet', 'Accessories'],
            'Clothing': ['Men', 'Women', 'Kids', 'Shoes'],
            'Food': ['Snacks', 'Beverages', 'Frozen', 'Fresh'],
            'Books': ['Fiction', 'Non-fiction', 'Educational', 'Children'],
            'Home': ['Furniture', 'Kitchen', 'Decor', 'Gardening'],
            'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Equipment'],
            'Beauty': ['Skincare', 'Makeup', 'Haircare', 'Fragrance']
        }
        
        cost_price = round(random.uniform(10, 500), 2)
        msrp = round(cost_price * random.uniform(1.2, 2.5), 2)
        
        product = {
            'product_id': f'PROD{str(i).zfill(6)}',
            'product_name': f'Product {i} {category}',
            'category': category,
            'subcategory': random.choice(subcategories[category]),
            'supplier': f'Supplier {random.randint(1, 20)}',
            'cost_price': cost_price,
            'msrp': msrp
        }
        products.append(product)
    
    df = pd.DataFrame(products)
    df.to_csv('data/products.csv', index=False)
    print(f"Generated {len(products)} products")

def generate_sales(num_records=1000000):
    """Generate sample sales data"""
    print("Generating sales data...")
    
    # Read existing customers and products
    customers = pd.read_csv('data/customers.csv')
    products = pd.read_csv('data/products.csv')
    
    sales = []
    order_id_counter = 1
    
    # Generate dates for the last 2 years
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    
    for _ in range(num_records):
        # Random date within range
        days_diff = (end_date - start_date).days
        random_days = random.randint(0, days_diff)
        order_date = start_date + timedelta(days=random_days)
        
        # Random customer and product
        customer = customers.sample(1).iloc[0]
        product = products.sample(1).iloc[0]
        
        # Generate order details
        quantity = random.randint(1, 10)
        unit_price = product['msrp'] * random.uniform(0.8, 1.2)
        total_amount = round(quantity * unit_price, 2)
        
        sale = {
            'order_id': f'ORD{str(order_id_counter).zfill(8)}',
            'order_date': order_date.strftime('%Y-%m-%d'),
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'unit_price': round(unit_price, 2),
            'total_amount': total_amount
        }
        sales.append(sale)
        
        order_id_counter += 1
        
        if len(sales) % 100000 == 0:
            print(f"Generated {len(sales)} sales records")
    
    # Create DataFrame and save
    df = pd.DataFrame(sales)
    
    # Shuffle data
    df = df.sample(frac=1).reset_index(drop=True)
    
    # Save in chunks to manage memory
    chunk_size = 200000
    for i in range(0, len(df), chunk_size):
        chunk = df[i:i + chunk_size]
        if i == 0:
            chunk.to_csv('data/sales.csv', index=False)
        else:
            chunk.to_csv('data/sales.csv', mode='a', header=False, index=False)
    
    print(f"Generated {len(sales)} sales records")

if __name__ == "__main__":
    # Create data directory if not exists
    os.makedirs('data', exist_ok=True)
    
    print("Generating sample data...")
    generate_customers(5000)
    generate_products(200)
    generate_sales(1000000)  # 1 million records
    print("Sample data generation completed!")