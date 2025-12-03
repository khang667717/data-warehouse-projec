#!/usr/bin/env python3
"""
Script to create all database tables
"""

import mysql.connector
from mysql.connector import Error
import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def execute_sql_file(filename, connection):
    """Execute SQL commands from a file"""
    try:
        cursor = connection.cursor()
        
        with open(filename, 'r') as file:
            sql_commands = file.read()
        
        # Split commands by semicolon
        commands = sql_commands.split(';')
        
        for command in commands:
            if command.strip():
                try:
                    cursor.execute(command)
                    print(f"✓ Executed: {command[:50]}...")
                except Error as e:
                    if "already exists" in str(e).lower():
                        print(f"✓ Table already exists: {e}")
                    else:
                        print(f"✗ Error executing command: {e}")
        
        connection.commit()
        cursor.close()
        print(f"\n✅ Executed {filename}")
        
    except Error as e:
        print(f"❌ Error executing {filename}: {e}")
        connection.rollback()
        raise

def main():
    """Main function to create all tables"""
    try:
        # Get credentials from environment
        db_user = os.getenv('STAGING_DB_USER', 'etl_user')
        db_password = os.getenv('STAGING_DB_PASSWORD', 'etl_password')
        db_host = os.getenv('STAGING_DB_HOST', 'localhost')
        db_port = int(os.getenv('STAGING_DB_PORT', '3306'))
        
        print(f"Connecting to MySQL server at {db_host}:{db_port} as {db_user}")
        
        # Connect to MySQL server
        connection = mysql.connector.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_password
        )
        
        if connection.is_connected():
            print("✅ Connected to MySQL server")
            
            # Execute SQL files in order
            sql_dir = 'sql'
            sql_files = [
                'create_staging_tables.sql',
                'create_dw_tables.sql',
                'create_indexes.sql'
            ]
            
            for sql_file in sql_files:
                file_path = os.path.join(sql_dir, sql_file)
                if os.path.exists(file_path):
                    print(f"\n📄 Processing {sql_file}...")
                    execute_sql_file(file_path, connection)
                else:
                    print(f"⚠️  File not found: {file_path}")
            
            print("\n🎉 All tables created successfully!")
            
    except Error as e:
        print(f"❌ Error: {e}")
        print("\n💡 Troubleshooting tips:")
        print(f"1. Check your credentials in .env file")
        print(f"2. User '{db_user}' should have CREATE privileges")
        print(f"3. Try running: GRANT ALL PRIVILEGES ON *.* TO '{db_user}'@'localhost';")
        
    finally:
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print("\n🔌 MySQL connection closed")

if __name__ == "__main__":
    main()