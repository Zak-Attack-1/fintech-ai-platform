#!/usr/bin/env python3
"""
Debug script to test database connection with detailed error information
"""
import os
import psycopg2
from dotenv import load_dotenv

def debug_connection():
    """Debug database connection with detailed output"""
    load_dotenv()
    
    # Print environment variables
    print("🔍 Environment Variables:")
    print(f"  POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'localhost')}")
    print(f"  POSTGRES_PORT: {os.getenv('POSTGRES_PORT', '5432')}")
    print(f"  POSTGRES_DB: {os.getenv('POSTGRES_DB')}")
    print(f"  POSTGRES_USER: {os.getenv('POSTGRES_USER')}")
    print(f"  DATABASE_URL: {os.getenv('DATABASE_URL')}")
    print()
    
    # Test 1: Direct connection using individual parameters
    print("🧪 Test 1: Direct connection using individual parameters")
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD')
        )
        print("✅ Connection successful!")
        conn.close()
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print(f"Error type: {type(e).__name__}")
    
    print()
    
    # Test 2: Connection using DATABASE_URL
    print("🧪 Test 2: Connection using DATABASE_URL")
    try:
        database_url = os.getenv('DATABASE_URL')
        if database_url:
            conn = psycopg2.connect(database_url)
            print("✅ Connection successful!")
            conn.close()
        else:
            print("❌ DATABASE_URL not found")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print(f"Error type: {type(e).__name__}")
    
    print()
    
    # Test 3: Check if port is accessible
    print("🧪 Test 3: Check if port is accessible")
    import socket
    try:
        port = int(os.getenv('POSTGRES_PORT', '9543'))
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex(('localhost', port))
        sock.close()
        if result == 0:
            print(f"✅ Port {port} is accessible")
        else:
            print(f"❌ Port {port} is not accessible")
    except Exception as e:
        print(f"❌ Socket test failed: {e}")

if __name__ == "__main__":
    debug_connection()