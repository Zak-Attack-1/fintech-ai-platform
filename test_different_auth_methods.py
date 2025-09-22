#!/usr/bin/env python3
"""
Test different authentication methods
"""
import psycopg2

def test_auth_methods():
    """Test various authentication approaches"""
    
    print("Testing different authentication methods:")
    print("=" * 50)
    
    # Test 1: Standard connection
    print("\n1. Standard connection with password:")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="fintech_analytics",
            user="fintech_user",
            password="Rlzahinmyh3art"
        )
        print("   ✅ SUCCESS!")
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # Test 2: Connection without password (trust authentication)
    print("\n2. Connection without password:")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="fintech_analytics",
            user="fintech_user"
            # No password parameter
        )
        print("   ✅ SUCCESS!")
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # Test 3: Connect as postgres superuser first
    print("\n3. Connect as postgres superuser:")
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="postgres",
            user="postgres"
            # No password
        )
        print("   ✅ Connected as postgres!")
        
        # Check if fintech_user exists and has proper permissions
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rolname, rolcanlogin, rolpassword IS NOT NULL as has_password 
            FROM pg_roles 
            WHERE rolname IN ('fintech_user', 'postgres');
        """)
        roles = cursor.fetchall()
        print("   Roles in database:")
        for role, can_login, has_pwd in roles:
            print(f"     {role}: can_login={can_login}, has_password={has_pwd}")
        
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
    
    # Test 4: IPv4 vs IPv6
    print("\n4. Testing localhost vs 127.0.0.1:")
    for host in ["localhost", "127.0.0.1"]:
        try:
            conn = psycopg2.connect(
                host=host,
                port=5433,
                database="fintech_analytics",
                user="fintech_user",
                password="Rlzahinmyh3art"
            )
            print(f"   ✅ {host} works!")
            conn.close()
            return True
        except Exception as e:
            print(f"   ❌ {host} failed: {e}")
    
    return False

if __name__ == "__main__":
    success = test_auth_methods()
    if not success:
        print("\n" + "=" * 50)
        print("All authentication methods failed.")
        print("Let's check the PostgreSQL configuration...")