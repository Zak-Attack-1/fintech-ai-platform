"""
Setup script for Phase 2 - dbt implementation
"""
import subprocess
import os
from pathlib import Path

def create_dbt_structure():
    """Create the dbt directory structure"""
    
    directories = [
        'dbt/fintech_analytics/models/staging',
        'dbt/fintech_analytics/models/intermediate', 
        'dbt/fintech_analytics/models/marts',
        'dbt/fintech_analytics/models/analytics',
        'dbt/fintech_analytics/macros',
        'dbt/fintech_analytics/tests',
        'dbt/fintech_analytics/analyses',
        'dbt/fintech_analytics/seeds',
        'dbt/fintech_analytics/snapshots'
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def install_dbt():
    """Install dbt and required packages"""
    
    packages = [
        'dbt-postgres==1.6.0',
        'dbt-utils'
    ]
    
    for package in packages:
        try:
            subprocess.check_call(['pip', 'install', package])
            print(f"✅ Installed {package}")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install {package}: {e}")

def main():
    print("🚀 Setting up Phase 2: Advanced Data Modeling with dbt")
    print("=" * 60)
    
    # Install dbt
    print("\n📦 Installing dbt...")
    install_dbt()
    
    # Create directory structure
    print("\n📁 Creating dbt directory structure...")
    create_dbt_structure()
    
    print("\n✅ Phase 2 setup complete!")
    print("\n📋 Next Steps:")
    print("1. Copy the dbt configuration files from the artifacts")
    print("2. Copy the staging models")
    print("3. Run: cd dbt/fintech_analytics && dbt debug")
    print("4. Run: dbt run to build your first models!")

if __name__ == "__main__":
    main()