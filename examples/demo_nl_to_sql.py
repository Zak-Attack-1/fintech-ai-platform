"""
Demo script for Natural Language to SQL system
File: examples/demo_nl_to_sql.py

Run this to see the NL to SQL system in action!
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ai.nl_to_sql import nl_sql
import json

def print_result(result, show_data=True):
    """Pretty print query result"""
    print("\n" + "="*70)
    
    if result['success']:
        print(f"✓ Query successful!")
        print(f"  Intent: {result['intent']} (confidence: {result['confidence']:.2%})")
        print(f"  Method: {result['method']}")
        print(f"  Processing time: {result['processing_time']:.2f}s")
        print(f"  Rows returned: {result['row_count']}")
        
        print(f"\nGenerated SQL:")
        print(f"  {result['sql']}")
        
        if show_data and result['results']:
            print(f"\nResults (showing first 5):")
            for i, row in enumerate(result['results'][:5], 1):
                print(f"\n  {i}. {json.dumps(row, indent=6, default=str)}")
    else:
        print(f"✗ Query failed")
        print(f"  Error: {result['error']}")
        if 'sql' in result:
            print(f"  Generated SQL: {result['sql']}")
    
    print("="*70)

def demo_basic_queries():
    """Demo basic query types"""
    print("\n" + "🎯 " + "="*68)
    print("  DEMO 1: Basic Queries")
    print("="*70)
    
    queries = [
        "Show me the top 10 stocks by return",
        "List top 5 cryptocurrencies by daily return",
        "Show stocks with high volatility"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}] Natural Language Query:")
        print(f"    \"{query}\"")
        
        result = nl_sql.process_query(query, use_ai=False)
        print_result(result, show_data=True)
        
        input("\nPress Enter to continue...")

def demo_aggregations():
    """Demo aggregation queries"""
    print("\n" + "📊 " + "="*68)
    print("  DEMO 2: Aggregations & Analytics")
    print("="*70)
    
    queries = [
        "What's the average return by sector?",
        "Show average volatility for each asset type",
        "Calculate mean performance across all sectors"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}] Natural Language Query:")
        print(f"    \"{query}\"")
        
        result = nl_sql.process_query(query, use_ai=False)
        print_result(result, show_data=True)
        
        input("\nPress Enter to continue...")

def demo_comparisons():
    """Demo comparison queries"""
    print("\n" + "⚖️  " + "="*68)
    print("  DEMO 3: Comparisons")
    print("="*70)
    
    queries = [
        "Compare Bitcoin and Ethereum performance",
        "Compare stocks versus crypto returns",
        "Show AAPL vs MSFT vs GOOGL"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}] Natural Language Query:")
        print(f"    \"{query}\"")
        
        result = nl_sql.process_query(query, use_ai=False)
        print_result(result, show_data=True)
        
        input("\nPress Enter to continue...")

def demo_anomalies():
    """Demo anomaly detection queries"""
    print("\n" + "🚨 " + "="*68)
    print("  DEMO 4: Anomaly Detection")
    print("="*70)
    
    queries = [
        "Show me unusual market movements",
        "Find outliers in stock returns",
        "List recent market anomalies"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}] Natural Language Query:")
        print(f"    \"{query}\"")
        
        result = nl_sql.process_query(query, use_ai=False)
        print_result(result, show_data=True)
        
        input("\nPress Enter to continue...")

def interactive_mode():
    """Interactive query mode"""
    print("\n" + "💬 " + "="*68)
    print("  INTERACTIVE MODE")
    print("="*70)
    print("\nAsk questions in plain English!")
    print("Type 'quit' or 'exit' to stop\n")
    
    while True:
        try:
            query = input("\n❓ Your question: ").strip()
            
            if query.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Goodbye!")
                break
            
            if not query:
                continue
            
            # Ask if user wants to use AI
            use_ai = False
            if len(query) > 50:  # Suggest AI for complex queries
                response = input("  Use AI for this query? (y/n): ").lower()
                use_ai = response == 'y'
            
            result = nl_sql.process_query(query, use_ai=use_ai)
            print_result(result, show_data=True)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")

def main():
    """Main demo function"""
    print("\n" + "🤖 " + "="*68)
    print("  Natural Language to SQL - Interactive Demo")
    print("="*70)
    
    print("\nWhat would you like to demo?")
    print("1. Basic Queries (recommended start)")
    print("2. Aggregations & Analytics")
    print("3. Comparisons")
    print("4. Anomaly Detection")
    print("5. Interactive Mode (ask your own questions!)")
    print("6. Run all demos")
    print("0. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (0-6): ").strip()
            
            if choice == '0':
                print("\n👋 Goodbye!")
                break
            elif choice == '1':
                demo_basic_queries()
            elif choice == '2':
                demo_aggregations()
            elif choice == '3':
                demo_comparisons()
            elif choice == '4':
                demo_anomalies()
            elif choice == '5':
                interactive_mode()
            elif choice == '6':
                demo_basic_queries()
                demo_aggregations()
                demo_comparisons()
                demo_anomalies()
                print("\n✓ All demos complete!")
            else:
                print("❌ Invalid choice. Please enter 0-6.")
                continue
            
            # Ask to continue or exit
            if choice in ['1', '2', '3', '4', '6']:
                response = input("\nReturn to menu? (y/n): ").lower()
                if response != 'y':
                    print("\n👋 Goodbye!")
                    break
        
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()