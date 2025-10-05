"""
Demo script for Analysis Engine
File: examples/demo_analysis.py

Run this to see the Analysis Engine in action!
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.ai.analysis_engine import analysis_engine
from src.ai.anomaly_detector import anomaly_detector
import json

def demo_market_analysis():
    """Demo market condition analysis"""
    print("\n" + "📊 " + "="*68)
    print("  DEMO 1: Market Condition Analysis")
    print("="*70)
    
    print("\nAnalyzing current market conditions...")
    
    result = analysis_engine.analyze_market_conditions()
    
    if result['success']:
        print(f"\n✓ Market Analysis for {result['date']}")
        print("="*70)
        
        print(f"\nOverall Metrics:")
        print(f"  Market Mood: {result['overall_metrics']['market_mood'].upper()}")
        print(f"  Average Return: {result['overall_metrics']['avg_return']:.2%}")
        print(f"  Average Volatility: {result['overall_metrics']['avg_volatility']:.2%}")
        
        print(f"\nBy Asset Class:")
        for asset in result['by_asset_class']:
            print(f"\n  {asset['asset_class'].upper()}:")
            print(f"    Return: {float(asset.get('avg_return', 0) or 0):.2%}")
            print(f"    Volatility: {float(asset.get('return_volatility', 0) or 0):.2%}")
            print(f"    Regime: {asset.get('market_regime', 'N/A')}")
            print(f"    Sentiment: {asset.get('risk_sentiment', 'N/A')}")
        
        if result.get('ai_insights'):
            print(f"\n📝 AI Insights:")
            print(f"  {result['ai_insights']}")
    else:
        print(f"✗ Error: {result.get('error')}")
    
    input("\nPress Enter to continue...")

def demo_anomaly_detection():
    """Demo anomaly detection"""
    print("\n" + "🚨 " + "="*68)
    print("  DEMO 2: Anomaly Detection")
    print("="*70)
    
    print("\nScanning for market anomalies in last 30 days...")
    
    result = anomaly_detector.detect_recent_anomalies(days=30)
    
    if result['success']:
        print(f"\n✓ Found {result['anomalies_found']} anomalies")
        print("="*70)
        
        if result['anomalies_found'] > 0:
            print(f"\nSeverity Breakdown:")
            for severity, count in result['by_severity'].items():
                if count > 0:
                    print(f"  {severity.upper():>8}: {count}")
            
            print(f"\nTop Anomalies:")
            for i, anomaly in enumerate(result['top_anomalies'][:5], 1):
                asset = anomaly.get('asset_name', 'Unknown')
                date = anomaly.get('date', 'Unknown')
                return_val = float(anomaly.get('daily_return', 0))
                z_score = anomaly.get('z_score_abs', 0)
                severity = anomaly.get('severity', 'unknown')
                
                print(f"\n  {i}. {asset} on {date}")
                print(f"     Return: {return_val:>8.2%}")
                print(f"     Z-Score: {z_score:>7.2f}")
                print(f"     Severity: {severity.upper()}")
                
                if anomaly.get('similar_patterns'):
                    similar = anomaly['similar_patterns'][0]
                    print(f"     Similar to: {similar['id']} ({similar['similarity']:.1%} match)")
        else:
            print("\n✓ No significant anomalies detected - market is stable!")
    else:
        print(f"✗ Error: {result.get('error')}")
    
    input("\nPress Enter to continue...")

def demo_intelligent_queries():
    """Demo intelligent query system"""
    print("\n" + "💡 " + "="*68)
    print("  DEMO 3: Intelligent Query Analysis")
    print("="*70)
    
    queries = [
        "Compare Bitcoin and Ethereum performance",
        "Show top 5 cryptocurrencies by daily return",
        "List stocks with high volatility"
    ]
    
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}] Query: {query}")
        print("-" * 70)
        
        result = analysis_engine.generate_insight(query, use_ai=False)
        
        if result['success']:
            print(f"✓ Found {result['row_count']} results")
            print(f"  Processing time: {result['processing_time']:.2f}s")
            
            if result.get('summary_statistics'):
                print(f"\n  Summary Statistics:")
                stats = result['summary_statistics']['statistics']
                for metric, values in list(stats.items())[:2]:
                    print(f"    {metric}:")
                    print(f"      Mean: {values['mean']:.4f}")
                    print(f"      Range: {values['min']:.4f} to {values['max']:.4f}")
            
            if result.get('insight'):
                print(f"\n  💭 Insight: {result['insight']}")
            
            if result.get('related_patterns'):
                print(f"\n  🔗 Related patterns found: {len(result['related_patterns'])}")
            
            # Show sample results
            if result['results']:
                print(f"\n  Sample Results:")
                for row in result['results'][:3]:
                    print(f"    {json.dumps(row, indent=6, default=str)[:200]}...")
        else:
            print(f"✗ Error: {result.get('error')}")
        
        if i < len(queries):
            input("\nPress Enter for next query...")

def demo_asset_analysis():
    """Demo individual asset analysis"""
    print("\n" + "🎯 " + "="*68)
    print("  DEMO 4: Asset Performance Analysis")
    print("="*70)
    
    assets = ['BTC', 'ETH']
    
    for asset in assets:
        print(f"\n{'='*70}")
        print(f"Analyzing: {asset}")
        print('='*70)
        
        result = analysis_engine.analyze_asset_performance(asset)
        
        if result['success']:
            asset_data = result['asset']
            
            print(f"\n📈 Basic Info:")
            print(f"  Name: {asset_data.get('asset_name', 'N/A')}")
            print(f"  Type: {asset_data.get('asset_type', 'N/A')}")
            print(f"  Current Price: ${float(asset_data.get('current_price', 0) or 0):,.2f}")
            
            print(f"\n📊 Performance:")
            print(f"  Total Return: {float(asset_data.get('total_return', 0) or 0):.2%}")
            print(f"  Annualized Return: {float(asset_data.get('annualized_return', 0) or 0):.2%}")
            print(f"  Sharpe Ratio: {float(asset_data.get('sharpe_ratio', 0) or 0):.2f}")
            print(f"  Max Drawdown: {float(asset_data.get('max_drawdown', 0) or 0):.2%}")
            
            print(f"\n⚠️  Risk Assessment:")
            risk = result['risk_assessment']
            print(f"  Risk Level: {risk['level']}")
            print(f"  Volatility: {risk['volatility']:.2%}")
            
            print(f"\n📉 Technical Signals:")
            tech = result['technical_summary']
            print(f"  Trend (MA): {tech['trend']}")
            print(f"  Momentum (RSI): {tech['momentum']}")
            print(f"  Risk Profile: {tech['risk_profile']}")
            
            print(f"\n😊 Sentiment: {result['sentiment']['label']} ({result['sentiment']['score']:.2%})")
        else:
            print(f"✗ Error: {result.get('error')}")
        
        input("\nPress Enter to continue...")

def demo_recommendations():
    """Demo recommendation system"""
    print("\n" + "🎁 " + "="*68)
    print("  DEMO 5: AI Recommendations")
    print("="*70)
    
    print("\nGenerating personalized recommendations...")
    
    recs = analysis_engine.get_recommendations()
    
    print(f"\n✓ Generated {len(recs)} recommendations:")
    print("="*70)
    
    for i, rec in enumerate(recs, 1):
        print(f"\n{i}. {rec}")
    
    input("\nPress Enter to continue...")

def demo_summary():
    """Demo anomaly summary"""
    print("\n" + "📋 " + "="*68)
    print("  DEMO 6: Anomaly Summary Report")
    print("="*70)
    
    summary = anomaly_detector.get_anomaly_summary(days=14)
    
    print(f"\n{summary}")
    
    input("\nPress Enter to return to menu...")

def main():
    """Main demo function"""
    print("\n" + "🤖 " + "="*68)
    print("  Analysis Engine - Interactive Demo")
    print("="*70)
    
    while True:
        print("\nWhat would you like to demo?")
        print("1. Market Condition Analysis")
        print("2. Anomaly Detection")
        print("3. Intelligent Query System")
        print("4. Asset Performance Analysis")
        print("5. AI Recommendations")
        print("6. Anomaly Summary Report")
        print("7. Run all demos")
        print("0. Exit")
        
        try:
            choice = input("\nEnter your choice (0-7): ").strip()
            
            if choice == '0':
                print("\nGoodbye!")
                break
            elif choice == '1':
                demo_market_analysis()
            elif choice == '2':
                demo_anomaly_detection()
            elif choice == '3':
                demo_intelligent_queries()
            elif choice == '4':
                demo_asset_analysis()
            elif choice == '5':
                demo_recommendations()
            elif choice == '6':
                demo_summary()
            elif choice == '7':
                demo_market_analysis()
                demo_anomaly_detection()
                demo_intelligent_queries()
                demo_asset_analysis()
                demo_recommendations()
                demo_summary()
                print("\n✓ All demos complete!")
            else:
                print("Invalid choice. Please enter 0-7.")
        
        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}")

if __name__ == "__main__":
    main()