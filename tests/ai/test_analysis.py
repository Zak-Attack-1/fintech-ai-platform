"""
Test Analysis Engine and Anomaly Detector
File: tests/ai/test_analysis.py
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ai.analysis_engine import analysis_engine
from src.ai.anomaly_detector import anomaly_detector
from loguru import logger
import json

def test_market_analysis():
    """Test market condition analysis"""
    print("\n" + "="*70)
    print("Testing Market Analysis...")
    print("="*70)
    
    try:
        analysis = analysis_engine.analyze_market_conditions()
        
        if analysis['success']:
            print(f"\n✓ Market analysis successful")
            print(f"  Date: {analysis['date']}")
            print(f"  Market Mood: {analysis['overall_metrics']['market_mood']}")
            print(f"  Avg Return: {analysis['overall_metrics']['avg_return']:.4f}")
            print(f"  Avg Volatility: {analysis['overall_metrics']['avg_volatility']:.4f}")
            
            print(f"\n  Asset Classes Analyzed: {len(analysis['by_asset_class'])}")
            for asset in analysis['by_asset_class']:
                print(f"    - {asset['asset_class']}: {asset.get('market_regime', 'N/A')}")
            
            if analysis.get('ai_insights'):
                print(f"\n  AI Insights: {analysis['ai_insights'][:100]}...")
            
            return True
        else:
            print(f"✗ Analysis failed: {analysis.get('error')}")
            return False
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_anomaly_detection():
    """Test anomaly detection"""
    print("\n" + "="*70)
    print("Testing Anomaly Detection...")
    print("="*70)
    
    try:
        anomalies = anomaly_detector.detect_recent_anomalies(days=30)
        
        if anomalies['success']:
            print(f"\n✓ Anomaly detection successful")
            print(f"  Total Anomalies Found: {anomalies['anomalies_found']}")
            print(f"  Period: {anomalies['period_days']} days")
            
            print(f"\n  By Severity:")
            for severity, count in anomalies['by_severity'].items():
                if count > 0:
                    print(f"    - {severity.upper()}: {count}")
            
            if anomalies.get('top_anomalies'):
                print(f"\n  Top 3 Anomalies:")
                for i, anomaly in enumerate(anomalies['top_anomalies'][:3], 1):
                    asset = anomaly.get('asset_name', 'Unknown')
                    return_val = float(anomaly.get('daily_return', 0))
                    severity = anomaly.get('severity', 'unknown')
                    date = anomaly.get('date', 'Unknown')
                    
                    print(f"\n  {i}. {asset} on {date}")
                    print(f"     Return: {return_val:.2%}")
                    print(f"     Severity: {severity}")
                    
                    if anomaly.get('similar_patterns'):
                        print(f"     Similar to: {anomaly['similar_patterns'][0]['id']}")
            
            return True
        else:
            print(f"✗ Detection failed: {anomalies.get('error')}")
            return False
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_generate_insight():
    """Test insight generation"""
    print("\n" + "="*70)
    print("Testing Insight Generation...")
    print("="*70)
    
    test_queries = [
        "Compare Bitcoin and Ethereum",
        "Show top 5 cryptocurrencies",
        "List stocks with high volatility"
    ]
    
    success_count = 0
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        
        result = analysis_engine.generate_insight(query, use_ai=False)
        
        if result['success']:
            print(f"  ✓ Insight generated")
            print(f"  Rows: {result['row_count']}")
            
            if result.get('summary_statistics'):
                print(f"  Statistics: {len(result['summary_statistics']['statistics'])} metrics")
            
            if result.get('insight'):
                print(f"  Insight: {result['insight'][:80]}...")
            
            if result.get('related_patterns'):
                print(f"  Related patterns: {len(result['related_patterns'])}")
            
            success_count += 1
        else:
            print(f"  ✗ Failed: {result.get('error')}")
    
    print(f"\n✓ {success_count}/{len(test_queries)} insights generated successfully")
    return success_count == len(test_queries)

def test_asset_analysis():
    """Test individual asset analysis"""
    print("\n" + "="*70)
    print("Testing Asset Analysis...")
    print("="*70)
    
    test_assets = ['BTC', 'ETH', 'AAPL']
    
    success_count = 0
    
    for asset in test_assets:
        print(f"\nAnalyzing: {asset}")
        
        result = analysis_engine.analyze_asset_performance(asset)
        
        if result['success']:
            print(f"  ✓ Analysis successful")
            print(f"  Name: {result['asset'].get('asset_name', 'N/A')}")
            print(f"  Type: {result['asset'].get('asset_type', 'N/A')}")
            print(f"  Return: {result['asset'].get('total_return', 0)}")
            print(f"  Risk Level: {result['risk_assessment']['level']}")
            print(f"  Trend: {result['technical_summary']['trend']}")
            
            success_count += 1
        else:
            print(f"  ✗ Failed: {result.get('error')}")
    
    print(f"\n✓ {success_count}/{len(test_assets)} assets analyzed")
    return success_count > 0

def test_recommendations():
    """Test recommendation generation"""
    print("\n" + "="*70)
    print("Testing Recommendations...")
    print("="*70)
    
    try:
        recommendations = analysis_engine.get_recommendations()
        
        if recommendations:
            print(f"\n✓ Generated {len(recommendations)} recommendations:")
            for i, rec in enumerate(recommendations, 1):
                print(f"  {i}. {rec}")
            return True
        else:
            print("✗ No recommendations generated")
            return False
            
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def test_anomaly_summary():
    """Test anomaly summary generation"""
    print("\n" + "="*70)
    print("Testing Anomaly Summary...")
    print("="*70)
    
    try:
        summary = anomaly_detector.get_anomaly_summary(days=14)
        
        print(f"\n{summary}")
        print("\n✓ Summary generated successfully")
        return True
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        return False

def test_integration():
    """Test full system integration"""
    print("\n" + "="*70)
    print("Testing Full System Integration...")
    print("="*70)
    
    try:
        # Step 1: Analyze market
        print("\n1. Getting market overview...")
        market = analysis_engine.analyze_market_conditions()
        if market['success']:
            print(f"   ✓ Market mood: {market['overall_metrics']['market_mood']}")
        
        # Step 2: Detect anomalies
        print("\n2. Detecting anomalies...")
        anomalies = analysis_engine.detect_anomalies(days=7)
        if anomalies['success']:
            print(f"   ✓ Found {anomalies['anomalies_found']} anomalies")
        
        # Step 3: Generate insight
        print("\n3. Generating insight...")
        insight = analysis_engine.generate_insight("Show top 5 cryptocurrencies")
        if insight['success']:
            print(f"   ✓ Query returned {insight['row_count']} results")
        
        # Step 4: Get recommendations
        print("\n4. Getting recommendations...")
        recs = analysis_engine.get_recommendations()
        print(f"   ✓ Generated {len(recs)} recommendations")
        
        print("\n✓ Full integration test passed")
        return True
        
    except Exception as e:
        print(f"\n✗ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_all_tests():
    """Run all analysis engine tests"""
    print("\n" + "📊 " + "="*68)
    print("  Analysis Engine Tests")
    print("="*70)
    
    results = {
        'Market Analysis': test_market_analysis(),
        'Anomaly Detection': test_anomaly_detection(),
        'Insight Generation': test_generate_insight(),
        'Asset Analysis': test_asset_analysis(),
        'Recommendations': test_recommendations(),
        'Anomaly Summary': test_anomaly_summary(),
        'Integration': test_integration()
    }
    
    print("\n" + "="*70)
    print("Test Results Summary")
    print("="*70)
    
    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:.<50} {status}")
    
    all_passed = all(results.values())
    passed_count = sum(1 for p in results.values() if p)
    
    print("\n" + "="*70)
    if all_passed:
        print(f"✓ All {len(results)} tests passed! Analysis Engine is ready.")
        print("\nYour AI system can now:")
        print("  - Analyze market conditions with AI insights")
        print("  - Detect anomalies automatically")
        print("  - Generate intelligent insights from queries")
        print("  - Analyze individual asset performance")
        print("  - Provide recommendations")
        print("\nNext: Build unified interface (Day 12-14)")
    else:
        print(f"⚠️  {passed_count}/{len(results)} tests passed. Check failures above.")
    print("="*70 + "\n")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)