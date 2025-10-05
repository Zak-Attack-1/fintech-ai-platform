"""
Main AI analysis engine for financial insights
File: src/ai/analysis_engine.py
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd
from loguru import logger
from src.ai.hf_api import hf_api
from src.ai.local_models import local_models
from src.ai.vector_store import vector_store
from src.ai.nl_to_sql import nl_sql
from src.ai.anomaly_detector import anomaly_detector

class FinancialAnalysisEngine:
    """Main AI analysis engine for financial insights"""
    
    def __init__(self):
        self.analysis_cache = {}
        logger.info("FinancialAnalysisEngine initialized")
    
    def analyze_market_conditions(self, date: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze overall market conditions
        
        Args:
            date: Specific date to analyze (YYYY-MM-DD), or None for latest
            
        Returns:
            Market analysis with AI insights
        """
        logger.info(f"Analyzing market conditions for {date or 'latest date'}")
        
        # Build query
        if date:
            query = f"""
            SELECT date, asset_class, avg_return, return_volatility, 
                   market_regime, risk_sentiment
            FROM public_marts.mart_daily_market_summary
            WHERE date = '{date}'
            ORDER BY asset_class
            """
        else:
            query = """
            SELECT date, asset_class, avg_return, return_volatility, 
                   market_regime, risk_sentiment
            FROM public_marts.mart_daily_market_summary
            WHERE date = (SELECT MAX(date) FROM public_marts.mart_daily_market_summary)
            ORDER BY asset_class
            """
        
        try:
            market_data = nl_sql._execute_query(query)
            
            if not market_data:
                return {
                    'success': False,
                    'error': 'No market data available for this date'
                }
            
            # Extract key metrics
            analysis_date = market_data[0]['date']
            
            # Calculate aggregate metrics
            total_assets = len(market_data)
            avg_market_return = sum(float(d.get('avg_return', 0) or 0) for d in market_data) / total_assets
            avg_volatility = sum(float(d.get('return_volatility', 0) or 0) for d in market_data) / total_assets
            
            # Sentiment analysis using local model
            sentiments = []
            for asset_class_data in market_data:
                regime = asset_class_data.get('market_regime', 'neutral')
                sentiment_text = f"Market regime is {regime} with risk sentiment {asset_class_data.get('risk_sentiment', 'neutral')}"
                sentiment = local_models.analyze_sentiment(sentiment_text)
                sentiments.append({
                    'asset_class': asset_class_data['asset_class'],
                    'sentiment': sentiment
                })
            
            # Determine overall market mood
            positive_sentiments = sum(1 for s in sentiments if s['sentiment']['label'] == 'positive')
            overall_mood = 'bullish' if positive_sentiments > len(sentiments) / 2 else 'bearish' if positive_sentiments < len(sentiments) / 3 else 'neutral'
            
            analysis = {
                'success': True,
                'date': analysis_date,
                'overall_metrics': {
                    'avg_return': avg_market_return,
                    'avg_volatility': avg_volatility,
                    'market_mood': overall_mood
                },
                'by_asset_class': market_data,
                'sentiment_analysis': sentiments,
                'timestamp': datetime.now().isoformat()
            }
            
            # Add AI explanation if API available
            if hf_api._check_rate_limits()[0]:
                ai_analysis = hf_api.explain_financial_pattern({
                    'date': analysis_date,
                    'avg_return': f"{avg_market_return:.2%}",
                    'volatility': f"{avg_volatility:.2%}",
                    'mood': overall_mood,
                    'regimes': [d.get('market_regime') for d in market_data]
                })
                
                if ai_analysis:
                    analysis['ai_insights'] = ai_analysis
            
            return analysis
            
        except Exception as e:
            logger.error(f"Market analysis failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def detect_anomalies(self, days: int = 7) -> Dict[str, Any]:
        """
        Detect and analyze market anomalies
        
        Args:
            days: Number of days to look back
            
        Returns:
            Anomaly detection results with insights
        """
        logger.info(f"Detecting anomalies over last {days} days")
        
        anomalies = anomaly_detector.detect_recent_anomalies(days=days)
        
        if not anomalies['success']:
            return anomalies
        
        # Add insights for top anomalies
        if anomalies.get('top_anomalies'):
            for anomaly in anomalies['top_anomalies'][:3]:
                # Analyze each anomaly
                insight = anomaly_detector.analyze_anomaly(anomaly, use_ai=False)
                anomaly['analysis'] = insight
                
                # Compare to historical
                comparison = anomaly_detector.compare_to_historical(anomaly)
                anomaly['historical_comparison'] = comparison
        
        return anomalies
    
    def generate_insight(self, query: str, use_ai: bool = True) -> Dict[str, Any]:
        """
        Generate intelligent insight from natural language query
        
        Args:
            query: User's question in natural language
            use_ai: Whether to use AI for explanation
            
        Returns:
            Query results with AI-generated insights
        """
        logger.info(f"Generating insight for: {query}")
        
        # Process query using NL→SQL
        result = nl_sql.process_query(query, use_ai=use_ai)
        
        if not result['success']:
            return result
        
        # Generate summary statistics
        if result['results'] and len(result['results']) > 0:
            df = pd.DataFrame(result['results'])
            
            # Identify numeric columns
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            
            if len(numeric_cols) > 0:
                summary = {
                    'count': len(df),
                    'statistics': {}
                }
                
                for col in numeric_cols:
                    summary['statistics'][col] = {
                        'mean': float(df[col].mean()),
                        'median': float(df[col].median()),
                        'min': float(df[col].min()),
                        'max': float(df[col].max())
                    }
                
                result['summary_statistics'] = summary
        
        # Search for related patterns
        if result['results']:
            # Create search query from results
            search_query = f"Analysis of {query} showing patterns in financial data"
            related_patterns = vector_store.search_similar_patterns(
                search_query,
                n_results=3
            )
            
            if related_patterns:
                result['related_patterns'] = related_patterns
        
        # Generate AI insight for complex queries
        if use_ai and result['row_count'] > 0 and hf_api._check_rate_limits()[0]:
            insight_prompt = {
                'query': query,
                'row_count': result['row_count'],
                'summary': result.get('summary_statistics', {})
            }
            
            ai_insight = hf_api.explain_financial_pattern(insight_prompt)
            if ai_insight:
                result['ai_insight'] = ai_insight
        else:
            # Provide simple insight using local analysis
            result['insight'] = self._generate_simple_insight(result)
        
        return result
    
    def _generate_simple_insight(self, result: Dict[str, Any]) -> str:
        """Generate simple insight without AI"""
        row_count = result.get('row_count', 0)
        intent = result.get('intent', 'unknown')
        
        if row_count == 0:
            return "No data found matching your query criteria."
        
        if intent == 'comparison':
            return f"Found {row_count} items for comparison. Review the metrics to identify differences."
        elif intent == 'aggregation':
            return f"Aggregated data across {row_count} categories. Check the averages and totals."
        elif intent == 'data_retrieval':
            return f"Retrieved {row_count} records matching your criteria."
        else:
            return f"Query returned {row_count} results."
    
    def analyze_asset_performance(self, asset_symbol: str) -> Dict[str, Any]:
        """
        Comprehensive analysis of a specific asset
        
        Args:
            asset_symbol: Asset ticker/symbol
            
        Returns:
            Detailed asset analysis
        """
        logger.info(f"Analyzing performance for {asset_symbol}")
        
        # Get asset performance data
        query = f"""
        SELECT asset_symbol, asset_name, asset_type, sector,
               current_price, total_return, annualized_return, 
               annualized_volatility, sharpe_ratio, max_drawdown,
               risk_return_profile, dominant_ma_signal, dominant_rsi_signal
        FROM public_marts.mart_asset_performance
        WHERE LOWER(asset_symbol) = LOWER('{asset_symbol}')
        """
        
        try:
            results = nl_sql._execute_query(query)
            
            if not results:
                return {
                    'success': False,
                    'error': f'Asset {asset_symbol} not found'
                }
            
            asset_data = results[0]
            
            # Sentiment analysis of profile
            profile = asset_data.get('risk_return_profile', 'unknown')
            ma_signal = asset_data.get('dominant_ma_signal', 'neutral')
            rsi_signal = asset_data.get('dominant_rsi_signal', 'neutral')
            
            sentiment_text = f"Asset shows {profile} profile with {ma_signal} trend and {rsi_signal} momentum"
            sentiment = local_models.analyze_sentiment(sentiment_text)
            
            # Search for similar assets
            search_query = f"Asset with {profile} characteristics and {float(asset_data.get('total_return', 0)):.2%} return"
            similar = vector_store.search_similar_patterns(search_query, n_results=3)
            
            analysis = {
                'success': True,
                'asset': asset_data,
                'sentiment': sentiment,
                'technical_summary': {
                    'trend': ma_signal,
                    'momentum': rsi_signal,
                    'risk_profile': profile
                },
                'similar_patterns': similar
            }
            
            # Risk assessment
            volatility = float(asset_data.get('annualized_volatility', 0) or 0)
            sharpe = float(asset_data.get('sharpe_ratio', 0) or 0)
            
            if volatility > 0.5:
                risk_level = 'High'
            elif volatility > 0.3:
                risk_level = 'Medium'
            else:
                risk_level = 'Low'
            
            analysis['risk_assessment'] = {
                'level': risk_level,
                'volatility': volatility,
                'risk_adjusted_return': sharpe
            }
            
            return analysis
            
        except Exception as e:
            logger.error(f"Asset analysis failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_recommendations(self, context: str = 'general') -> List[str]:
        """
        Get AI-powered recommendations
        
        Args:
            context: Context for recommendations (general/risk/growth/etc)
            
        Returns:
            List of recommendations
        """
        # Get recent market conditions
        market_analysis = self.analyze_market_conditions()
        
        recommendations = []
        
        if market_analysis['success']:
            mood = market_analysis['overall_metrics'].get('market_mood', 'neutral')
            volatility = market_analysis['overall_metrics'].get('avg_volatility', 0)
            
            # Basic rule-based recommendations
            if mood == 'bearish':
                recommendations.append("Consider defensive positions in current bearish environment")
                recommendations.append("Review portfolio risk exposure and consider hedging strategies")
            elif mood == 'bullish':
                recommendations.append("Favorable conditions for growth-oriented positions")
                recommendations.append("Monitor for overbought conditions in high-momentum assets")
            
            if volatility > 0.3:
                recommendations.append("High volatility detected - consider reducing position sizes")
            
            recommendations.append("Maintain diversification across asset classes")
        
        return recommendations

# Global instance
analysis_engine = FinancialAnalysisEngine()