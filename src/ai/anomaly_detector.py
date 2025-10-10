"""
Automated anomaly detection system
File: src/ai/anomaly_detector.py
"""
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from loguru import logger
from src.ai.nl_to_sql import nl_sql
from src.ai.vector_store import vector_store
from src.ai.hf_api import hf_api
import pandas as pd

class AnomalyDetector:
    """Detect and analyze market anomalies"""
    
    def __init__(self):
        self.anomaly_threshold = 2.0  # Z-score threshold
        logger.info("AnomalyDetector initialized")
    
    def detect_recent_anomalies(self, days: int = 7, 
                                min_severity: str = 'medium') -> Dict[str, Any]:
        """
        Detect anomalies from the database
        
        Args:
            days: Number of days to look back
            min_severity: Minimum severity (low/medium/high/critical)
            
        Returns:
            Dict with anomalies and analysis
        """
        logger.info(f"Detecting anomalies from last {days} days")
        
        # Query anomalies from database
        # Note: Column names may vary - adjust based on your actual schema
        query = f"""
        SELECT asset_name, asset_id, date, daily_return, anomaly_type,
               anomaly_score, price_change_pct
        FROM public_analytics.analytics_market_anomalies
        WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
          AND ABS(anomaly_score) > {self.anomaly_threshold}
        ORDER BY date DESC, ABS(anomaly_score) DESC
        LIMIT 50
        """
        
        try:
            result = nl_sql._execute_query(query)
            
            if not result:
                return {
                    'success': True,
                    'anomalies_found': 0,
                    'message': f'No significant anomalies detected in last {days} days'
                }
            
            # Classify by severity
            classified = self._classify_anomalies(result)
            
            # Find similar historical patterns for top anomalies
            enriched = self._enrich_with_patterns(classified[:10])
            
            return {
                'success': True,
                'anomalies_found': len(result),
                'period_days': days,
                'by_severity': {
                    'critical': len([a for a in classified if a['severity'] == 'critical']),
                    'high': len([a for a in classified if a['severity'] == 'high']),
                    'medium': len([a for a in classified if a['severity'] == 'medium']),
                    'low': len([a for a in classified if a['severity'] == 'low'])
                },
                'top_anomalies': enriched,
                'all_anomalies': classified
            }
            
        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _classify_anomalies(self, anomalies: List[Dict]) -> List[Dict]:
        """Classify anomalies by severity"""
        classified = []
        
        for anomaly in anomalies:
            # Use anomaly_score if available, otherwise fall back to daily_return
            z_score = abs(float(anomaly.get('anomaly_score', 0)))
            if z_score == 0:
                # Fallback: estimate from daily_return
                daily_return = abs(float(anomaly.get('daily_return', 0)))
                z_score = daily_return * 100  # Rough approximation
            
            # Determine severity
            if z_score >= 4.0:
                severity = 'critical'
            elif z_score >= 3.0:
                severity = 'high'
            elif z_score >= 2.5:
                severity = 'medium'
            else:
                severity = 'low'
            
            # Add classification
            anomaly['severity'] = severity
            anomaly['z_score_abs'] = z_score
            
            classified.append(anomaly)
        
        return classified
    
    def _enrich_with_patterns(self, anomalies: List[Dict]) -> List[Dict]:
        """Enrich anomalies with similar historical patterns"""
        enriched = []
        
        for anomaly in anomalies:
            # Create description for vector search
            asset = anomaly.get('asset_name', 'Unknown')
            return_val = float(anomaly.get('daily_return', 0))
            z_score = anomaly.get('z_score_abs', 0)
            
            description = f"{asset} experienced {return_val:.2%} return with z-score {z_score:.2f}"
            
            # Search for similar patterns
            similar = vector_store.search_similar_anomalies(
                description,
                n_results=3,
                min_severity='medium'
            )
            
            anomaly['similar_patterns'] = similar
            enriched.append(anomaly)
        
        return enriched
    
    def analyze_anomaly(self, anomaly: Dict[str, Any], 
                       use_ai: bool = True) -> Dict[str, Any]:
        """
        Generate detailed analysis of a specific anomaly
        
        Args:
            anomaly: Anomaly data dict
            use_ai: Whether to use AI for explanation
            
        Returns:
            Analysis with explanation and recommendations
        """
        asset = anomaly.get('asset_name', 'Unknown')
        return_val = float(anomaly.get('daily_return', 0))
        z_score = float(anomaly.get('return_z_score', 0))
        date = anomaly.get('date', 'Unknown')
        
        analysis = {
            'asset': asset,
            'date': date,
            'return': return_val,
            'z_score': z_score,
            'severity': anomaly.get('severity', 'medium'),
            'type': anomaly.get('anomaly_type', 'unknown')
        }
        
        # Statistical context
        if abs(z_score) >= 3:
            analysis['probability'] = 'Less than 0.3% (very rare)'
        elif abs(z_score) >= 2.5:
            analysis['probability'] = 'About 1% (rare)'
        else:
            analysis['probability'] = 'About 5% (unusual)'
        
        # Direction
        analysis['direction'] = 'positive' if return_val > 0 else 'negative'
        
        # AI explanation (if available and requested)
        if use_ai and hf_api._check_rate_limits()[0]:
            pattern_data = {
                'asset': asset,
                'return': f"{return_val:.2%}",
                'z_score': f"{z_score:.2f}",
                'date': date,
                'severity': analysis['severity']
            }
            
            explanation = hf_api.explain_financial_pattern(pattern_data)
            if explanation:
                analysis['ai_explanation'] = explanation
        
        return analysis
    
    def get_anomaly_summary(self, days: int = 7) -> str:
        """
        Get text summary of recent anomalies
        
        Args:
            days: Number of days to analyze
            
        Returns:
            Human-readable summary
        """
        result = self.detect_recent_anomalies(days)
        
        if not result['success']:
            return f"Error detecting anomalies: {result.get('error')}"
        
        if result['anomalies_found'] == 0:
            return f"No significant anomalies detected in the last {days} days."
        
        summary = f"Anomaly Report - Last {days} Days\n"
        summary += "=" * 50 + "\n\n"
        
        summary += f"Total Anomalies: {result['anomalies_found']}\n"
        summary += f"By Severity:\n"
        for severity, count in result['by_severity'].items():
            if count > 0:
                summary += f"  - {severity.upper()}: {count}\n"
        
        summary += f"\nTop Anomalies:\n"
        for i, anomaly in enumerate(result['top_anomalies'][:5], 1):
            asset = anomaly.get('asset_name', 'Unknown')
            return_val = float(anomaly.get('daily_return', 0))
            severity = anomaly.get('severity', 'unknown')
            date = anomaly.get('date', 'Unknown')
            
            summary += f"\n{i}. {asset} ({date})\n"
            summary += f"   Return: {return_val:.2%} | Severity: {severity}\n"
            
            if anomaly.get('similar_patterns'):
                summary += f"   Similar to: {anomaly['similar_patterns'][0]['id']}\n"
        
        return summary
    
    def compare_to_historical(self, current_anomaly: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare current anomaly to historical patterns
        
        Args:
            current_anomaly: Current anomaly data
            
        Returns:
            Comparison analysis
        """
        asset = current_anomaly.get('asset_name', 'Unknown')
        return_val = float(current_anomaly.get('daily_return', 0))
        z_score = abs(float(current_anomaly.get('return_z_score', 0)))
        
        # Search historical patterns
        description = f"Significant price movement of {return_val:.2%} with high volatility"
        similar = vector_store.search_similar_anomalies(description, n_results=5)
        
        if not similar:
            return {
                'has_historical_match': False,
                'message': 'No similar historical patterns found'
            }
        
        # Find best match
        best_match = similar[0]
        similarity = best_match.get('similarity', 0)
        
        comparison = {
            'has_historical_match': True,
            'similarity_score': similarity,
            'best_match': {
                'event': best_match['id'],
                'description': best_match['description'],
                'date': best_match['metadata'].get('date', 'Unknown'),
                'severity': best_match['metadata'].get('severity', 'Unknown')
            },
            'all_matches': similar
        }
        
        # Categorize similarity
        if similarity > 0.8:
            comparison['match_quality'] = 'Very similar'
        elif similarity > 0.6:
            comparison['match_quality'] = 'Moderately similar'
        else:
            comparison['match_quality'] = 'Somewhat similar'
        
        return comparison

# Global instance
anomaly_detector = AnomalyDetector()