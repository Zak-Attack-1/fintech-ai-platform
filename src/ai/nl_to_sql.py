"""
Natural language to SQL conversion system
File: src/ai/nl_to_sql.py
"""
import sqlparse
from typing import Dict, Any, Optional, List
import re
from loguru import logger
from src.ai.hf_api import hf_api
from src.ai.local_models import local_models
from src.ai.schema_manager import schema_manager
from src.ai.config import config
import psycopg2
from datetime import datetime

class NaturalLanguageSQL:
    """Convert natural language queries to SQL and execute them"""
    
    def __init__(self):
        self.db_params = config.get_db_connection_params()
        
        # Query intent categories
        self.query_intents = [
            'data_retrieval',      # "Show me X"
            'aggregation',         # "Average/Sum of X"
            'comparison',          # "Compare X vs Y"
            'trend_analysis',      # "Over time", "trending"
            'anomaly_detection'    # "Unusual", "outliers"
        ]
        
        logger.info("NaturalLanguageSQL initialized")
    
    def process_query(self, natural_language_query: str, 
                     use_ai: bool = True) -> Dict[str, Any]:
        """
        Process natural language query and return results
        
        Args:
            natural_language_query: User's question in plain English
            use_ai: Whether to use AI for complex queries
            
        Returns:
            Dict with success status, SQL, results, and metadata
        """
        logger.info(f"Processing query: {natural_language_query}")
        
        start_time = datetime.now()
        
        # Step 1: Classify query intent
        intent_result = local_models.classify_query_intent(
            natural_language_query,
            self.query_intents
        )
        
        intent = intent_result['intent']
        confidence = intent_result['confidence']
        
        logger.debug(f"Intent: {intent} (confidence: {confidence:.2%})")
        
        # Step 2: Generate SQL
        if confidence > 0.7 or not use_ai:
            # Try template-based approach first (fast, no API)
            sql_query = self._generate_sql_template(natural_language_query, intent)
            method = 'template'
        else:
            sql_query = None
            method = 'attempted_template'
        
        # Fallback to AI if template fails
        if not sql_query and use_ai:
            schema_info = schema_manager.get_schema_for_sql_generation(natural_language_query)
            sql_query = hf_api.generate_sql_from_nl(natural_language_query, schema_info)
            method = 'ai_generated'
        
        if not sql_query:
            return {
                'success': False,
                'error': 'Could not generate SQL query',
                'suggestion': 'Try rephrasing your question or be more specific',
                'intent': intent,
                'confidence': confidence,
                'processing_time': (datetime.now() - start_time).total_seconds()
            }
        
        logger.debug(f"Generated SQL ({method}): {sql_query[:100]}...")
        
        # Step 3: Validate SQL
        is_valid, validation_error = self._validate_sql(sql_query)
        if not is_valid:
            return {
                'success': False,
                'error': f'SQL validation failed: {validation_error}',
                'sql': sql_query,
                'intent': intent,
                'method': method,
                'processing_time': (datetime.now() - start_time).total_seconds()
            }
        
        # Step 4: Execute query
        try:
            results = self._execute_query(sql_query)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'success': True,
                'sql': sql_query,
                'results': results,
                'row_count': len(results),
                'intent': intent,
                'confidence': confidence,
                'method': method,
                'processing_time': processing_time
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'sql': sql_query,
                'intent': intent,
                'method': method,
                'processing_time': (datetime.now() - start_time).total_seconds()
            }
    
    def _generate_sql_template(self, query: str, intent: str) -> Optional[str]:
        """Generate SQL using template-based approach for common queries"""
        
        query_lower = query.lower()
        
        # Data retrieval patterns
        if intent == 'data_retrieval' or 'show' in query_lower or 'list' in query_lower or 'get' in query_lower:
            
            # Stock queries
            if 'stock' in query_lower:
                if 'top' in query_lower or 'best' in query_lower or 'highest' in query_lower:
                    limit = self._extract_number(query, default=10)
                    if 'return' in query_lower or 'performance' in query_lower:
                        return f"""
                        SELECT asset_symbol, asset_name, total_return, sharpe_ratio, sector
                        FROM mart_asset_performance
                        WHERE asset_type = 'stock'
                        ORDER BY total_return DESC
                        LIMIT {limit}
                        """
                    elif 'volatil' in query_lower:
                        return f"""
                        SELECT ticker, company_name, volatility_20d, daily_return, sector
                        FROM int_stock_daily_analysis
                        WHERE date = (SELECT MAX(date) FROM int_stock_daily_analysis)
                        ORDER BY volatility_20d DESC
                        LIMIT {limit}
                        """
                    elif 'volume' in query_lower:
                        return f"""
                        SELECT ticker, company_name, volume, close_price, daily_return
                        FROM int_stock_daily_analysis
                        WHERE date = (SELECT MAX(date) FROM int_stock_daily_analysis)
                        ORDER BY volume DESC
                        LIMIT {limit}
                        """
                else:
                    return """
                    SELECT ticker, company_name, close_price, daily_return, sector
                    FROM int_stock_daily_analysis
                    WHERE date = (SELECT MAX(date) FROM int_stock_daily_analysis)
                    ORDER BY ticker
                    LIMIT 20
                    """
            
            # Crypto queries
            elif 'crypto' in query_lower or 'bitcoin' in query_lower or 'ethereum' in query_lower:
                if 'top' in query_lower or 'best' in query_lower:
                    limit = self._extract_number(query, default=10)
                    return f"""
                    SELECT symbol, name, price_usd, daily_return, volume_24h
                    FROM int_crypto_analysis
                    WHERE date = (SELECT MAX(date) FROM int_crypto_analysis)
                    ORDER BY daily_return DESC
                    LIMIT {limit}
                    """
                else:
                    return """
                    SELECT symbol, name, price_usd, daily_return, ma_7d, ma_30d
                    FROM int_crypto_analysis
                    WHERE date = (SELECT MAX(date) FROM int_crypto_analysis)
                    ORDER BY symbol
                    LIMIT 20
                    """
        
        # Aggregation patterns
        elif intent == 'aggregation' or 'average' in query_lower or 'avg' in query_lower or 'mean' in query_lower:
            
            if 'return' in query_lower:
                if 'sector' in query_lower:
                    return """
                    SELECT sector, 
                           AVG(total_return) as avg_return,
                           AVG(volatility) as avg_volatility,
                           COUNT(*) as num_assets
                    FROM mart_asset_performance
                    WHERE asset_type = 'stock' AND sector IS NOT NULL
                    GROUP BY sector
                    ORDER BY avg_return DESC
                    """
                else:
                    return """
                    SELECT asset_type,
                           AVG(total_return) as avg_return,
                           AVG(volatility) as avg_volatility,
                           COUNT(*) as num_assets
                    FROM mart_asset_performance
                    GROUP BY asset_type
                    ORDER BY avg_return DESC
                    """
            
            elif 'volatil' in query_lower:
                if 'sector' in query_lower:
                    return """
                    SELECT sector,
                           AVG(volatility) as avg_volatility,
                           MAX(volatility) as max_volatility,
                           MIN(volatility) as min_volatility
                    FROM mart_asset_performance
                    WHERE asset_type = 'stock' AND sector IS NOT NULL
                    GROUP BY sector
                    ORDER BY avg_volatility DESC
                    """
        
        # Comparison patterns
        elif intent == 'comparison' or 'compare' in query_lower or 'vs' in query_lower or 'versus' in query_lower:
            
            # Extract asset names
            assets = self._extract_assets(query_lower)
            
            if len(assets) >= 2:
                assets_list = "', '".join(assets)
                return f"""
                SELECT asset_symbol, asset_name, total_return, 
                       annualized_return, sharpe_ratio, volatility
                FROM mart_asset_performance
                WHERE LOWER(asset_symbol) IN ('{assets_list}')
                   OR LOWER(asset_name) IN ('{assets_list}')
                ORDER BY total_return DESC
                """
            else:
                # Generic comparison by asset type
                return """
                SELECT asset_type,
                       AVG(total_return) as avg_return,
                       AVG(sharpe_ratio) as avg_sharpe,
                       AVG(volatility) as avg_volatility
                FROM mart_asset_performance
                GROUP BY asset_type
                ORDER BY avg_return DESC
                """
        
        # Trend analysis
        elif intent == 'trend_analysis' or 'trend' in query_lower or 'over time' in query_lower:
            
            days = self._extract_number(query, default=30)
            
            if 'market' in query_lower:
                return f"""
                SELECT date, asset_class, avg_return, 
                       return_volatility, market_regime
                FROM mart_daily_market_summary
                WHERE date >= CURRENT_DATE - INTERVAL '{days} days'
                ORDER BY date DESC, asset_class
                """
        
        # Anomaly detection
        elif intent == 'anomaly_detection' or 'anomaly' in query_lower or 'unusual' in query_lower or 'outlier' in query_lower:
            
            return """
            SELECT asset_name, date, daily_return, volume,
                   return_z_score, anomaly_type
            FROM analytics_market_anomalies
            WHERE ABS(return_z_score) > 2
            ORDER BY date DESC, ABS(return_z_score) DESC
            LIMIT 20
            """
        
        return None
    
    def _extract_number(self, text: str, default: int = 10) -> int:
        """Extract number from text (e.g., 'top 5 stocks' -> 5)"""
        numbers = re.findall(r'\b\d+\b', text)
        return int(numbers[0]) if numbers else default
    
    def _extract_assets(self, text: str) -> List[str]:
        """Extract asset names from text"""
        # Common asset keywords
        assets = []
        
        asset_keywords = {
            'bitcoin': 'btc',
            'ethereum': 'eth',
            'apple': 'aapl',
            'microsoft': 'msft',
            'google': 'googl',
            'amazon': 'amzn',
            'tesla': 'tsla'
        }
        
        for keyword, symbol in asset_keywords.items():
            if keyword in text:
                assets.append(symbol)
        
        # Also look for ticker-like patterns (2-5 uppercase letters)
        tickers = re.findall(r'\b[A-Z]{2,5}\b', text.upper())
        assets.extend([t.lower() for t in tickers])
        
        return list(set(assets))  # Remove duplicates
    
    def _validate_sql(self, sql: str) -> tuple[bool, Optional[str]]:
        """
        Validate SQL query for safety and correctness
        
        Returns:
            (is_valid, error_message)
        """
        sql_lower = sql.lower().strip()
        
        # Disallow dangerous operations
        dangerous_keywords = ['drop', 'delete', 'truncate', 'insert', 'update', 'alter', 'create', 'grant', 'revoke']
        for keyword in dangerous_keywords:
            if re.search(rf'\b{keyword}\b', sql_lower):
                return False, f"Dangerous keyword '{keyword}' not allowed"
        
        # Must be a SELECT statement
        if not sql_lower.startswith('select'):
            return False, "Only SELECT queries are allowed"
        
        # Basic syntax validation
        try:
            parsed = sqlparse.parse(sql)
            if not parsed or len(parsed) == 0:
                return False, "Invalid SQL syntax"
        except Exception as e:
            return False, f"SQL parsing error: {str(e)}"
        
        # Check for LIMIT clause (add if missing)
        if 'limit' not in sql_lower:
            logger.debug("No LIMIT clause found, will add default")
        
        return True, None
    
    def _execute_query(self, sql: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Execute SQL query and return results"""
        
        # Add LIMIT if not present
        if 'limit' not in sql.lower():
            sql = f"{sql.rstrip(';')} LIMIT {limit}"
        
        conn = psycopg2.connect(**self.db_params)
        cursor = conn.cursor()
        
        try:
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                # Convert row to dict, handling special types
                row_dict = {}
                for col, val in zip(columns, row):
                    # Convert datetime/date to string
                    if hasattr(val, 'isoformat'):
                        row_dict[col] = val.isoformat()
                    # Convert Decimal to float
                    elif hasattr(val, '__float__'):
                        row_dict[col] = float(val)
                    else:
                        row_dict[col] = val
                
                results.append(row_dict)
            
            logger.info(f"Query returned {len(results)} rows")
            return results
            
        finally:
            cursor.close()
            conn.close()

# Global instance
nl_sql = NaturalLanguageSQL()