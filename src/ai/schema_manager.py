"""
Database schema information manager
File: src/ai/schema_manager.py

Provides schema information for SQL generation
"""
from typing import Dict, List, Any
from loguru import logger

class SchemaManager:
    """Manages database schema information for AI queries"""
    
    def __init__(self):
        self.schema_info = self._build_schema_info()
        logger.info("SchemaManager initialized with dbt warehouse schema")
    
    def _build_schema_info(self) -> Dict[str, Any]:
        """Build comprehensive schema information"""
        return {
            'staging': {
                'stg_stocks': {
                    'description': 'Staging table for raw stock data',
                    'columns': {
                        'ticker': 'Stock ticker symbol',
                        'date': 'Trading date',
                        'open': 'Opening price',
                        'high': 'High price',
                        'low': 'Low price',
                        'close': 'Closing price',
                        'volume': 'Trading volume',
                        'company_name': 'Company name',
                        'sector': 'Business sector',
                        'industry': 'Industry classification'
                    }
                },
                'stg_economic_indicators': {
                    'description': 'Staging table for economic indicators',
                    'columns': {
                        'series_id': 'FRED series ID',
                        'date': 'Observation date',
                        'value': 'Indicator value',
                        'series_title': 'Indicator name',
                        'units': 'Measurement units',
                        'frequency': 'Data frequency'
                    }
                },
                'stg_crypto': {
                    'description': 'Staging table for cryptocurrency data',
                    'columns': {
                        'symbol': 'Crypto symbol',
                        'name': 'Cryptocurrency name',
                        'date': 'Date',
                        'price_usd': 'Price in USD',
                        'volume_24h': '24-hour trading volume',
                        'market_cap': 'Market capitalization',
                        'category': 'Crypto category'
                    }
                }
            },
            'intermediate': {
                'public_intermediate.int_stock_daily_analysis': {
                    'description': 'Daily stock analysis with technical indicators',
                    'columns': {
                        'ticker': 'Stock ticker',
                        'date': 'Trading date',
                        'close_price': 'Closing price',
                        'volume': 'Trading volume',
                        'daily_return': 'Daily percentage return',
                        'sma_short': 'Short-term moving average (20 days)',
                        'sma_long': 'Long-term moving average (50 days)',
                        'rsi_14d': 'Relative Strength Index (14 days)',
                        'volatility_20d': '20-day volatility',
                        'company_name': 'Company name',
                        'sector': 'Business sector',
                        'market_cap_category': 'Market cap size (Large/Mid/Small)'
                    },
                    'common_filters': ['sector', 'market_cap_category'],
                    'common_sorts': ['daily_return', 'volatility_20d', 'volume']
                },
                'int_economic_analysis': {
                    'description': 'Economic indicators with calculations',
                    'columns': {
                        'series_id': 'FRED series ID',
                        'date': 'Date',
                        'value': 'Indicator value',
                        'percentage_change': 'Period-over-period change',
                        'year_over_year_change': 'Year-over-year change',
                        'series_title': 'Indicator name',
                        'indicator_category': 'Category (GDP, Inflation, Employment, etc.)',
                        'units': 'Units'
                    },
                    'common_filters': ['indicator_category'],
                    'common_sorts': ['date', 'percentage_change']
                },
                'int_crypto_analysis': {
                    'description': 'Cryptocurrency analysis with metrics',
                    'columns': {
                        'symbol': 'Crypto symbol',
                        'name': 'Name',
                        'date': 'Date',
                        'price_usd': 'Price in USD',
                        'volume_24h': '24-hour volume',
                        'daily_return': 'Daily return',
                        'ma_7d': '7-day moving average',
                        'ma_30d': '30-day moving average',
                        'volatility_30d': '30-day volatility',
                        'crypto_category': 'Category (DeFi, Layer1, etc.)'
                    },
                    'common_filters': ['crypto_category'],
                    'common_sorts': ['daily_return', 'volume_24h', 'price_usd']
                }
            },
            'marts': {
                'mart_daily_market_summary': {
                    'description': 'Daily market summary by asset class',
                    'columns': {
                        'date': 'Date',
                        'asset_class': 'Asset class (stock/crypto/economic)',
                        'avg_return': 'Average return',
                        'return_volatility': 'Return volatility',
                        'total_volume': 'Total trading volume',
                        'market_regime': 'Market regime (bull/bear/neutral)',
                        'risk_sentiment': 'Risk sentiment score'
                    },
                    'common_filters': ['asset_class', 'market_regime'],
                    'common_sorts': ['date', 'avg_return']
                },
                'mart_asset_performance': {
                    'description': 'Asset performance metrics',
                    'columns': {
                        'asset_symbol': 'Asset symbol/ticker',
                        'asset_type': 'Type (stock/crypto)',
                        'asset_name': 'Asset name',
                        'total_return': 'Total return',
                        'annualized_return': 'Annualized return',
                        'volatility': 'Volatility',
                        'sharpe_ratio': 'Sharpe ratio',
                        'max_drawdown': 'Maximum drawdown',
                        'beta': 'Market beta',
                        'sector': 'Sector (for stocks)',
                        'performance_category': 'Performance tier'
                    },
                    'common_filters': ['asset_type', 'sector', 'performance_category'],
                    'common_sorts': ['total_return', 'sharpe_ratio', 'volatility']
                },
                'analytics_cross_asset_correlations': {
                    'description': 'Cross-asset correlation matrix',
                    'columns': {
                        'asset_1': 'First asset',
                        'asset_2': 'Second asset',
                        'correlation_coefficient': 'Correlation (-1 to 1)',
                        'relationship_type': 'Type (positive/negative/none)',
                        'correlation_strength': 'Strength (strong/moderate/weak)'
                    },
                    'common_filters': ['relationship_type', 'correlation_strength'],
                    'common_sorts': ['correlation_coefficient']
                },
                'analytics_market_anomalies': {
                    'description': 'Detected market anomalies',
                    'columns': {
                        'asset_id': 'Asset identifier',
                        'asset_name': 'Asset name',
                        'date': 'Date',
                        'daily_return': 'Return',
                        'volume': 'Volume',
                        'return_z_score': 'Z-score for return',
                        'volume_z_score': 'Z-score for volume',
                        'anomaly_type': 'Type of anomaly'
                    },
                    'common_filters': ['anomaly_type'],
                    'common_sorts': ['date', 'return_z_score']
                }
            }
        }
    
    def get_table_description(self, table_name: str) -> str:
        """Get formatted table description for SQL generation"""
        
        # Search through all layers
        for layer_name, tables in self.schema_info.items():
            if table_name in tables:
                table_info = tables[table_name]
                
                description = f"Table: {table_name}\n"
                description += f"Description: {table_info['description']}\n"
                description += "Columns:\n"
                
                for col, desc in table_info['columns'].items():
                    description += f"  - {col}: {desc}\n"
                
                if 'common_filters' in table_info:
                    description += f"Common filters: {', '.join(table_info['common_filters'])}\n"
                
                if 'common_sorts' in table_info:
                    description += f"Common sorts: {', '.join(table_info['common_sorts'])}\n"
                
                return description
        
        return f"Table {table_name} not found"
    
    def get_all_tables(self) -> List[str]:
        """Get list of all table names"""
        tables = []
        for layer_name, layer_tables in self.schema_info.items():
            tables.extend(layer_tables.keys())
        return tables
    
    def get_relevant_tables(self, query: str) -> List[str]:
        """
        Determine relevant tables based on query keywords
        
        Args:
            query: Natural language query
            
        Returns:
            List of relevant table names
        """
        query_lower = query.lower()
        relevant_tables = []
        
        # Keywords to table mapping
        keyword_mapping = {
            'stock': ['int_stock_daily_analysis', 'mart_asset_performance'],
            'crypto': ['int_crypto_analysis', 'mart_asset_performance'],
            'economic': ['int_economic_analysis'],
            'market': ['mart_daily_market_summary'],
            'correlation': ['analytics_cross_asset_correlations'],
            'anomaly': ['analytics_market_anomalies'],
            'performance': ['mart_asset_performance'],
            'volatility': ['int_stock_daily_analysis', 'int_crypto_analysis'],
            'return': ['mart_asset_performance', 'int_stock_daily_analysis']
        }
        
        for keyword, tables in keyword_mapping.items():
            if keyword in query_lower:
                relevant_tables.extend(tables)
        
        # Remove duplicates while preserving order
        relevant_tables = list(dict.fromkeys(relevant_tables))
        
        # Default to most commonly used tables if none found
        if not relevant_tables:
            relevant_tables = [
                'mart_asset_performance',
                'int_stock_daily_analysis',
                'mart_daily_market_summary'
            ]
        
        return relevant_tables
    
    def get_schema_for_sql_generation(self, query: str) -> str:
        """
        Get formatted schema information for SQL generation
        
        Args:
            query: Natural language query
            
        Returns:
            Formatted schema string for prompt
        """
        relevant_tables = self.get_relevant_tables(query)
        
        schema_text = "Available PostgreSQL Tables:\n\n"
        
        for table_name in relevant_tables[:3]:  # Limit to top 3 most relevant
            schema_text += self.get_table_description(table_name)
            schema_text += "\n"
        
        schema_text += "\nNotes:\n"
        schema_text += "- Use proper PostgreSQL syntax\n"
        schema_text += "- Always include LIMIT clause (max 100 rows)\n"
        schema_text += "- Use meaningful column aliases\n"
        schema_text += "- Date format: 'YYYY-MM-DD'\n"
        
        return schema_text

# Global instance
schema_manager = SchemaManager()