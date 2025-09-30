"""
Vector database for financial pattern storage and retrieval
File: src/ai/vector_store.py
"""
import chromadb
from chromadb.config import Settings
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
from loguru import logger
from src.ai.local_models import local_models
from src.ai.config import config
import os

class FinancialVectorStore:
    """Vector database for financial patterns, anomalies, and historical events"""
    
    def __init__(self):
        logger.info("Initializing vector store...")
        
        # Ensure directory exists
        os.makedirs(config.chroma_persist_dir, exist_ok=True)
        
        # Initialize ChromaDB with persistence
        self.client = chromadb.PersistentClient(
            path=config.chroma_persist_dir
        )
        
        # Create or get collections
        self.patterns_collection = self.client.get_or_create_collection(
            name="financial_patterns",
            metadata={"description": "Market patterns and technical signals"}
        )
        
        self.anomalies_collection = self.client.get_or_create_collection(
            name="market_anomalies",
            metadata={"description": "Historical market anomalies and crashes"}
        )
        
        self.events_collection = self.client.get_or_create_collection(
            name="market_events",
            metadata={"description": "Major market events and news"}
        )
        
        logger.info(f"Vector store initialized at {config.chroma_persist_dir}")
        logger.info(f"Collections: {self.client.list_collections()}")
    
    def add_market_pattern(self, pattern_id: str, pattern_description: str,
                          pattern_type: str, metadata: Dict[str, Any]):
        """
        Add market pattern to vector store
        
        Args:
            pattern_id: Unique identifier for pattern
            pattern_description: Text description of the pattern
            pattern_type: Type of pattern (e.g., 'technical_signal', 'price_pattern')
            metadata: Additional metadata (severity, asset_type, etc.)
        """
        try:
            # Generate embedding
            embedding = local_models.generate_embeddings([pattern_description])[0]
            
            # Prepare metadata
            full_metadata = {
                'pattern_type': pattern_type,
                'timestamp': datetime.now().isoformat(),
                **metadata
            }
            
            # Store pattern
            self.patterns_collection.add(
                embeddings=[embedding.tolist()],
                documents=[pattern_description],
                metadatas=[full_metadata],
                ids=[pattern_id]
            )
            
            logger.debug(f"Added pattern {pattern_id} to vector store")
            
        except Exception as e:
            logger.error(f"Failed to add pattern {pattern_id}: {e}")
    
    def add_anomaly(self, anomaly_id: str, anomaly_description: str,
                   severity: str, metadata: Dict[str, Any]):
        """
        Add market anomaly to vector store
        
        Args:
            anomaly_id: Unique identifier
            anomaly_description: Text description
            severity: 'low', 'medium', 'high', 'critical'
            metadata: Additional context
        """
        try:
            embedding = local_models.generate_embeddings([anomaly_description])[0]
            
            full_metadata = {
                'severity': severity,
                'detected_at': datetime.now().isoformat(),
                **metadata
            }
            
            self.anomalies_collection.add(
                embeddings=[embedding.tolist()],
                documents=[anomaly_description],
                metadatas=[full_metadata],
                ids=[anomaly_id]
            )
            
            logger.debug(f"Added anomaly {anomaly_id} with severity {severity}")
            
        except Exception as e:
            logger.error(f"Failed to add anomaly: {e}")
    
    def add_market_event(self, event_id: str, event_description: str,
                        event_date: str, metadata: Dict[str, Any]):
        """
        Add historical market event
        
        Args:
            event_id: Unique identifier
            event_description: Event description
            event_date: Date of event (YYYY-MM-DD)
            metadata: Additional context
        """
        try:
            embedding = local_models.generate_embeddings([event_description])[0]
            
            full_metadata = {
                'event_date': event_date,
                'indexed_at': datetime.now().isoformat(),
                **metadata
            }
            
            self.events_collection.add(
                embeddings=[embedding.tolist()],
                documents=[event_description],
                metadatas=[full_metadata],
                ids=[event_id]
            )
            
            logger.debug(f"Added market event {event_id}")
            
        except Exception as e:
            logger.error(f"Failed to add event: {e}")
    
    def search_similar_patterns(self, query: str, n_results: int = 5,
                               pattern_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for similar patterns using semantic similarity
        
        Args:
            query: Query text
            n_results: Number of results to return
            pattern_type: Filter by pattern type
            
        Returns:
            List of similar patterns with metadata
        """
        try:
            # Generate query embedding
            query_embedding = local_models.generate_embeddings([query])[0]
            
            # Build where clause for filtering
            where_clause = {'pattern_type': pattern_type} if pattern_type else None
            
            # Search
            results = self.patterns_collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=n_results,
                where=where_clause
            )
            
            # Format results
            return self._format_results(results)
            
        except Exception as e:
            logger.error(f"Pattern search failed: {e}")
            return []
    
    def search_similar_anomalies(self, query: str, n_results: int = 5,
                                 min_severity: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for similar historical anomalies
        
        Args:
            query: Query text describing the anomaly
            n_results: Number of results to return
            min_severity: Minimum severity level to return
            
        Returns:
            List of similar anomalies
        """
        try:
            query_embedding = local_models.generate_embeddings([query])[0]
            
            # Map severity to filter
            severity_order = {'low': 0, 'medium': 1, 'high': 2, 'critical': 3}
            where_clause = None
            if min_severity and min_severity in severity_order:
                # Note: ChromaDB doesn't support >= on strings, so we filter in post-processing
                pass
            
            results = self.anomalies_collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=n_results * 2  # Get more, filter later
            )
            
            formatted = self._format_results(results)
            
            # Filter by severity if needed
            if min_severity:
                min_level = severity_order[min_severity]
                formatted = [
                    r for r in formatted 
                    if severity_order.get(r['metadata'].get('severity', 'low'), 0) >= min_level
                ][:n_results]
            
            return formatted
            
        except Exception as e:
            logger.error(f"Anomaly search failed: {e}")
            return []
    
    def search_historical_events(self, query: str, n_results: int = 5,
                                date_from: Optional[str] = None,
                                date_to: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Search for similar historical market events
        
        Args:
            query: Query text
            n_results: Number of results
            date_from: Start date filter (YYYY-MM-DD)
            date_to: End date filter (YYYY-MM-DD)
            
        Returns:
            List of similar events
        """
        try:
            query_embedding = local_models.generate_embeddings([query])[0]
            
            results = self.events_collection.query(
                query_embeddings=[query_embedding.tolist()],
                n_results=n_results * 2
            )
            
            formatted = self._format_results(results)
            
            # Filter by date if needed
            if date_from or date_to:
                formatted = [
                    r for r in formatted
                    if self._date_in_range(
                        r['metadata'].get('event_date'),
                        date_from,
                        date_to
                    )
                ][:n_results]
            
            return formatted
            
        except Exception as e:
            logger.error(f"Event search failed: {e}")
            return []
    
    def _format_results(self, results: Dict) -> List[Dict[str, Any]]:
        """Format ChromaDB results into consistent structure"""
        formatted_results = []
        
        if not results['ids'] or not results['ids'][0]:
            return []
        
        for i in range(len(results['ids'][0])):
            formatted_results.append({
                'id': results['ids'][0][i],
                'description': results['documents'][0][i],
                'metadata': results['metadatas'][0][i],
                'similarity': 1 - results['distances'][0][i],  # Convert distance to similarity
                'relevance': 'high' if (1 - results['distances'][0][i]) > 0.8 else 'medium' if (1 - results['distances'][0][i]) > 0.6 else 'low'
            })
        
        return formatted_results
    
    def _date_in_range(self, event_date: Optional[str], 
                      date_from: Optional[str], 
                      date_to: Optional[str]) -> bool:
        """Check if date is within range"""
        if not event_date:
            return True
        
        if date_from and event_date < date_from:
            return False
        if date_to and event_date > date_to:
            return False
        
        return True
    
    def build_pattern_library(self):
        """Build initial pattern library with common financial patterns"""
        logger.info("Building pattern library from predefined patterns...")
        
        patterns = [
            {
                'id': 'golden_cross',
                'description': 'Short-term moving average crosses above long-term moving average, indicating bullish momentum',
                'type': 'technical_signal',
                'metadata': {'signal_type': 'bullish', 'reliability': 'medium'}
            },
            {
                'id': 'death_cross',
                'description': 'Short-term moving average crosses below long-term moving average, indicating bearish momentum',
                'type': 'technical_signal',
                'metadata': {'signal_type': 'bearish', 'reliability': 'medium'}
            },
            {
                'id': 'high_volume_breakout',
                'description': 'Price breaks through resistance level with volume 3x higher than average',
                'type': 'price_pattern',
                'metadata': {'signal_type': 'bullish', 'reliability': 'high'}
            },
            {
                'id': 'volume_climax',
                'description': 'Extreme volume spike followed by price reversal, often marks trend exhaustion',
                'type': 'price_pattern',
                'metadata': {'signal_type': 'reversal', 'reliability': 'high'}
            },
            {
                'id': 'divergence_rsi',
                'description': 'RSI makes higher lows while price makes lower lows, suggesting potential reversal',
                'type': 'technical_signal',
                'metadata': {'signal_type': 'bullish', 'reliability': 'medium'}
            },
            {
                'id': 'volatility_spike',
                'description': 'Volatility increases by more than 200% from average, indicating market uncertainty',
                'type': 'volatility_pattern',
                'metadata': {'signal_type': 'neutral', 'reliability': 'high'}
            },
            {
                'id': 'mean_reversion',
                'description': 'Asset price deviates significantly from average and shows signs of returning',
                'type': 'statistical_pattern',
                'metadata': {'signal_type': 'neutral', 'reliability': 'medium'}
            },
            {
                'id': 'momentum_acceleration',
                'description': 'Rate of price change is increasing, indicating strengthening trend',
                'type': 'momentum_pattern',
                'metadata': {'signal_type': 'continuation', 'reliability': 'medium'}
            }
        ]
        
        for pattern in patterns:
            self.add_market_pattern(
                pattern['id'],
                pattern['description'],
                pattern['type'],
                pattern['metadata']
            )
        
        logger.info(f"✓ Added {len(patterns)} patterns to library")
    
    def build_anomaly_library(self):
        """Build library of historical market anomalies"""
        logger.info("Building anomaly library...")
        
        anomalies = [
            {
                'id': 'flash_crash_2010',
                'description': 'Sudden market drop of over 9% within minutes followed by rapid recovery, caused by algorithmic trading',
                'severity': 'critical',
                'metadata': {'date': '2010-05-06', 'asset_type': 'stock', 'cause': 'algorithmic'}
            },
            {
                'id': 'black_monday_1987',
                'description': 'Global stock market crash with 22% single-day decline, largest one-day percentage decline in history',
                'severity': 'critical',
                'metadata': {'date': '1987-10-19', 'asset_type': 'stock', 'cause': 'panic_selling'}
            },
            {
                'id': 'crypto_crash_2022',
                'description': 'Cryptocurrency market loses over 60% of value amid rising interest rates and regulatory concerns',
                'severity': 'high',
                'metadata': {'date': '2022-05-01', 'asset_type': 'crypto', 'cause': 'macro_conditions'}
            },
            {
                'id': 'swiss_franc_shock',
                'description': 'Swiss National Bank removes currency cap causing 30% franc appreciation in minutes',
                'severity': 'high',
                'metadata': {'date': '2015-01-15', 'asset_type': 'forex', 'cause': 'policy_change'}
            },
            {
                'id': 'gamestop_squeeze',
                'description': 'Retail-driven short squeeze causes stock to surge 1500% in two weeks',
                'severity': 'medium',
                'metadata': {'date': '2021-01-28', 'asset_type': 'stock', 'cause': 'short_squeeze'}
            },
            {
                'id': 'circuit_breaker_2020',
                'description': 'Multiple trading halts triggered as market drops amid COVID-19 pandemic fears',
                'severity': 'high',
                'metadata': {'date': '2020-03-16', 'asset_type': 'stock', 'cause': 'pandemic'}
            }
        ]
        
        for anomaly in anomalies:
            self.add_anomaly(
                anomaly['id'],
                anomaly['description'],
                anomaly['severity'],
                anomaly['metadata']
            )
        
        logger.info(f"✓ Added {len(anomalies)} anomalies to library")
    
    def get_collection_stats(self) -> Dict[str, Any]:
        """Get statistics about collections"""
        return {
            'patterns': {
                'count': self.patterns_collection.count(),
                'name': 'financial_patterns'
            },
            'anomalies': {
                'count': self.anomalies_collection.count(),
                'name': 'market_anomalies'
            },
            'events': {
                'count': self.events_collection.count(),
                'name': 'market_events'
            },
            'storage_path': config.chroma_persist_dir
        }
    
    def clear_all_collections(self):
        """Clear all collections (use with caution!)"""
        logger.warning("Clearing all vector store collections...")
        self.client.delete_collection("financial_patterns")
        self.client.delete_collection("market_anomalies")
        self.client.delete_collection("market_events")
        
        # Recreate collections
        self.__init__()
        logger.info("Collections cleared and recreated")

# Global instance
vector_store = FinancialVectorStore()