"""
Local CPU-optimized models for frequent operations
File: src/ai/local_models.py

This module manages three AI models that run locally on CPU:
1. Sentence Transformers - Generate embeddings (384 dimensions)
2. FinBERT - Financial sentiment analysis
3. BART-MNLI - Zero-shot classification for query intent

All models use lazy loading and are optimized for CPU performance.
"""
from sentence_transformers import SentenceTransformer
from transformers import pipeline
import numpy as np
from typing import List, Dict, Any, Optional
from loguru import logger
import warnings

# Suppress transformer warnings for cleaner output
warnings.filterwarnings('ignore')

class LocalModelManager:
    """Manages local CPU models for unlimited usage"""
    
    def __init__(self):
        """Initialize the model manager with lazy loading"""
        self._embedding_model = None
        self._sentiment_model = None
        self._classifier = None
        logger.info("LocalModelManager initialized (models load on first use)")
    
    @property
    def embedding_model(self):
        """
        Lazy load embedding model
        Model: sentence-transformers/all-MiniLM-L6-v2
        Size: ~80 MB
        Output: 384-dimensional vectors
        """
        if self._embedding_model is None:
            logger.info("Loading embedding model (all-MiniLM-L6-v2)...")
            logger.info("First load will download ~80 MB, please wait...")
            self._embedding_model = SentenceTransformer(
                'sentence-transformers/all-MiniLM-L6-v2',
                device='cpu'
            )
            logger.info("✓ Embedding model loaded successfully")
        return self._embedding_model
    
    @property
    def sentiment_model(self):
        """
        Lazy load financial sentiment model
        Model: ProsusAI/finbert
        Size: ~440 MB
        Output: positive/negative/neutral with confidence
        """
        if self._sentiment_model is None:
            logger.info("Loading financial sentiment model (FinBERT)...")
            logger.info("First load will download ~440 MB, please wait...")
            self._sentiment_model = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",
                device=-1  # -1 means CPU
            )
            logger.info("✓ Sentiment model loaded successfully")
        return self._sentiment_model
    
    @property
    def classifier(self):
        """
        Lazy load zero-shot classifier
        Model: facebook/bart-large-mnli
        Size: ~1.6 GB
        Output: Classification across arbitrary labels
        """
        if self._classifier is None:
            logger.info("Loading zero-shot classifier (BART-large-MNLI)...")
            logger.info("First load will download ~1.6 GB, please wait...")
            self._classifier = pipeline(
                "zero-shot-classification",
                model="facebook/bart-large-mnli",
                device=-1  # -1 means CPU
            )
            logger.info("✓ Classifier loaded successfully")
        return self._classifier
    
    def generate_embeddings(self, texts: List[str], 
                           batch_size: int = 32,
                           show_progress: bool = False) -> np.ndarray:
        """
        Generate embeddings for text
        
        Args:
            texts: List of text strings to embed
            batch_size: Batch size for processing (default: 32)
            show_progress: Show progress bar for large batches
            
        Returns:
            numpy array of shape (len(texts), 384)
            
        Example:
            >>> embeddings = local_models.generate_embeddings(["Bitcoin rising", "Stock market"])
            >>> print(embeddings.shape)
            (2, 384)
        """
        try:
            # Auto-enable progress bar for large batches
            if len(texts) > 10 and not show_progress:
                show_progress = True
            
            embeddings = self.embedding_model.encode(
                texts,
                batch_size=batch_size,
                convert_to_numpy=True,
                show_progress_bar=show_progress,
                normalize_embeddings=True  # Normalize for better similarity
            )
            
            logger.debug(f"Generated embeddings for {len(texts)} texts")
            return embeddings
            
        except Exception as e:
            logger.error(f"Embedding generation failed: {e}")
            # Return zero embeddings as fallback
            return np.zeros((len(texts), 384))
    
    def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        """
        Analyze financial sentiment of text
        
        Args:
            text: Text to analyze (max 512 characters)
            
        Returns:
            Dict with:
                - label: 'positive', 'negative', or 'neutral'
                - score: Confidence score (0-1)
                - confidence: 'high', 'medium', or 'low'
                
        Example:
            >>> sentiment = local_models.analyze_sentiment("Apple reports record earnings")
            >>> print(sentiment)
            {'label': 'positive', 'score': 0.95, 'confidence': 'high'}
        """
        try:
            # Truncate to model max length (512 tokens ≈ 2000 chars)
            if len(text) > 512:
                text = text[:512]
                logger.debug("Text truncated to 512 characters")
            
            result = self.sentiment_model(text)[0]
            
            # Determine confidence level
            score = float(result['score'])
            if score > 0.8:
                confidence = 'high'
            elif score > 0.6:
                confidence = 'medium'
            else:
                confidence = 'low'
            
            return {
                'label': result['label'],
                'score': score,
                'confidence': confidence
            }
            
        except Exception as e:
            logger.error(f"Sentiment analysis failed: {e}")
            return {
                'label': 'neutral',
                'score': 0.5,
                'confidence': 'low',
                'error': str(e)
            }
    
    def classify_query_intent(self, query: str, 
                             candidates: List[str],
                             multi_label: bool = False) -> Dict[str, Any]:
        """
        Classify user query intent using zero-shot classification
        
        Args:
            query: User query to classify
            candidates: List of possible intent labels
            multi_label: Whether multiple labels can apply
            
        Returns:
            Dict with:
                - intent: Top predicted intent
                - confidence: Confidence score for top intent
                - all_intents: Dict of all intents with scores
                
        Example:
            >>> intent = local_models.classify_query_intent(
            ...     "Show me top stocks",
            ...     ["data_retrieval", "aggregation", "comparison"]
            ... )
            >>> print(intent)
            {'intent': 'data_retrieval', 'confidence': 0.89, 'all_intents': {...}}
        """
        try:
            result = self.classifier(
                query,
                candidate_labels=candidates,
                multi_label=multi_label
            )
            
            return {
                'intent': result['labels'][0],
                'confidence': float(result['scores'][0]),
                'all_intents': {
                    label: float(score) 
                    for label, score in zip(result['labels'], result['scores'])
                }
            }
            
        except Exception as e:
            logger.error(f"Query classification failed: {e}")
            return {
                'intent': candidates[0] if candidates else 'unknown',
                'confidence': 0.0,
                'all_intents': {},
                'error': str(e)
            }
    
    def compute_similarity(self, 
                          query_embedding: np.ndarray, 
                          corpus_embeddings: np.ndarray) -> np.ndarray:
        """
        Compute cosine similarity between query and corpus
        
        Args:
            query_embedding: Single query embedding (384,)
            corpus_embeddings: Array of corpus embeddings (N, 384)
            
        Returns:
            Array of similarity scores (N,) in range [0, 1]
            
        Example:
            >>> query_emb = local_models.generate_embeddings(["Bitcoin"])[0]
            >>> corpus_emb = local_models.generate_embeddings(["BTC", "Ethereum", "Stock"])
            >>> similarities = local_models.compute_similarity(query_emb, corpus_emb)
            >>> print(similarities)
            [0.95, 0.72, 0.31]
        """
        try:
            # Ensure embeddings are normalized
            query_norm = query_embedding / (np.linalg.norm(query_embedding) + 1e-8)
            corpus_norm = corpus_embeddings / (np.linalg.norm(corpus_embeddings, axis=1, keepdims=True) + 1e-8)
            
            # Compute cosine similarity
            similarities = np.dot(corpus_norm, query_norm)
            
            # Clip to [0, 1] range (should already be, but ensure)
            similarities = np.clip(similarities, 0, 1)
            
            return similarities
            
        except Exception as e:
            logger.error(f"Similarity computation failed: {e}")
            return np.zeros(len(corpus_embeddings))
    
    def batch_sentiment_analysis(self, texts: List[str]) -> List[Dict[str, Any]]:
        """
        Analyze sentiment for multiple texts efficiently
        
        Args:
            texts: List of texts to analyze
            
        Returns:
            List of sentiment results
            
        Example:
            >>> texts = ["Bullish market", "Bearish trend", "Neutral outlook"]
            >>> results = local_models.batch_sentiment_analysis(texts)
            >>> for text, result in zip(texts, results):
            ...     print(f"{text}: {result['label']}")
        """
        results = []
        for text in texts:
            results.append(self.analyze_sentiment(text))
        return results
    
    def get_model_info(self) -> Dict[str, Any]:
        """
        Get information about loaded models
        
        Returns:
            Dict with model information including:
                - Model names
                - Whether they're loaded
                - Output dimensions
                - Memory usage (if available)
        """
        return {
            'embedding_model': {
                'name': 'all-MiniLM-L6-v2',
                'dimension': 384,
                'loaded': self._embedding_model is not None,
                'size_mb': 80,
                'speed': '~100 texts/sec'
            },
            'sentiment_model': {
                'name': 'FinBERT (ProsusAI/finbert)',
                'loaded': self._sentiment_model is not None,
                'size_mb': 440,
                'labels': ['positive', 'negative', 'neutral'],
                'speed': '~50 texts/sec'
            },
            'classifier': {
                'name': 'BART-large-MNLI',
                'loaded': self._classifier is not None,
                'size_mb': 1600,
                'type': 'zero-shot classification',
                'speed': '~20 queries/sec'
            }
        }
    
    def unload_models(self):
        """
        Unload all models to free memory
        Use this if you need to free up RAM
        """
        if self._embedding_model is not None:
            del self._embedding_model
            self._embedding_model = None
            logger.info("Embedding model unloaded")
        
        if self._sentiment_model is not None:
            del self._sentiment_model
            self._sentiment_model = None
            logger.info("Sentiment model unloaded")
        
        if self._classifier is not None:
            del self._classifier
            self._classifier = None
            logger.info("Classifier unloaded")
        
        # Force garbage collection
        import gc
        gc.collect()
        logger.info("All models unloaded, memory freed")

# Global instance - models load lazily on first use
local_models = LocalModelManager()