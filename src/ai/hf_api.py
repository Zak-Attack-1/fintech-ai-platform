"""
Hugging Face API integration for complex reasoning
File: src/ai/hf_api.py
"""
import requests
from typing import Dict, Any, Optional, List
import time
from datetime import datetime, timedelta
from loguru import logger
from src.ai.config import config
import json
import re

class HuggingFaceAPI:
    """Hugging Face Inference API client with intelligent rate limiting"""
    
    def __init__(self):
        self.api_key = config.hf_api_key
        self.base_url = "https://api-inference.huggingface.co/models"
        self.model = config.hf_model
        
        # Rate limiting tracking
        self.requests_today = 0
        self.last_reset_date = datetime.now().date()
        self.total_requests_month = 0
        self.last_request_time = None
        
        # Request headers
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Cache for repeated queries
        self.cache = {}
        self.cache_ttl = timedelta(hours=1)
        
        logger.info(f"HuggingFaceAPI initialized with model: {self.model}")
    
    def _check_rate_limits(self) -> tuple[bool, Optional[str]]:
        """
        Check if we're within rate limits
        Returns: (can_proceed, reason_if_not)
        """
        # Reset daily counter at midnight
        if datetime.now().date() != self.last_reset_date:
            self.requests_today = 0
            self.last_reset_date = datetime.now().date()
            logger.info("Daily request counter reset")
        
        # Check daily limit
        if self.requests_today >= config.hf_requests_per_day:
            return False, f"Daily limit reached ({config.hf_requests_per_day} requests)"
        
        # Check monthly limit
        if self.total_requests_month >= config.hf_requests_per_month:
            return False, f"Monthly limit reached ({config.hf_requests_per_month} requests)"
        
        # Rate limiting: minimum 1 second between requests
        if self.last_request_time:
            time_since_last = (datetime.now() - self.last_request_time).total_seconds()
            if time_since_last < 1.0:
                time.sleep(1.0 - time_since_last)
        
        return True, None
    
    def _get_cache_key(self, prompt: str, params: Dict) -> str:
        """Generate cache key for a request"""
        return f"{prompt[:100]}_{params.get('max_new_tokens')}_{params.get('temperature')}"
    
    def _check_cache(self, cache_key: str) -> Optional[str]:
        """Check if result exists in cache"""
        if cache_key in self.cache:
            cached_data, cached_time = self.cache[cache_key]
            if datetime.now() - cached_time < self.cache_ttl:
                logger.debug("Cache hit")
                return cached_data
            else:
                # Expired cache entry
                del self.cache[cache_key]
        return None
    
    def generate_text(self, prompt: str, max_length: int = 500,
                     use_cache: bool = True) -> Optional[str]:
        """
        Generate text completion using Hugging Face API
        
        Args:
            prompt: Input prompt
            max_length: Maximum tokens to generate
            use_cache: Whether to use cached results
            
        Returns:
            Generated text or None if failed
        """
        # Check cache first
        params = {
            "max_new_tokens": max_length,
            "temperature": config.temperature,
            "top_p": config.top_p,
        }
        cache_key = self._get_cache_key(prompt, params)
        
        if use_cache:
            cached_result = self._check_cache(cache_key)
            if cached_result:
                return cached_result
        
        # Check rate limits
        can_proceed, reason = self._check_rate_limits()
        if not can_proceed:
            logger.warning(f"Rate limit exceeded: {reason}")
            return None
        
        try:
            url = f"{self.base_url}/{self.model}"
            payload = {
                "inputs": prompt,
                "parameters": {
                    **params,
                    "return_full_text": False,
                    "do_sample": True,
                }
            }
            
            logger.debug(f"Sending request to HF API (length: {len(prompt)} chars)")
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            
            # Handle model loading
            if response.status_code == 503:
                logger.info("Model is loading, waiting 20 seconds...")
                time.sleep(20)
                response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            
            response.raise_for_status()
            
            # Update counters
            self.requests_today += 1
            self.total_requests_month += 1
            self.last_request_time = datetime.now()
            
            result = response.json()
            
            if isinstance(result, list) and len(result) > 0:
                generated_text = result[0].get('generated_text', '')
                
                # Cache the result
                if use_cache and generated_text:
                    self.cache[cache_key] = (generated_text, datetime.now())
                
                logger.debug(f"Generated {len(generated_text)} characters")
                return generated_text
            
            logger.warning(f"Unexpected response format: {result}")
            return None
            
        except requests.exceptions.Timeout:
            logger.error("Request timeout (30s)")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return None
    
    def generate_sql_from_nl(self, natural_language_query: str, 
                            schema_info: str) -> Optional[str]:
        """
        Generate SQL from natural language query
        
        Args:
            natural_language_query: User's question in natural language
            schema_info: Database schema description
            
        Returns:
            SQL query string or None
        """
        prompt = f"""You are a PostgreSQL expert for a financial analytics database.
Convert the natural language query to a valid PostgreSQL SQL query.

DATABASE SCHEMA:
{schema_info}

QUERY: {natural_language_query}

RULES:
- Return ONLY the SQL query, no explanations
- Use proper PostgreSQL syntax
- Include appropriate JOINs if needed
- Add LIMIT clause to prevent large results
- Use meaningful aliases

SQL Query:"""
        
        sql = self.generate_text(prompt, max_length=300)
        return self._clean_sql(sql) if sql else None
    
    def explain_financial_pattern(self, pattern_data: Dict[str, Any]) -> Optional[str]:
        """
        Generate explanation for financial patterns
        
        Args:
            pattern_data: Dictionary containing pattern information
            
        Returns:
            Human-readable explanation
        """
        # Format pattern data
        pattern_str = json.dumps(pattern_data, indent=2)
        
        prompt = f"""You are a financial analyst explaining market patterns to investors.

PATTERN DATA:
{pattern_str}

Provide a clear, concise explanation (2-3 sentences) that:
- Identifies what happened
- Explains why it matters
- Suggests what to watch for

Explanation:"""
        
        explanation = self.generate_text(prompt, max_length=200)
        return self._clean_explanation(explanation) if explanation else None
    
    def analyze_portfolio_risk(self, portfolio_data: Dict[str, Any]) -> Optional[str]:
        """
        Analyze portfolio risk and provide recommendations
        
        Args:
            portfolio_data: Portfolio holdings and metrics
            
        Returns:
            Risk analysis and recommendations
        """
        portfolio_str = json.dumps(portfolio_data, indent=2)
        
        prompt = f"""You are a risk management expert analyzing a portfolio.

PORTFOLIO:
{portfolio_str}

Provide a brief risk assessment (3-4 sentences) covering:
- Overall risk level
- Key concerns
- Diversification status
- One actionable recommendation

Analysis:"""
        
        analysis = self.generate_text(prompt, max_length=250)
        return self._clean_explanation(analysis) if analysis else None
    
    def summarize_market_news(self, news_items: List[str]) -> Optional[str]:
        """
        Summarize multiple market news items
        
        Args:
            news_items: List of news headlines/summaries
            
        Returns:
            Consolidated summary
        """
        news_text = "\n".join([f"- {item}" for item in news_items[:10]])
        
        prompt = f"""Summarize these market news items into key themes.

NEWS:
{news_text}

Provide a 2-3 sentence summary highlighting:
- Main market themes
- Sentiment (bullish/bearish/neutral)
- Key events

Summary:"""
        
        summary = self.generate_text(prompt, max_length=150)
        return self._clean_explanation(summary) if summary else None
    
    def _clean_sql(self, sql: str) -> str:
        """Clean and format SQL query"""
        if not sql:
            return ""
        
        # Remove markdown code blocks
        sql = re.sub(r'```sql\n?', '', sql)
        sql = re.sub(r'```\n?', '', sql)
        
        # Remove common prefixes
        sql = re.sub(r'^(SQL Query:|Query:)\s*', '', sql, flags=re.IGNORECASE)
        
        # Clean whitespace
        sql = sql.strip()
        
        # Remove trailing semicolon if present (will be added by executor)
        sql = sql.rstrip(';')
        
        return sql
    
    def _clean_explanation(self, text: str) -> str:
        """Clean explanation text"""
        if not text:
            return ""
        
        # Remove common prefixes
        text = re.sub(r'^(Explanation:|Analysis:|Summary:)\s*', '', text, flags=re.IGNORECASE)
        
        # Clean whitespace
        text = ' '.join(text.split())
        
        return text.strip()
    
    def get_usage_stats(self) -> Dict[str, Any]:
        """Get API usage statistics"""
        return {
            'requests_today': self.requests_today,
            'requests_month': self.total_requests_month,
            'remaining_today': config.hf_requests_per_day - self.requests_today,
            'remaining_month': config.hf_requests_per_month - self.total_requests_month,
            'last_request': self.last_request_time.isoformat() if self.last_request_time else None,
            'cache_size': len(self.cache),
            'daily_limit': config.hf_requests_per_day,
            'monthly_limit': config.hf_requests_per_month
        }
    
    def reset_usage_stats(self):
        """Reset usage statistics (for testing)"""
        self.requests_today = 0
        self.total_requests_month = 0
        logger.info("Usage statistics reset")
    
    def clear_cache(self):
        """Clear the response cache"""
        cache_size = len(self.cache)
        self.cache.clear()
        logger.info(f"Cleared {cache_size} cached responses")

# Global instance
hf_api = HuggingFaceAPI()