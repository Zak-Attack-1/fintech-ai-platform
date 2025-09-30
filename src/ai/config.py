"""
AI service configuration for hybrid approach
File: src/ai/config.py
"""
import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

@dataclass
class AIConfig:
    """Configuration for AI services"""
    
    # Hugging Face API (30,000 free requests/month)
    hf_api_key: str = os.getenv("HF_API_KEY", "")
    hf_model: str = "mistralai/Mixtral-8x7B-Instruct-v0.1"
    
    # Local CPU models
    embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"
    classification_model: str = "ProsusAI/finbert"
    
    # Vector database
    chroma_persist_dir: str = "./data/chroma"
    
    # API rate limits
    hf_requests_per_month: int = 30000
    hf_requests_per_day: int = 1000
    
    # Model parameters
    max_input_length: int = 2048
    temperature: float = 0.1  # Low temperature for factual responses
    top_p: float = 0.9
    
    # Database connection
    db_host: str = os.getenv("DB_HOST", "localhost")
    db_port: int = int(os.getenv("DB_PORT", "9543"))
    db_name: str = os.getenv("DB_NAME", "fintech_analytics")
    db_user: str = os.getenv("DB_USER", "fintech_user")
    db_password: str = os.getenv("POSTGRES_PASSWORD", "secure_password_change_me")
    
    def validate(self) -> bool:
        """Validate configuration"""
        if not self.hf_api_key:
            print("⚠️  Warning: HF_API_KEY not set. Complex queries will be limited.")
            return False
        
        if not self.db_password or self.db_password == "secure_password_change_me":
            print("⚠️  Warning: Using default database password. Update in production!")
        
        return True
    
    def get_db_connection_params(self) -> dict:
        """Get database connection parameters"""
        return {
            'host': self.db_host,
            'port': self.db_port,
            'database': self.db_name,
            'user': self.db_user,
            'password': self.db_password
        }

# Global config instance
config = AIConfig()

# Validate on import
config.validate()