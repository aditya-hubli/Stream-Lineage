"""
Configuration Management

Centralized configuration using environment variables.
"""

import os


class Config:
    """Application configuration from environment variables."""
    
    # Kafka Configuration
    KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:19092")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "streamlineage-lineage-ingester")
    
    # Database Configuration
    DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/lineage.duckdb")
    
    # API Configuration
    API_HOST = os.getenv("API_HOST", "0.0.0.0")
    API_PORT = int(os.getenv("API_PORT", "8000"))
    
    # Performance Configuration
    GRAPH_SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("GRAPH_SNAPSHOT_INTERVAL_MINUTES", "60"))
    MAX_EVENTS_PER_BATCH = int(os.getenv("MAX_EVENTS_PER_BATCH", "1000"))
    
    # Replay Configuration
    REPLAY_SEEK_RANGE_THRESHOLD = int(os.getenv("REPLAY_SEEK_RANGE_THRESHOLD", "10"))
    
    @classmethod
    def get_config_summary(cls) -> dict:
        """Get configuration summary for debugging."""
        return {
            "kafka_brokers": cls.KAFKA_BROKERS,
            "duckdb_path": cls.DUCKDB_PATH,
            "api_host": cls.API_HOST,
            "api_port": cls.API_PORT,
        }


config = Config()
