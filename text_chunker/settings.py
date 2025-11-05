# Pydantic settings for chunking service
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ connection
    # rabbitmq_url: str = Field("localhost", env="RABBITMQ_URL")
    # rabbitmq_host: str = Field("localhost", env="RABBITMQ_HOST")
    rabbitmq_host: str = Field("rabbitmq", env="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, env="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", env="RABBITMQ_VHOST")        
    rabbitmq_user: str = Field(..., env="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., env="RABBITMQ_PASSWORD")
    rabbitmq_prefetch_count: int = Field(16, env="RABBITMQ_PREFETCH_COUNT")

    # Input settings
    rabbitmq_input_exchange: str = Field("events", env="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("text", env="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("text", env="INPUT_ROUTING_KEY")

    # Output settings
    rabbitmq_output_exchange: str = Field("events", env="OUTPUT_EXCHANGE")
    rabbitmq_output_queue: str = Field("chunks", env="OUTPUT_QUEUE")
    rabbitmq_output_routing_key: str = Field("chunks", env="OUTPUT_ROUTING_KEY")

    # Chunker settings
    chunk_strategy: str = Field("words", env="CHUNK_STRATEGY")
    chunk_size: int = Field(1200, env="CHUNK_SIZE")
    chunk_overlap: int = Field(0, env="CHUNK_OVERLAP")

    # General settings
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        case_sensitive = False

settings = Settings()