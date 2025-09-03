from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ connection
    rabbitmq_url: str = Field("localhost", env="RABBITMQ_URL")
    rabbitmq_user: str = Field(..., env="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., env="RABBITMQ_PASSWORD")

    # Input settings
    input_exchange: str = Field("events", env="INPUT_EXCHANGE")
    input_queue: str = Field("text", env="INPUT_QUEUE")
    input_routing_key: str = Field("text", env="INPUT_ROUTING_KEY")

    # Output settings
    output_exchange: str = Field("events", env="OUTPUT_EXCHANGE")
    output_queue: str = Field("chunks", env="OUTPUT_QUEUE")
    output_routing_key: str = Field("chunks", env="OUTPUT_ROUTING_KEY")

    # Chunker settings
    prefetch_count: int = Field(16, env="PREFETCH_COUNT")
    chunk_strategy: str = Field("sliding", env="CHUNK_STRATEGY")
    chunk_size: int = Field(1200, env="CHUNK_SIZE")
    chunk_overlap: int = Field(200, env="CHUNK_OVERLAP")

    # General settings
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        case_sensitive = False

settings = Settings()