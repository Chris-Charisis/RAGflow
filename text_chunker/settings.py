# Pydantic settings for chunking service
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ
    rabbitmq_host: str = Field("rabbitmq", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, validation_alias="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", validation_alias="RABBITMQ_VHOST")        
    rabbitmq_user: str = Field(..., validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., validation_alias="RABBITMQ_PASSWORD")
    rabbitmq_prefetch_count: int = Field(16, validation_alias="RABBITMQ_PREFETCH_COUNT")

    # Input settings
    rabbitmq_input_exchange: str = Field("events", validation_alias="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("text", validation_alias="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("text", validation_alias="INPUT_ROUTING_KEY")

    # Output settings
    rabbitmq_output_exchange: str = Field("events", validation_alias="OUTPUT_EXCHANGE")
    rabbitmq_output_queue: str = Field("chunks", validation_alias="OUTPUT_QUEUE")
    rabbitmq_output_routing_key: str = Field("chunks", validation_alias="OUTPUT_ROUTING_KEY")

    # Chunker settings
    chunk_strategy: str = Field("recursive", validation_alias="CHUNK_STRATEGY")
    chunk_size: int = Field(350, validation_alias="CHUNK_SIZE")
    chunk_overlap: int = Field(0, validation_alias="CHUNK_OVERLAP")

    # General settings
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    class Config:
        case_sensitive = False

settings = Settings()