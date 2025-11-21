# Pydantic settings for PDF reader service
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # MinIO
    minio_endpoint: str = Field("minio:9000", validation_alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(..., validation_alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(..., validation_alias="MINIO_SECRET_KEY")
    minio_bucket: str = Field(..., validation_alias="MINIO_BUCKET")
    minio_secure: bool = Field(False, validation_alias="MINIO_SECURE")

    # RabbitMQ
    rabbitmq_host: str = Field("rabbitmq", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, validation_alias="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", validation_alias="RABBITMQ_VHOST")    
    rabbitmq_exchange: str = Field("events", validation_alias="RABBITMQ_EXCHANGE")
    rabbitmq_queue: str = Field("text", validation_alias="RABBITMQ_QUEUE")
    rabbitmq_routing_key: str = Field("text", validation_alias="RABBITMQ_ROUTING_KEY")
    rabbitmq_user: str = Field(..., validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., validation_alias="RABBITMQ_PASSWORD")
    rabbitmq_prefetch_count: int = Field(16, validation_alias="RABBITMQ_PREFETCH_COUNT")

    # RabbitMQ for 'deletion' events (separate topic/queue)
    rabbitmq_delete_exchange: str = Field("events", validation_alias="RABBITMQ_DELETE_EXCHANGE")
    rabbitmq_delete_queue: str = Field("deletions", validation_alias="RABBITMQ_DELETE_QUEUE")
    rabbitmq_delete_routing_key: str = Field("deletions", validation_alias="RABBITMQ_DELETE_ROUTING_KEY")

    # General settings
    workers: int = Field(4, validation_alias="WORKERS")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    # Idempotency & polling
    processed_prefix: str = Field(".processed", validation_alias="PROCESSED_PREFIX")
    poll_interval_seconds: int = Field(30, validation_alias="POLL_INTERVAL_SECONDS")    

    class Config:
        case_sensitive = False

settings = Settings()
