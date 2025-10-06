# Pydantic settings for PDF reader service
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # MinIO
    # minio_endpoint: str = Field("127.0.0.1:9000", env="MINIO_ENDPOINT")
    minio_endpoint: str = Field("minio", env="MINIO_ENDPOINT")
    minio_access_key: str = Field(..., env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(..., env="MINIO_SECRET_KEY")
    minio_bucket: str = Field(..., env="MINIO_BUCKET")
    minio_secure: bool = Field(False, env="MINIO_SECURE")

    # RabbitMQ general
    # rabbitmq_host: str = Field("localhost", env="RABBITMQ_HOST")
    rabbitmq_host: str = Field("rabbitmq", env="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, env="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", env="RABBITMQ_VHOST")    
    rabbitmq_exchange: str = Field("events", env="RABBITMQ_EXCHANGE")
    rabbitmq_queue: str | None = Field(None, env="RABBITMQ_QUEUE")
    rabbitmq_routing_key: str = Field("text", env="RABBITMQ_ROUTING_KEY")
    rabbitmq_user: str = Field(..., env="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., env="RABBITMQ_PASSWORD")

    # RabbitMQ for 'deletion' events (separate topic/queue)
    rabbitmq_delete_exchange: str = Field("events", env="RABBITMQ_DELETE_EXCHANGE")
    rabbitmq_delete_queue: str | None = Field(None, env="RABBITMQ_DELETE_QUEUE")
    rabbitmq_delete_routing_key: str = Field("deletions", env="RABBITMQ_DELETE_ROUTING_KEY")

    # General settings
    workers: int = Field(4, env="WORKERS")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    # Idempotency & polling
    processed_prefix: str = Field(".processed", env="PROCESSED_PREFIX")
    poll_interval_seconds: int = Field(30, env="POLL_INTERVAL_SECONDS")    

    class Config:
        case_sensitive = False

settings = Settings()
