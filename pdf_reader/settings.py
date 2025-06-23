# Pydantic settings for PDF reader service
from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # MinIO
    minio_endpoint: str = Field("127.0.0.1:9000", env="MINIO_ENDPOINT")
    minio_access_key: str = Field(..., env="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(..., env="MINIO_SECRET_KEY")
    minio_bucket: str = Field(..., env="MINIO_BUCKET")
    minio_secure: bool = Field(False, env="MINIO_SECURE")

    # RabbitMQ
    rabbitmq_url: str = Field("localhost", env="RABBITMQ_URL")
    rabbitmq_exchange: str = Field("events", env="RABBITMQ_EXCHANGE")
    rabbitmq_queue: str = Field("text", env="RABBITMQ_QUEUE")
    rabbitmq_routing_key: str = Field("text", env="RABBITMQ_ROUTING_KEY")
    rabbitmq_url: str = Field("localhost", env="RABBITMQ_URL")
    rabbitmq_user: str = Field(..., env="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., env="RABBITMQ_PASSWORD")

    # General settings
    workers: int = Field(4, env="WORKERS")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        case_sensitive = False

settings = Settings()
