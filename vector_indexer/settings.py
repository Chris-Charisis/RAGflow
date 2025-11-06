from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ (same style as your existing code)
    rabbitmq_host: str = Field("localhost", env="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, env="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", env="RABBITMQ_VHOST")
    rabbitmq_user: str = Field("user", env="RABBITMQ_USER")
    rabbitmq_password: str = Field("password", env="RABBITMQ_PASSWORD")
    rabbitmq_exchange: str = Field("events", env="RABBITMQ_EXCHANGE")
    rabbitmq_prefetch_count: int = Field(16, env="RABBITMQ_PREFETCH_COUNT")


    # Input settings
    rabbitmq_input_exchange: str = Field("events", env="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("embeddings", env="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("embeddings", env="INPUT_ROUTING_KEY")

   # Backend selection
    backend: str = Field("weaviate", env="INDEX_BACKEND")  # 'weaviate' (default) | 'other'
    collection: str = Field("RAGChunks", env="COLLECTION")

    # Weaviate (v4)
    weaviate_url: str | None = Field(None, env="WEAVIATE_URL")   # e.g. https://yourcluster.weaviate.network
    weaviate_api_key: str | None = Field("user-a-key", env="WEAVIATE_API_KEY")
    weaviate_host: str = Field("localhost", env="WEAVIATE_HOST")
    weaviate_port: int = Field(8080, env="WEAVIATE_PORT")
    weaviate_grpc_port: int = Field(50051, env="WEAVIATE_GRPC_PORT")
    weaviate_tenant: str | None = Field(None, env="WEAVIATE_TENANT")  # optional multi-tenancy
    create_collection_if_missing: bool = Field(True, env="WEAVIATE_CREATE_COLLECTION")

    # Behavior
    dry_run: bool = Field(False, env="DRY_RUN")
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        case_sensitive = False

settings = Settings()