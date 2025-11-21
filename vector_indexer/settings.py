# Pydantic settings for vector indexer service
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ
    rabbitmq_host: str = Field("rabbitmq", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, validation_alias="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", validation_alias="RABBITMQ_VHOST")
    rabbitmq_user: str = Field("user", validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field("password", validation_alias="RABBITMQ_PASSWORD")
    rabbitmq_exchange: str = Field("events", validation_alias="RABBITMQ_EXCHANGE")
    rabbitmq_prefetch_count: int = Field(16, validation_alias="RABBITMQ_PREFETCH_COUNT")

    # Input settings
    rabbitmq_input_exchange: str = Field("events", validation_alias="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("embeddings", validation_alias="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("embeddings", validation_alias="INPUT_ROUTING_KEY")

   # Backend selection
    backend: str = Field("weaviate", validation_alias="INDEX_BACKEND")  # 'weaviate' (default) | 'other'
    collection: str = Field("RAGChunks", validation_alias="COLLECTION")

    # Weaviate (v4) settings    
    weaviate_url: str | None = Field(None, validation_alias="WEAVIATE_URL")
    weaviate_api_key: str | None = Field("user-a-key", validation_alias="WEAVIATE_API_KEY")
    weaviate_host: str = Field("weaviate", validation_alias="WEAVIATE_HOST")
    weaviate_port: int = Field(8080, validation_alias="WEAVIATE_PORT")
    weaviate_grpc_port: int = Field(50051, validation_alias="WEAVIATE_GRPC_PORT")
    weaviate_tenant: str | None = Field(None, validation_alias="WEAVIATE_TENANT")
    create_collection_if_missing: bool = Field(True, validation_alias="WEAVIATE_CREATE_COLLECTION")

    # General settings
    dry_run: bool = Field(False, validation_alias="DRY_RUN")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    model_config = SettingsConfigDict(
        case_sensitive = False,
    )
        

settings = Settings()