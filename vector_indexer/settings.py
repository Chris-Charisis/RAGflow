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

    # Input settings (embeddings -> index)
    rabbitmq_input_exchange: str = Field("events", validation_alias="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("embeddings", validation_alias="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("embeddings", validation_alias="INPUT_ROUTING_KEY")

    # Deletion settings (deletion events -> remove from index). Must match pdf_reader.
    rabbitmq_delete_exchange: str = Field("events", validation_alias="RABBITMQ_DELETE_EXCHANGE")
    rabbitmq_delete_queue: str = Field("deletions", validation_alias="RABBITMQ_DELETE_QUEUE")
    rabbitmq_delete_routing_key: str = Field("deletions", validation_alias="RABBITMQ_DELETE_ROUTING_KEY")

   # Backend selection
    backend: str = Field("weaviate", validation_alias="INDEX_BACKEND")  # 'weaviate' (default) | 'other'
    collection: str = Field("recursive_with_overlap", validation_alias="COLLECTION")
    # Companion collection holding deletion tombstones (one per doc_id).
    tombstone_collection: str = Field("Tombstones", validation_alias="TOMBSTONE_COLLECTION")

    # Weaviate (v4) settings    
    weaviate_url: str | None = Field(None, validation_alias="WEAVIATE_URL")
    weaviate_api_key: str = Field("user-a-key", validation_alias="WEAVIATE_API_KEY")
    weaviate_host: str = Field("weaviate", validation_alias="WEAVIATE_HOST")
    weaviate_port: int = Field(8080, validation_alias="WEAVIATE_PORT")
    weaviate_grpc_port: int = Field(50051, validation_alias="WEAVIATE_GRPC_PORT")
    weaviate_tenant: str | None = Field(None, validation_alias="WEAVIATE_TENANT")
    create_collection_if_missing: bool = Field(True, validation_alias="WEAVIATE_CREATE_COLLECTION")

    # Batching (user-tunable throughput hyperparameter; 1 = per-message)
    batch_size: int = Field(16, validation_alias="BATCH_SIZE")
    batch_flush_seconds: float = Field(2.0, validation_alias="BATCH_FLUSH_SECONDS")

    # Observability
    metrics_port: int = Field(9100, validation_alias="METRICS_PORT")

    # General settings
    dry_run: bool = Field(False, validation_alias="DRY_RUN")
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    model_config = SettingsConfigDict(
        case_sensitive = False,
    )
        

settings = Settings()