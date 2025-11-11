from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ (same style as your existing code)
    rabbitmq_host: str = Field("rabbitmq", validation_alias="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, validation_alias="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", validation_alias="RABBITMQ_VHOST")
    rabbitmq_user: str = Field("user", validation_alias="RABBITMQ_USER")
    rabbitmq_password: str = Field("password", validation_alias="RABBITMQ_PASSWORD")
    rabbitmq_exchange: str = Field("events", validation_alias="RABBITMQ_EXCHANGE")
    rabbitmq_prefetch_count: int = Field(16, validation_alias="RABBITMQ_PREFETCH_COUNT")


    # Input settings
    rabbitmq_input_exchange: str = Field("events", validation_alias="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("chunks", validation_alias="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("chunks", validation_alias="INPUT_ROUTING_KEY")

    # Output settings
    rabbitmq_output_exchange: str = Field("events", validation_alias="OUTPUT_EXCHANGE")
    rabbitmq_output_queue: str = Field("embeddings", validation_alias="OUTPUT_QUEUE")
    rabbitmq_output_routing_key: str = Field("embeddings", validation_alias="OUTPUT_ROUTING_KEY")

    # Ollama
    ollama_base_url: str = Field("http://192.168.18.106:11434", validation_alias="OLLAMA_BASE_URL")
    ollama_model: str = Field("mxbai-embed-large:335m", validation_alias="OLLAMA_MODEL")
    ollama_timeout_seconds: int = Field(60, validation_alias="OLLAMA_TIMEOUT_SECONDS")
    ollama_dimensions: int | None = Field(None, validation_alias="OLLAMA_DIMENSIONS")

    # General
    log_level: str = Field("INFO", validation_alias="LOG_LEVEL")

    model_config = SettingsConfigDict(
        case_sensitive = False,
    )

settings = Settings()
