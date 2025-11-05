from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    # RabbitMQ (same style as your existing code)
    rabbitmq_host: str = Field("rabbitmq", env="RABBITMQ_HOST")
    rabbitmq_port: int = Field(5672, env="RABBITMQ_PORT")
    rabbitmq_vhost: str = Field("/", env="RABBITMQ_VHOST")
    rabbitmq_user: str = Field(..., env="RABBITMQ_USER")
    rabbitmq_password: str = Field(..., env="RABBITMQ_PASSWORD")
    rabbitmq_exchange: str = Field("events", env="RABBITMQ_EXCHANGE")
    rabbitmq_prefetch_count: int = Field(16, env="RABBITMQ_PREFETCH_COUNT")


    # Input settings
    rabbitmq_input_exchange: str = Field("events", env="INPUT_EXCHANGE")
    rabbitmq_input_queue: str = Field("chunks", env="INPUT_QUEUE")
    rabbitmq_input_routing_key: str = Field("chunks", env="INPUT_ROUTING_KEY")

    # Output settings
    rabbitmq_output_exchange: str = Field("events", env="OUTPUT_EXCHANGE")
    rabbitmq_output_queue: str = Field("embeddings", env="OUTPUT_QUEUE")
    rabbitmq_output_routing_key: str = Field("embeddings", env="OUTPUT_ROUTING_KEY")

    # Ollama
    ollama_base_url: str = Field("http://ollama:11434", env="OLLAMA_BASE_URL")
    ollama_model: str = Field("mxbai-embed-large", env="OLLAMA_MODEL")
    ollama_timeout_seconds: int = Field(60, env="OLLAMA_TIMEOUT_SECONDS")
    ollama_dimensions: int | None = Field(None, env="OLLAMA_DIMENSIONS")

    # General
    log_level: str = Field("INFO", env="LOG_LEVEL")

    class Config:
        case_sensitive = False

settings = Settings()
