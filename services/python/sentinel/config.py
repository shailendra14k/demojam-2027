from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql://casino:casino@casino-db:5432/casino"
    kafka_bootstrap: str = "sentinel-kafka-kafka-bootstrap:9092"
    kafka_client_id: str = "casino-sentinel"

    nvidia_nim_url: str = ""  # unused
    tfm_mode: str = "hashed"  # hashed | checkpoint | http
    tfm_checkpoint: str = ""  # path to models/decoder-foundation-model
    tfm_embed_url: str = ""  # http://tfm-embed:8080/v1/embeddings
    data_mode: str = "auto"  # auto | live | mock

    api_host: str = "0.0.0.0"
    api_port: int = 8080

    # Score weights from the architecture diagram
    weight_rules: float = 0.30
    weight_foundation: float = 0.35
    weight_xgboost: float = 0.20
    weight_kyc: float = 0.15

    simulator_interval_sec: float = 2.5
    simulator_enabled: bool = True
    # Tail Postgres if CDC is unavailable. OpenShift demo: false (Kafka CDC only).
    # Local Podman sets DIRECT_POLL=true because that stack has no Debezium.
    direct_poll: bool = False
    kafka_optional: bool = True
    # OpenShift AI / OVMS. Empty = local joblib. Example:
    # http://aml-xgboost.casino-sentinel.svc:8080/v2/models/aml-xgboost/infer
    xgb_infer_url: str = ""


settings = Settings()
