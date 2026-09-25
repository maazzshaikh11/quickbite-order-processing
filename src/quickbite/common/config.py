"""Central application configuration, loaded from environment variables.

Every tunable knob of the system lives here so behaviour can be changed
without touching code (see `.env.example`).
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- Infrastructure --------------------------------------------------
    database_url: str = "sqlite+aiosqlite:///./quickbite.db"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # --- Reliability ------------------------------------------------------
    # How many delayed retries a message gets before it is dead-lettered.
    max_retries: int = 3
    # Base delay (seconds) before the first retry; doubles every attempt.
    retry_base_delay_seconds: float = 5.0
    # Short retries for UpstreamNotReady (event arrived before upstream commit).
    notready_max_retries: int = 10
    notready_delay_seconds: float = 2.0
    # RabbitMQ QoS prefetch count per worker instance.
    prefetch_count: int = 5

    # --- Payment simulation ----------------------------------------------
    # random | never_fail | always_fail | fail_twice
    # Overridable per order via the `payment_behavior` field of POST /orders.
    payment_failure_mode: str = "random"
    # Failure probability used when payment_failure_mode == "random".
    payment_failure_rate: float = 0.25
    # Simulated payment latency window (seconds).
    payment_min_delay_seconds: float = 0.5
    payment_max_delay_seconds: float = 2.0

    # --- Other worker simulation ------------------------------------------
    restaurant_delay_seconds: float = 1.0
    delivery_delay_seconds: float = 1.0

    # --- API ---------------------------------------------------------------
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    log_level: str = "INFO"


settings = Settings()
