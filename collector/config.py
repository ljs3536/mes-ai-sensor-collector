from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_topic_prefix: str = "mes"
    influx_url: str = "http://localhost:8086"
    influx_token: str = "mes-dev-token"
    influx_org: str = "mes"
    influx_bucket: str = "sensors"
    cors_origins: list[str] = ["http://localhost:3001"]
    timezone: str = "Asia/Seoul"


@lru_cache
def get_settings() -> Settings:
    return Settings()
