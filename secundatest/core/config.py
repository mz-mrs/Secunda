from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_port: int = 5432
    postgres_host: str = "localhost"

    api_key: str

    rabbitmq_user: str
    rabbitmq_password: str
    rabbitmq_port: int = 5672
    rabbitmq_host: str = "localhost"

    @property
    def rabbitmq_url(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:"
            f"{self.rabbitmq_password}@"
            f"{self.rabbitmq_host}:"
            f"{self.rabbitmq_port}/"
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
