"""Typed configuration loaded from environment variables (prefix ``APP_``).

Only ``.env.example`` is committed. Secrets (the JWT signing secret, the Anthropic API key) are
``SecretStr`` and come only from the environment, so they never appear in ``repr`` or logs.
"""

from __future__ import annotations

import re
from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from pydantic import AliasChoices, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

__all__ = ["Settings", "get_settings"]

_ORIGIN = re.compile(r"^https?://[A-Za-z0-9.-]+(:\d{1,5})?$")
_HOSTNAME = re.compile(
    r"^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$"
)
_MIN_JWT_SECRET_BYTES = 32


def _split_csv(value: object) -> object:
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return value


class Settings(BaseSettings):
    """Service settings. Every field can be set with ``APP_<FIELD_NAME>``."""

    model_config = SettingsConfigDict(
        env_prefix="APP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )

    # --- service ---
    service_name: str = Field(
        default="replayable-agent-runtime", pattern=r"^[a-z0-9][a-z0-9-]{0,62}$"
    )
    environment: Literal["dev", "test", "prod"] = "dev"
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_json: bool = True
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    enable_docs: bool = False

    # --- HTTP security ---
    jwt_secret: SecretStr | None = None
    jwt_issuer: str = Field(default="rar-local", min_length=1, max_length=128)
    jwt_audience: str = Field(default="rar-control-plane", min_length=1, max_length=128)
    jwt_ttl_seconds: int = Field(default=3600, ge=60, le=86_400)
    jwt_leeway_seconds: int = Field(default=30, ge=0, le=300)
    cors_allow_origins: Annotated[list[str], NoDecode] = Field(default_factory=list)
    rate_limit_per_second: float = Field(default=5.0, gt=0)
    rate_limit_burst: int = Field(default=20, ge=1)
    rate_limit_max_clients: int = Field(default=10_000, ge=1)
    max_body_bytes: int = Field(default=1_048_576, ge=1)
    max_input_bytes: int = Field(default=65_536, ge=1)
    metrics_require_auth: bool = True

    # --- observability ---
    tracing_exporter: Literal["none", "console"] = "none"

    # --- durable runtime ---
    journal_path: Path = Path("var/rar.db")
    sinks_path: Path = Path("var/sinks.db")
    workspace_dir: Path = Path("var/workspace")
    journal_synchronous: Literal["NORMAL", "FULL"] = "NORMAL"
    max_history_events: int = Field(default=10_000, ge=2)
    lease_seconds: float = Field(default=30.0, gt=0)
    heartbeat_seconds: float = Field(default=10.0, gt=0)
    poll_seconds: float = Field(default=0.5, gt=0)
    worker_id: str | None = Field(default=None, pattern=r"^[A-Za-z0-9._-]{1,128}$")
    embedded_worker: bool = True
    egress_allowlist: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["kb.internal", "status.internal"]
    )

    # --- models ---
    provider: Literal["fake", "replay", "anthropic"] = "fake"
    cassette_path: Path | None = None
    anthropic_api_key: SecretStr | None = Field(
        default=None, validation_alias=AliasChoices("ANTHROPIC_API_KEY", "anthropic_api_key")
    )
    llm_model: str = "claude-opus-5"
    llm_effort: Literal["low", "medium", "high", "xhigh", "max"] = "high"
    llm_max_retries: int = Field(default=2, ge=0, le=10)
    # The SDK default. Non-streaming requests up to 16K output tokens can legitimately take minutes;
    # a shorter total timeout cuts them off, and each retry can be billed again.
    llm_timeout_seconds: float = Field(default=600.0, gt=0)
    llm_refusal_fallback: bool = True
    budget_max_steps: int = Field(default=50, ge=0)
    budget_max_tokens: int = Field(default=200_000, ge=0)
    budget_max_usd: float = Field(default=1.0, ge=0)

    @field_validator("jwt_secret")
    @classmethod
    def _check_jwt_secret(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and value.get_secret_value():
            if len(value.get_secret_value().encode("utf-8")) < _MIN_JWT_SECRET_BYTES:
                raise ValueError("jwt_secret must be at least 32 bytes")
            return value
        return None

    @field_validator("cors_allow_origins", "egress_allowlist", mode="before")
    @classmethod
    def _parse_csv(cls, value: object) -> object:
        return _split_csv(value)

    @field_validator("cors_allow_origins")
    @classmethod
    def _check_origins(cls, value: list[str]) -> list[str]:
        for origin in value:
            if not _ORIGIN.fullmatch(origin):
                raise ValueError(
                    "cors_allow_origins must be explicit scheme://host[:port] origins "
                    "('*' is refused)"
                )
        return value

    @field_validator("egress_allowlist")
    @classmethod
    def _check_egress(cls, value: list[str]) -> list[str]:
        for host in value:
            if not _HOSTNAME.fullmatch(host):
                raise ValueError("egress_allowlist entries must be lowercase hostnames")
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings (cached). Tests construct ``Settings(...)`` directly instead."""
    return Settings()
