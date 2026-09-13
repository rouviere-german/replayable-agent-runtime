from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from replayable_agent_runtime.core.config import Settings, get_settings

pytestmark = pytest.mark.unit


def test_defaults_fail_closed_and_stay_offline() -> None:
    settings = Settings(_env_file=None)
    assert settings.jwt_secret is None
    assert settings.cors_allow_origins == []
    assert settings.enable_docs is False
    assert settings.metrics_require_auth is True
    assert settings.host == "127.0.0.1"
    assert settings.anthropic_api_key is None
    assert settings.provider == "fake"
    assert settings.llm_model == "claude-opus-5"
    assert settings.journal_synchronous == "NORMAL"
    assert settings.egress_allowlist == ["kb.internal", "status.internal"]


def test_environment_variables_are_parsed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("APP_JWT_SECRET", "s" * 40)
    monkeypatch.setenv("APP_CORS_ALLOW_ORIGINS", "https://app.example.com,http://localhost:3000")
    monkeypatch.setenv("APP_EGRESS_ALLOWLIST", "kb.internal, docs.internal")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "not-a-real-key")
    monkeypatch.setenv("APP_LLM_EFFORT", "medium")
    monkeypatch.setenv("APP_JOURNAL_SYNCHRONOUS", "FULL")
    settings = Settings(_env_file=None)
    assert settings.jwt_secret is not None
    assert "s" * 40 not in repr(settings)
    assert settings.cors_allow_origins == ["https://app.example.com", "http://localhost:3000"]
    assert settings.egress_allowlist == ["kb.internal", "docs.internal"]
    assert settings.anthropic_api_key is not None
    assert settings.anthropic_api_key.get_secret_value() == "not-a-real-key"
    assert "not-a-real-key" not in repr(settings)
    assert settings.llm_effort == "medium"
    assert settings.journal_synchronous == "FULL"


@pytest.mark.parametrize("origins", [["*"], ["https://example.com/"], ["example.com"]])
def test_wildcard_or_malformed_origins_are_rejected(origins: list[str]) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, cors_allow_origins=origins)


@pytest.mark.parametrize("hosts", [["*"], ["KB.internal"], ["http://kb.internal"], ["10.0.0.1:80"]])
def test_egress_allowlist_entries_must_be_plain_hostnames(hosts: list[str]) -> None:
    with pytest.raises(ValidationError):
        Settings(_env_file=None, egress_allowlist=hosts)


def test_env_example_documents_every_setting(repo_root: Path) -> None:
    lines = (repo_root / ".env.example").read_text(encoding="utf-8").splitlines()
    documented = {
        line.split("=", 1)[0].strip() for line in lines if line.strip() and not line.startswith("#")
    }
    expected = {
        "ANTHROPIC_API_KEY" if name == "anthropic_api_key" else f"APP_{name.upper()}"
        for name in Settings.model_fields
    }
    assert documented == expected


def test_get_settings_is_cached() -> None:
    get_settings.cache_clear()
    try:
        assert get_settings() is get_settings()
    finally:
        get_settings.cache_clear()
