from __future__ import annotations

import time

import jwt
import pytest
from pydantic import SecretStr, ValidationError

from replayable_agent_runtime.core.config import Settings
from replayable_agent_runtime.core.security import (
    SCOPES,
    TokenService,
    extract_bearer,
    generate_secret,
    require_scope,
)

pytestmark = pytest.mark.unit

SECRET = "unit-test-signing-secret-" + "x" * 24


def service(secret: str | None = SECRET, **kwargs: object) -> TokenService:
    return TokenService(
        SecretStr(secret) if secret is not None else None,
        issuer="rar-test",
        audience="rar-api",
        **kwargs,  # type: ignore[arg-type]
    )


def test_issued_tokens_verify_with_their_scopes() -> None:
    tokens = service()
    principal = tokens.verify(tokens.issue("alice", ["runs:start", "runs:read"]))
    assert principal is not None
    assert principal.subject == "alice"
    assert principal.scopes == frozenset({"runs:start", "runs:read"})
    assert principal.has("runs:read")
    assert not principal.has("runs:approve")
    assert len(principal.token_id) == 32


def test_expired_future_and_foreign_tokens_are_refused() -> None:
    tokens = service(leeway_seconds=0)
    now = time.time()
    assert (
        tokens.verify(tokens.issue("alice", ["runs:read"], ttl_seconds=60, now=now - 3600)) is None
    )
    assert tokens.verify(tokens.issue("alice", ["runs:read"], now=now + 3600)) is None
    other_audience = TokenService(SecretStr(SECRET), issuer="rar-test", audience="other")
    assert tokens.verify(other_audience.issue("alice", ["runs:read"])) is None
    other_issuer = TokenService(SecretStr(SECRET), issuer="elsewhere", audience="rar-api")
    assert tokens.verify(other_issuer.issue("alice", ["runs:read"])) is None
    other_secret = service("another-signing-secret-" + "y" * 24)
    assert tokens.verify(other_secret.issue("alice", ["runs:read"])) is None


def _claims(**overrides: object) -> dict[str, object]:
    now = int(time.time())
    claims: dict[str, object] = {
        "iss": "rar-test",
        "aud": "rar-api",
        "sub": "alice",
        "scope": "runs:read runs:approve",
        "iat": now,
        "nbf": now,
        "exp": now + 60,
        "jti": "abc",
    }
    claims.update(overrides)
    return claims


@pytest.mark.filterwarnings("ignore::jwt.warnings.InsecureKeyLengthWarning")
def test_algorithm_confusion_none_and_missing_claims_are_refused() -> None:
    tokens = service()
    assert tokens.verify(jwt.encode(_claims(), None, algorithm="none")) is None
    assert tokens.verify(jwt.encode(_claims(), SECRET, algorithm="HS512")) is None
    for claim in ("exp", "iat", "nbf", "sub", "iss", "aud", "jti"):
        claims = _claims()
        del claims[claim]
        assert tokens.verify(jwt.encode(claims, SECRET, algorithm="HS256")) is None, claim
    assert tokens.verify(jwt.encode(_claims(sub="bad subject"), SECRET, algorithm="HS256")) is None
    assert (
        tokens.verify(jwt.encode(_claims(scope=["runs:read"]), SECRET, algorithm="HS256")) is None
    )
    unknown = tokens.verify(jwt.encode(_claims(scope="runs:read admin"), SECRET, algorithm="HS256"))
    assert unknown is not None
    assert unknown.scopes == frozenset({"runs:read"})
    signature_flipped = tokens.issue("alice", ["runs:read"])[:-2] + "AA"
    assert tokens.verify(signature_flipped) is None
    assert tokens.verify("x" * 5000) is None
    assert tokens.verify(None) is None


def test_without_a_secret_everything_fails_closed() -> None:
    tokens = service(None)
    assert not tokens.configured
    assert tokens.verify(service().issue("alice", ["runs:read"])) is None
    with pytest.raises(ValueError, match="not configured"):
        tokens.issue("alice", ["runs:read"])


def test_issue_validates_subject_and_scopes() -> None:
    with pytest.raises(ValueError, match="subject"):
        service().issue("not a subject!", ["runs:read"])
    with pytest.raises(ValueError, match="unknown scopes"):
        service().issue("alice", ["runs:admin"])
    with pytest.raises(ValueError, match="unknown scope"):
        require_scope("runs:admin")
    assert frozenset({"runs:start", "runs:signal", "runs:read", "runs:approve"}) == SCOPES


def test_secrets_must_be_long_and_come_from_settings() -> None:
    with pytest.raises(ValidationError, match="32 bytes"):
        Settings(_env_file=None, jwt_secret=SecretStr("short"))
    assert Settings(_env_file=None, jwt_secret=SecretStr("")).jwt_secret is None
    assert len(generate_secret()) >= 43
    settings = Settings(_env_file=None, jwt_secret=SecretStr(SECRET))
    assert SECRET not in repr(settings)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"authorization": "Bearer abc"}, "abc"),
        ({"authorization": "bearer  abc "}, "abc"),
        ({"authorization": "Basic abc"}, None),
        ({"authorization": "Bearer "}, None),
        ({}, None),
    ],
)
def test_extract_bearer(headers: dict[str, str], expected: str | None) -> None:
    assert extract_bearer(headers) == expected
