"""Local-issuer JWT authentication with scopes.

Tokens are HS256 JWTs signed with ``APP_JWT_SECRET`` (at least 32 bytes, from the environment
only). Verification pins the algorithm list to ``HS256`` (so ``alg: none`` and algorithm confusion
are refused), requires ``exp``, ``iat``, ``nbf``, ``sub``, ``iss``, ``aud`` and ``jti``, and checks
issuer and audience. Scopes:

- ``runs:start``: start runs;
- ``runs:signal``: send signals and request cancellation;
- ``runs:read``: read runs, history, approvals, verification results and metrics;
- ``runs:approve``: decide approvals (never for a run the same subject started).

With no secret configured, every protected route fails closed with 401.
"""

from __future__ import annotations

import re
import secrets
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Annotated, Any

import jwt
from fastapi import Depends, Request
from pydantic import SecretStr

from replayable_agent_runtime.core.errors import AppError

__all__ = [
    "SCOPES",
    "Principal",
    "TokenService",
    "extract_bearer",
    "generate_secret",
    "require_scope",
]

SCOPES = frozenset({"runs:start", "runs:signal", "runs:read", "runs:approve"})
_ALGORITHM = "HS256"
_SUBJECT = re.compile(r"^[A-Za-z0-9._:@-]{1,128}$")
_MAX_TOKEN_LENGTH = 4096


def generate_secret() -> str:
    """A new random signing secret (256 bits, URL-safe)."""
    return secrets.token_urlsafe(32)


@dataclass(frozen=True, slots=True)
class Principal:
    """An authenticated caller: the token subject and its granted scopes."""

    subject: str
    scopes: frozenset[str]
    token_id: str

    def has(self, scope: str) -> bool:
        return scope in self.scopes


class TokenService:
    def __init__(
        self,
        secret: SecretStr | None,
        *,
        issuer: str,
        audience: str,
        ttl_seconds: int = 3600,
        leeway_seconds: int = 30,
    ) -> None:
        self._secret = secret
        self.issuer = issuer
        self.audience = audience
        self.ttl_seconds = ttl_seconds
        self.leeway_seconds = leeway_seconds

    @property
    def configured(self) -> bool:
        return self._secret is not None and bool(self._secret.get_secret_value())

    def _key(self) -> str:
        if self._secret is None or not self._secret.get_secret_value():
            raise ValueError("APP_JWT_SECRET is not configured")
        return self._secret.get_secret_value()

    def issue(
        self,
        subject: str,
        scopes: Iterable[str],
        *,
        ttl_seconds: int | None = None,
        now: float | None = None,
    ) -> str:
        if not _SUBJECT.fullmatch(subject):
            raise ValueError("subject must match ^[A-Za-z0-9._:@-]{1,128}$")
        granted = sorted(set(scopes))
        unknown = [scope for scope in granted if scope not in SCOPES]
        if unknown:
            raise ValueError(f"unknown scopes: {unknown}")
        issued = int(now if now is not None else time.time())
        claims: dict[str, Any] = {
            "iss": self.issuer,
            "aud": self.audience,
            "sub": subject,
            "scope": " ".join(granted),
            "iat": issued,
            "nbf": issued,
            "exp": issued + (ttl_seconds if ttl_seconds is not None else self.ttl_seconds),
            "jti": uuid.uuid4().hex,
        }
        return jwt.encode(claims, self._key(), algorithm=_ALGORITHM)

    def verify(self, token: str | None) -> Principal | None:
        """Return the principal for a valid token, else None (never raises on bad input)."""
        if not self.configured or not token or len(token) > _MAX_TOKEN_LENGTH:
            return None
        try:
            claims = jwt.decode(
                token,
                self._key(),
                algorithms=[_ALGORITHM],
                audience=self.audience,
                issuer=self.issuer,
                leeway=self.leeway_seconds,
                options={"require": ["exp", "iat", "nbf", "sub", "iss", "aud", "jti"]},
            )
        except jwt.PyJWTError:
            return None
        subject = claims.get("sub")
        scope = claims.get("scope", "")
        if not isinstance(subject, str) or not _SUBJECT.fullmatch(subject):
            return None
        if not isinstance(scope, str):
            return None
        scopes = frozenset(item for item in scope.split() if item in SCOPES)
        return Principal(subject=subject, scopes=scopes, token_id=str(claims.get("jti", "")))


def extract_bearer(headers: Mapping[str, str]) -> str | None:
    """Read ``Authorization: Bearer <token>``. Plain mappings must use lowercase names."""
    auth = headers.get("authorization")
    if auth and auth[:7].lower() == "bearer ":
        return auth[7:].strip() or None
    return None


def _unauthorized(request: Request) -> AppError:
    metrics = getattr(request.app.state, "metrics", None)
    if metrics is not None:
        metrics.auth_failures_total.inc()
    return AppError(
        401,
        "unauthorized",
        "A valid bearer token is required.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def authenticate(request: Request) -> Principal:
    service: TokenService = request.app.state.token_service
    principal = service.verify(extract_bearer(request.headers))
    if principal is None:
        raise _unauthorized(request)
    return principal


def require_scope(scope: str) -> Callable[[Request], Principal]:
    """FastAPI dependency factory: authenticate, then require ``scope`` (401, then 403)."""
    if scope not in SCOPES:
        raise ValueError(f"unknown scope {scope}")

    def dependency(request: Request) -> Principal:
        principal = authenticate(request)
        if not principal.has(scope):
            raise AppError(403, "forbidden", f"The token lacks the {scope} scope.")
        return principal

    return dependency


AuthenticatedPrincipal = Annotated[Principal, Depends(authenticate)]
