"""Exception hierarchy for the agentic kit and the durable runtime built on it."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from replayable_agent_runtime.agentic.providers.base import Usage

__all__ = [
    "ActivityFailedError",
    "AgenticError",
    "ApprovalDeniedError",
    "ApprovalError",
    "ApprovalIntegrityError",
    "AuditIntegrityError",
    "BudgetError",
    "BudgetExceededError",
    "CassetteFormatError",
    "CassetteMissError",
    "ProviderAuthError",
    "ProviderError",
    "ProviderRateLimitedError",
    "ProviderRefusalError",
    "ProviderRequestError",
    "ProviderTruncatedError",
    "ProviderUnavailableError",
    "StaleFencingTokenError",
    "ToolArgumentError",
    "ToolExecutionError",
    "ToolNotAllowedError",
    "ToolPolicyError",
]


class AgenticError(Exception):
    """Base class for every error raised by the agentic kit."""


class ProviderError(AgenticError):
    """An LLM call failed. ``retryable`` says whether the same request may succeed later.

    ``model`` and ``usage`` are set when the failed call was still billed (a truncated response, a
    refusal after partial output, malformed structured output), so callers charge budgets for it.
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        model: str | None = None,
        usage: Usage | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.model = model
        self.usage = usage


class ProviderAuthError(ProviderError):
    """Missing, invalid or insufficient credentials (never retryable)."""


class ProviderRequestError(ProviderError):
    """The request itself was rejected (bad parameters, unknown model, too large)."""


class ProviderRateLimitedError(ProviderError):
    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message, retryable=True)
        self.retry_after = retry_after


class ProviderUnavailableError(ProviderError):
    """Server-side failure, overload, timeout or network error."""

    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=True)


class ProviderRefusalError(ProviderError):
    """The model (or its safety classifiers, after any fallback) declined the request."""

    def __init__(
        self,
        category: str | None,
        explanation: str | None = None,
        *,
        model: str | None = None,
        usage: Usage | None = None,
    ) -> None:
        super().__init__(
            f"model refused the request (category={category or 'unspecified'})",
            model=model,
            usage=usage,
        )
        self.category = category
        self.explanation = explanation


class ProviderTruncatedError(ProviderError):
    """Output hit ``max_tokens``; partial output is discarded rather than trusted."""


class CassetteMissError(ProviderError):
    """ReplayProvider has no recorded response for this request hash."""

    def __init__(self, request_sha256: str) -> None:
        super().__init__(f"no cassette entry for request {request_sha256}")
        self.request_sha256 = request_sha256


class CassetteFormatError(ProviderError):
    """A cassette file line is not a valid entry."""


class ToolNotAllowedError(AgenticError):
    """Unknown tool, a tool outside the allowlist, or capabilities that were not granted."""


class ToolArgumentError(AgenticError):
    """Tool arguments failed schema validation."""


class ToolPolicyError(AgenticError):
    """A tool handler refused an operation (workspace escape, egress outside the allowlist).

    Messages name the rule that was violated, never the offending value.
    """


class ToolExecutionError(AgenticError):
    """A tool handler raised; details stay in logs, not in model-visible output."""


class ActivityFailedError(AgenticError):
    """A recorded activity (tool or LLM step) failed; replay re-raises the recorded failure."""

    def __init__(self, message: str, *, error_type: str = "ActivityFailedError") -> None:
        super().__init__(message)
        self.error_type = error_type


class StaleFencingTokenError(AgenticError):
    """A sink refused a write whose fencing token is lower than the highest it has seen."""

    def __init__(self, scope: str, token: int, highest: int) -> None:
        super().__init__(f"stale fencing token {token} for {scope} (highest seen {highest})")
        self.scope = scope
        self.token = token
        self.highest = highest


class ApprovalError(AgenticError):
    """An approval decision was invalid (unknown id, not pending, hash mismatch, self-approval)."""


class ApprovalIntegrityError(ApprovalError):
    """A stored approval no longer matches the call it was requested for."""


class ApprovalDeniedError(AgenticError):
    """A human denied the approval a write tool or gate was waiting for."""

    def __init__(self, name: str, decided_by: str | None, reason: str | None = None) -> None:
        super().__init__(f"approval for {name} was denied")
        self.name = name
        self.decided_by = decided_by
        self.reason = reason


class BudgetError(AgenticError):
    """Budget state is invalid (for example, limits raised on resume)."""


class BudgetExceededError(BudgetError):
    def __init__(self, dimension: str, limit: int, attempted: int) -> None:
        super().__init__(f"{dimension} budget exceeded: {attempted} > {limit}")
        self.dimension = dimension
        self.limit = limit
        self.attempted = attempted


class AuditIntegrityError(AgenticError):
    """The journal hash chain failed verification."""
