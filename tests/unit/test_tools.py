from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import BaseModel, ConfigDict, Field, JsonValue, SecretStr

from replayable_agent_runtime.agentic.errors import (
    ToolArgumentError,
    ToolNotAllowedError,
    ToolPolicyError,
)
from replayable_agent_runtime.agentic.providers.base import ToolUseBlock
from replayable_agent_runtime.agentic.tools import (
    Capability,
    EffectClass,
    ToolCallContext,
    ToolDefinition,
    ToolRegistry,
    tool_call_hash,
)
from replayable_agent_runtime.core.canonical import canonical_json
from replayable_agent_runtime.runtime.builtin_tools import (
    EgressPolicy,
    FakeServiceTransport,
    WorkspaceJail,
    default_services,
)

pytestmark = pytest.mark.unit

CTX = ToolCallContext(run_id="run-1", seq=3, idempotency_key="run-1:3", fencing_token=1)


class EchoArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    text: str = Field(max_length=20)
    count: int = 1


class WriteArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str


class OpenArgs(BaseModel):
    path: str


def echo(args: EchoArgs, ctx: ToolCallContext) -> JsonValue:
    return args.text * args.count


def write(args: WriteArgs, ctx: ToolCallContext) -> JsonValue:
    return f"wrote {args.path} with key {ctx.idempotency_key}"


ECHO = ToolDefinition("echo", "Echo text.", EchoArgs, frozenset({Capability.READ_DATA}), echo)
WRITE = ToolDefinition(
    "write_file",
    "Write a file.",
    WriteArgs,
    frozenset({Capability.WRITE_DATA}),
    write,
    effect=EffectClass.WRITE,
)


def make_registry() -> ToolRegistry:
    registry = ToolRegistry({Capability.READ_DATA, Capability.WRITE_DATA})
    registry.register(ECHO)
    registry.register(WRITE)
    return registry


def call(name: str, **arguments: JsonValue) -> ToolUseBlock:
    return ToolUseBlock(id="call-1", name=name, arguments=arguments)


def test_registration_requires_granted_capabilities() -> None:
    registry = ToolRegistry({Capability.READ_DATA})
    with pytest.raises(ToolNotAllowedError, match="write_data"):
        registry.register(WRITE)
    with pytest.raises(ValueError, match="already registered"):
        make_registry().register(ECHO)


def test_effect_classes_decide_approval_and_cannot_be_misdeclared() -> None:
    assert (ECHO.needs_approval, ECHO.has_side_effects) == (False, False)
    assert (WRITE.needs_approval, WRITE.has_side_effects) == (True, True)
    reviewed = ToolDefinition(
        "echo2", "Echo.", EchoArgs, frozenset({Capability.READ_DATA}), echo, requires_approval=True
    )
    assert reviewed.needs_approval
    undo = ToolDefinition(
        "undo", "Undo.", WriteArgs, frozenset({Capability.WRITE_DATA}), write,
        effect=EffectClass.COMPENSATE,
    )  # fmt: skip
    assert (undo.needs_approval, undo.has_side_effects) == (False, True)
    with pytest.raises(ValueError, match="effect 'read'"):
        ToolDefinition("w", "W.", WriteArgs, frozenset({Capability.WRITE_DATA}), write)
    with pytest.raises(ValueError, match="cannot opt out"):
        ToolDefinition(
            "w", "W.", WriteArgs, frozenset({Capability.WRITE_DATA}), write,
            effect=EffectClass.WRITE, requires_approval=False,
        )  # fmt: skip
    with pytest.raises(ValueError, match="without approval"):
        ToolDefinition(
            "u", "U.", WriteArgs, frozenset({Capability.WRITE_DATA}), write,
            effect=EffectClass.COMPENSATE, requires_approval=True,
        )  # fmt: skip
    with pytest.raises(ValueError, match="extra='forbid'"):
        ToolDefinition("o", "O.", OpenArgs, frozenset({Capability.READ_DATA}), lambda a, c: "")
    with pytest.raises(ValueError, match="invalid tool name"):
        ToolDefinition("bad name", "x", EchoArgs, frozenset({Capability.READ_DATA}), echo)
    with pytest.raises(ValueError, match="at least one capability"):
        ToolDefinition("none", "x", EchoArgs, frozenset(), echo)


@pytest.mark.parametrize(
    "arguments",
    [
        {"text": "hi", "extra": "SECRETVALUE"},
        {"text": "hi", "count": "3"},
        {"text": "SECRETVALUE" * 3},
        {},
    ],
)
def test_invalid_arguments_are_rejected_without_echoing_values(
    arguments: dict[str, JsonValue],
) -> None:
    with pytest.raises(ToolArgumentError) as info:
        make_registry().validate_call(call("echo", **arguments))
    assert "SECRETVALUE" not in str(info.value)


def test_valid_calls_are_canonicalized_bound_to_their_hash_and_runnable() -> None:
    validated = make_registry().validate_call(call("echo", text="hi"))
    assert validated.canonical_args == {"text": "hi", "count": 1}
    assert validated.call_hash == tool_call_hash("echo", {"count": 1, "text": "hi"})
    assert validated.run(CTX) == "hi"
    written = make_registry().validate_call(call("write_file", path="a.txt"))
    assert written.run(CTX, authorized_hash=written.call_hash) == "wrote a.txt with key run-1:3"
    with pytest.raises(ToolArgumentError, match="after authorization"):
        written.run(CTX, authorized_hash="0" * 64)
    args = written.args
    assert isinstance(args, WriteArgs)
    args.path = "/etc/passwd"
    with pytest.raises(ToolArgumentError, match="changed after validation"):
        written.run(CTX, authorized_hash=written.call_hash)


def test_unknown_tools_and_scoped_allowlists() -> None:
    registry = make_registry()
    with pytest.raises(ToolNotAllowedError, match="delete_all"):
        registry.validate_call(call("delete_all"))
    scoped = registry.scoped(["echo"])
    assert scoped.names == ("echo",)
    assert "echo" in scoped
    assert "write_file" not in scoped
    assert scoped.granted == frozenset({Capability.READ_DATA})
    with pytest.raises(ValueError, match="unknown tools"):
        registry.scoped(["echo", "missing"])
    specs = registry.specs()
    assert [spec.name for spec in specs] == ["echo", "write_file"]
    assert all(spec.input_schema.get("additionalProperties") is False for spec in specs)


class TokenArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")
    token: SecretStr


def test_argument_models_that_do_not_round_trip_are_refused() -> None:
    registry = ToolRegistry({Capability.READ_DATA})
    registry.register(
        ToolDefinition(
            "use_token", "Use.", TokenArgs, frozenset({Capability.READ_DATA}), lambda a, c: "ok"
        )
    )
    with pytest.raises(ToolArgumentError, match="canonical") as info:
        registry.validate_call(call("use_token", token="s3cr3t-value"))
    assert "s3cr3t-value" not in str(info.value)


_args = st.dictionaries(st.text(max_size=5), st.integers() | st.text(max_size=5), max_size=4)


@given(_args, _args)
def test_call_hash_equality_coincides_with_canonical_equality(
    a: dict[str, int | str], b: dict[str, int | str]
) -> None:
    same_hash = tool_call_hash("t", a) == tool_call_hash("t", b)
    assert same_hash == (canonical_json(a) == canonical_json(b))


# ------------------------------------------------------------------ workspace jail


@pytest.mark.parametrize(
    "path",
    [
        "../outside.txt",
        "notes/../../outside.txt",
        "/etc/passwd",
        "\\windows\\win.ini",
        "C:\\Windows\\win.ini",
        "C:relative.txt",
        "\\\\server\\share\\file",
        "file.txt:stream",
        "nul\x00byte",
        "./file.txt",
        "",
        "a" * 300,
    ],
)
def test_the_file_jail_refuses_paths_that_leave_the_workspace(tmp_path: Path, path: str) -> None:
    jail = WorkspaceJail(tmp_path / "ws")
    with pytest.raises(ToolPolicyError) as info:
        jail.resolve(path)
    assert path not in str(info.value) or not path


def test_the_file_jail_resolves_relative_paths_inside(tmp_path: Path) -> None:
    jail = WorkspaceJail(tmp_path / "ws")
    target = jail.resolve("notes/today.md")
    assert target == (tmp_path / "ws" / "notes" / "today.md").resolve()


def test_symlinks_that_resolve_outside_the_workspace_are_refused(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_text("secret", encoding="utf-8")
    jail = WorkspaceJail(tmp_path / "ws")
    link = jail.root / "link"
    try:
        os.symlink(outside, link, target_is_directory=True)
    except OSError:
        if sys.platform == "win32":
            pytest.skip("creating symlinks needs developer mode or admin rights on Windows")
        raise
    with pytest.raises(ToolPolicyError, match="escapes"):
        jail.resolve("link/secret.txt")


# ------------------------------------------------------------------ egress allowlist


@pytest.mark.parametrize(
    ("url", "reason"),
    [
        ("ftp://kb.internal/file", "only http and https"),
        ("http://evil.example/search", "allowlist"),
        ("http://kb.internal@evil.example/", "credentials"),
        ("http://user:pw@kb.internal/", "credentials"),
        ("http://127.0.0.1/", "IP address"),
        ("http://[::1]/", "IP address"),
        ("http://kb.internal:8080/", "non-default ports"),
        ("http://kb.internal.evil.example/", "allowlist"),
        ("http://KB.internal\\@evil/", "backslashes"),
        ("http://kb.internal/ search", "whitespace"),
        ("http://kb.internal:99999/", "malformed"),
        ("http:///nohost", "valid hostname"),
    ],
)
def test_egress_is_limited_to_exact_allowlisted_hosts(url: str, reason: str) -> None:
    with pytest.raises(ToolPolicyError, match=reason):
        EgressPolicy(["kb.internal"]).check(url)


def test_allowlisted_hosts_resolve_to_in_process_fake_services() -> None:
    policy = EgressPolicy(["kb.internal", "status.internal", "unrouted.internal"])
    host, path, query = policy.check("https://KB.internal/search?q=cache")
    transport = FakeServiceTransport(default_services())
    status, body = transport.get(host, path, query)
    assert (host, status) == ("kb.internal", 200)
    assert "cache-sizing" in body
    assert transport.get(*policy.check("http://kb.internal/articles/rollback-guide"))[0] == 200
    assert transport.get(*policy.check("http://kb.internal/articles/missing"))[0] == 404
    assert transport.get(*policy.check("http://kb.internal/other"))[0] == 404
    assert transport.get(*policy.check("http://status.internal/cache"))[0] == 200
    assert transport.get(*policy.check("http://status.internal/other"))[0] == 404
    assert transport.get(*policy.check("http://unrouted.internal/"))[0] == 502
    assert transport.get(*policy.check("http://kb.internal/search?q=zzz"))[1] == "no results"
