# Security policy

## Supported versions

Only the latest commit on `main` is supported. This is a portfolio project, not a product with
release branches.

## Reporting a vulnerability

Please do not open a public issue for a security problem. Use GitHub's private vulnerability
reporting ("Report a vulnerability" under the repository's Security tab). Include:

- the affected commit and component;
- steps to reproduce, ideally as a failing test;
- the impact you expect.

You can expect an acknowledgement within 7 days and a fix or a written assessment within 30 days.
Credit is given in the fix commit unless you ask otherwise.

## Scope

In scope: the code in this repository, its container image and its CI configuration. Of particular
interest: ways to make a run execute a write that no second principal approved, to replay a run
past a tampered journal without detection, to apply an effect twice outside the documented crash
window, or to bypass the file jail or egress allowlist.

Out of scope: denial of service by traffic volume, findings that need write access to the journal
database file (that access is equivalent to full control and is documented in
docs/THREAT_MODEL.md), and issues in third-party dependencies that are already publicly tracked
(please report those upstream).

## How this repository is hardened

- Threats, controls and the tests that prove them: [docs/THREAT_MODEL.md](docs/THREAT_MODEL.md);
  runtime guarantees: [docs/SEMANTICS.md](docs/SEMANTICS.md).
- Every security claim is linked to evidence in [CLAIMS.md](CLAIMS.md).
- API access uses local-issuer HS256 JWTs with scopes; the signing secret comes only from the
  environment and the API fails closed without it.
- Write tools block on approvals bound to the exact call hash and decided by a different principal.
- The journal is hash-chained, verified before every replay and verifiable with `rar audit verify`.
- CI runs ruff, mypy --strict, pytest with a coverage floor, bandit, semgrep (registry packs plus
  `.semgrep/rules.yml`), pip-audit, gitleaks, hadolint, a trivy image scan and a syft CycloneDX SBOM.
- Artifacts and state use pickle-free formats (JSON and SQLite).
