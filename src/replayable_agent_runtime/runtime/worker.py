"""Workers: claim runs under a lease, heartbeat it, activate, and wake due timers.

Journal appends are synchronous SQLite transactions made from the activation's coroutine, so a slow
append blocks the worker's event loop for its duration. Lease renewal therefore runs on a thread of
its own (the journal is thread-safe): a stalled loop cannot starve the heartbeat and lose a healthy
worker's run to a reclaim (docs/adr/0006-synchronous-journal-io-and-an-out-of-loop-heartbeat.md).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
import threading
import time
from dataclasses import dataclass, field

from replayable_agent_runtime.core.logging import get_logger
from replayable_agent_runtime.runtime.engine import ActivationOutcome, Runtime, activate
from replayable_agent_runtime.runtime.journal import Lease

__all__ = ["Worker", "WorkerHealth", "default_worker_id"]

_log = get_logger("replayable_agent_runtime.worker")


def default_worker_id() -> str:
    return f"{socket.gethostname()[:40]}-{os.getpid()}"


@dataclass
class WorkerHealth:
    """Loop heartbeats for ``/readyz``: monotonic times of the last scheduler and lease ticks."""

    last_scheduler_tick: float = 0.0
    last_lease_tick: float = 0.0
    started: bool = False
    stale_after_seconds: float = 10.0
    clock: object = field(default=time.monotonic, repr=False)

    def _now(self) -> float:
        return time.monotonic()

    def scheduler_tick(self) -> None:
        self.last_scheduler_tick = self._now()

    def lease_tick(self) -> None:
        self.last_lease_tick = self._now()

    def scheduler_ok(self) -> bool:
        return self.started and self._now() - self.last_scheduler_tick < self.stale_after_seconds

    def lease_ok(self) -> bool:
        return self.started and self._now() - self.last_lease_tick < self.stale_after_seconds


class Worker:
    def __init__(
        self,
        runtime: Runtime,
        *,
        worker_id: str | None = None,
        lease_seconds: float = 30.0,
        heartbeat_seconds: float | None = 10.0,
        poll_seconds: float = 0.5,
        health: WorkerHealth | None = None,
    ) -> None:
        if heartbeat_seconds is not None and heartbeat_seconds >= lease_seconds:
            raise ValueError("heartbeat_seconds must be shorter than lease_seconds")
        self.runtime = runtime
        self.worker_id = worker_id or default_worker_id()
        self.lease_seconds = lease_seconds
        self.heartbeat_seconds = heartbeat_seconds
        self.poll_seconds = poll_seconds
        self.health = health or WorkerHealth(stale_after_seconds=max(10.0, poll_seconds * 20))

    def _heartbeat(self, lease: Lease, interval: float, stop: threading.Event) -> None:
        """Renew the lease every ``interval`` seconds until ``stop`` is set or renewal fails."""
        while not stop.wait(interval):
            renewed = self.runtime.journal.renew(lease, self.lease_seconds)
            self.health.lease_tick()
            if not renewed:
                # After the activation released the run a late renewal is expected to fail.
                if not stop.is_set():
                    _log.warning("lease_renewal_failed", run_id=lease.run_id, fencing=lease.fencing)
                return

    async def run_once(self, *, run_id: str | None = None) -> ActivationOutcome | None:
        """Wake due timers, claim one runnable run and activate it."""
        journal = self.runtime.journal
        journal.wake_due()
        self.health.scheduler_tick()
        claim = journal.claim(self.worker_id, self.lease_seconds, run_id=run_id)
        self.health.lease_tick()
        if claim is None:
            return None
        stop = threading.Event()
        heartbeat: threading.Thread | None = None
        if self.heartbeat_seconds is not None:
            heartbeat = threading.Thread(
                target=self._heartbeat,
                args=(claim.lease, self.heartbeat_seconds, stop),
                name=f"rar-heartbeat-{claim.lease.run_id}",
                daemon=True,
            )
            heartbeat.start()
        try:
            outcome = await activate(self.runtime, claim)
        finally:
            stop.set()
            if heartbeat is not None:
                await asyncio.to_thread(heartbeat.join)
        self.runtime.metrics.activations_total.labels(outcome.outcome).inc()
        _log.info(
            "activation",
            run_id=outcome.run_id,
            outcome=outcome.outcome,
            fencing=outcome.fencing,
            reclaimed=claim.reclaimed,
        )
        return outcome

    async def run_until_idle(self, *, max_activations: int = 10_000) -> list[ActivationOutcome]:
        """Activate runs until nothing is runnable (waiting runs stay parked)."""
        outcomes: list[ActivationOutcome] = []
        while len(outcomes) < max_activations:
            outcome = await self.run_once()
            if outcome is None:
                break
            outcomes.append(outcome)
        return outcomes

    async def _scheduler_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            self.runtime.journal.wake_due()
            self.health.scheduler_tick()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)

    async def serve(self, stop: asyncio.Event) -> None:
        """Run the scheduler loop and the claim/lease loop until ``stop`` is set."""
        self.health.started = True
        self.health.scheduler_tick()
        self.health.lease_tick()
        scheduler = asyncio.create_task(self._scheduler_loop(stop))
        try:
            while not stop.is_set():
                outcome = await self.run_once()
                if outcome is None:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
        finally:
            stop.set()
            await scheduler
