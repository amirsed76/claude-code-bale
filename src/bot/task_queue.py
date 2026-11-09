"""Persistent, single-task-per-user execution for agentic text requests.

The queue owns execution separately from delivery. A crash after execution may
redeliver a response, but never starts the finished task again. CLI session IDs
are checkpointed before tools run, allowing interrupted attempts to resume.
"""

import asyncio
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

import aiosqlite
import structlog
from telegram.error import BadRequest, NetworkError, RetryAfter

from ..claude.exceptions import ClaudeProcessError, ClaudeTimeoutError
from ..claude.sdk_integration import ClaudeResponse

logger = structlog.get_logger()

CONTINUE_PROMPT = (
    "Continue the original user task from the saved conversation and current "
    "workspace state. The previous run was interrupted by an execution limit "
    "or connection loss. Inspect what already succeeded before taking action; "
    "do not repeat completed commands or external side effects. Finish the "
    "remaining work. If blocked or you need user input, explain and stop."
)


@dataclass
class TaskJob:
    """Durable input and current execution checkpoint."""

    id: int
    user_id: int
    payload: Dict[str, Any]
    session_id: Optional[str]
    attempts: int
    state: str
    response: Optional[ClaudeResponse] = None


class TaskQueue:
    """One process owns the queue; SQLite survives process and host restarts."""

    def __init__(
        self,
        path: Path,
        execute: Callable[[TaskJob, asyncio.Event], Awaitable[ClaudeResponse]],
        deliver: Callable[[TaskJob], Awaitable[None]],
        authorize: Callable[[TaskJob], Awaitable[bool]],
    ) -> None:
        self.path = path
        self.execute = execute
        self.deliver = deliver
        self.authorize = authorize
        self.workers: Dict[int, asyncio.Task] = {}
        self.events: Dict[int, asyncio.Event] = {}
        self.closing = False

    async def initialize(self) -> None:
        """Create the additive table without changing existing storage tables."""
        async with aiosqlite.connect(self.path) as db:
            await db.executescript(
                """
                CREATE TABLE IF NOT EXISTS background_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    session_id TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'queued',
                    response TEXT,
                    fingerprint TEXT,
                    repeats INTEGER NOT NULL DEFAULT 0,
                    failures INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                CREATE UNIQUE INDEX IF NOT EXISTS background_tasks_active_user
                ON background_tasks(user_id)
                WHERE state IN ('queued', 'running', 'delivering');
                """
            )
            await db.commit()

    async def recover(self) -> None:
        """Resume accepted work after startup, including pending delivery."""
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                "SELECT id, user_id FROM background_tasks "
                "WHERE state IN ('queued', 'running', 'delivering') ORDER BY id"
            )
            for job_id, user_id in await cursor.fetchall():
                self._spawn(job_id, user_id)

    async def submit(self, user_id: int, payload: Dict[str, Any]) -> bool:
        """Commit acceptance before starting; reject overlapping user requests."""
        if self.closing:
            return False
        async with aiosqlite.connect(self.path) as db:
            try:
                cursor = await db.execute(
                    "INSERT INTO background_tasks(user_id, payload) VALUES (?, ?)",
                    (user_id, json.dumps(payload)),
                )
                await db.commit()
            except aiosqlite.IntegrityError:
                return False
            job_id = cursor.lastrowid
        previous = self.workers.get(user_id)
        if previous is not None:
            await asyncio.shield(previous)
        self._spawn(job_id, user_id)
        return True

    def _spawn(self, job_id: int, user_id: int) -> None:
        if user_id in self.workers:
            return
        self.events[user_id] = asyncio.Event()
        self.workers[user_id] = asyncio.create_task(
            self._worker(job_id, user_id), name=f"background-task-{job_id}"
        )

    async def latest(self, user_id: int) -> Optional[TaskJob]:
        """Return the last persisted task for status reporting."""
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                "SELECT id FROM background_tasks WHERE user_id = ? "
                "ORDER BY id DESC LIMIT 1",
                (user_id,),
            )
            row = await cursor.fetchone()
        return await self.get(row[0]) if row else None

    async def get(self, job_id: int) -> TaskJob:
        """Read the last committed checkpoint."""
        async with aiosqlite.connect(self.path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM background_tasks WHERE id = ?", (job_id,)
            )
            row = await cursor.fetchone()
        return TaskJob(
            id=row["id"],
            user_id=row["user_id"],
            payload=json.loads(row["payload"]),
            session_id=row["session_id"],
            attempts=row["attempts"],
            state=row["state"],
            response=(
                ClaudeResponse(**json.loads(row["response"]))
                if row["response"]
                else None
            ),
        )

    async def _update(self, job_id: int, **values: Any) -> None:
        # Callers supply only internal column names; values are always bound.
        columns = ", ".join(f"{key} = ?" for key in values)
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                f"UPDATE background_tasks SET {columns}, "
                "updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (*values.values(), job_id),
            )
            await db.commit()

    async def save_payload(self, job: TaskJob) -> None:
        """Save output attachments and routing metadata before delivery."""
        await self._update(job.id, payload=json.dumps(job.payload))

    async def checkpoint(self, job_id: int, session_id: str) -> None:
        """Persist the real CLI session ID immediately, including new sessions."""
        await self._update(job_id, session_id=session_id)

    async def stop(self, user_id: int) -> bool:
        """Commit a stop before signalling, so a restart cannot undo it."""
        async with aiosqlite.connect(self.path) as db:
            cursor = await db.execute(
                "UPDATE background_tasks SET state = 'stopped', "
                "updated_at = CURRENT_TIMESTAMP "
                "WHERE user_id = ? AND state IN ('queued', 'running', 'delivering')",
                (user_id,),
            )
            await db.commit()
            stopped = cursor.rowcount > 0
        if stopped and user_id in self.events:
            self.events[user_id].set()
        return stopped

    async def _deliver_with_retry(self, job: TaskJob) -> None:
        """Reconnect messaging without ever re-executing the completed task."""
        delay = 1
        while True:
            event = self.events.get(job.user_id)
            if event is not None and event.is_set():
                return
            try:
                await self.deliver(job)
                return
            except RetryAfter as error:
                seconds = error.retry_after
                if hasattr(seconds, "total_seconds"):
                    seconds = seconds.total_seconds()
                await self._delivery_delay(job.user_id, float(seconds))
            except BadRequest:
                raise  # Invalid payloads cannot be repaired by reconnecting.
            except NetworkError:
                await self._delivery_delay(job.user_id, delay)
                delay = min(delay * 2, 30)

    async def _delivery_delay(self, user_id: int, seconds: float) -> None:
        event = self.events.get(user_id)
        if event is None:
            await asyncio.sleep(seconds)
            return
        try:
            await asyncio.wait_for(event.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    async def _mark_delivered(self, job_id: int) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE background_tasks SET state = 'done' "
                "WHERE id = ? AND state = 'delivering'",
                (job_id,),
            )
            await db.commit()

    async def _finish(self, job: TaskJob, response: ClaudeResponse) -> None:
        async with aiosqlite.connect(self.path) as db:
            await db.execute(
                "UPDATE background_tasks SET state = CASE WHEN state = 'stopped' "
                "THEN 'stopped' ELSE 'delivering' END, response = ? WHERE id = ?",
                (json.dumps(asdict(response)), job.id),
            )
            await db.commit()
        current = await self.get(job.id)
        if current.state == "stopped":
            current.response.interrupted = True
        await self._deliver_with_retry(current)
        if current.state == "delivering":
            await self._mark_delivered(job.id)

    async def _worker(self, job_id: int, user_id: int) -> None:
        event = self.events[user_id]
        try:
            while not self.closing:
                job = await self.get(job_id)
                if job.state not in ("queued", "running", "delivering"):
                    return
                if not await self.authorize(job):
                    await self._update(
                        job_id,
                        state="paused",
                        response=json.dumps(
                            asdict(
                                self._notice(
                                    "Task paused: user or project access "
                                    "is no longer available."
                                )
                            )
                        ),
                    )
                    await self._deliver_with_retry(await self.get(job_id))
                    return
                if job.state == "delivering":
                    await self._deliver_with_retry(job)
                    await self._mark_delivered(job_id)
                    return
                if job.attempts and not job.session_id:
                    await self._finish(
                        job,
                        self._notice(
                            "The run stopped before its session could be saved. "
                            "Please inspect the workspace before retrying."
                        ),
                    )
                    return
                await self._update(job_id, state="running", attempts=job.attempts + 1)
                try:
                    response = await self.execute(job, event)
                except (ClaudeTimeoutError, ClaudeProcessError):
                    current = await self.get(job_id)
                    if current.state == "stopped" or event.is_set():
                        await self._finish(current, self._notice("Stopped by user."))
                        return
                    if not current.session_id:
                        await self._finish(
                            current,
                            self._notice(
                                "Connection lost before a session was saved. "
                                "Please inspect the workspace before retrying."
                            ),
                        )
                        return
                    async with aiosqlite.connect(self.path) as db:
                        await db.execute(
                            "UPDATE background_tasks SET failures = failures + 1 "
                            "WHERE id = ?",
                            (job_id,),
                        )
                        cursor = await db.execute(
                            "SELECT failures FROM background_tasks WHERE id = ?",
                            (job_id,),
                        )
                        failures = (await cursor.fetchone())[0]
                        await db.commit()
                    if failures >= 5:
                        await self._finish(
                            current,
                            self._notice(
                                "Repeated execution failures; automatic continuation "
                                "paused. Send a message after resolving the problem."
                            ),
                        )
                        return
                    # Stop remains responsive during reconnect backoff.
                    try:
                        await asyncio.wait_for(event.wait(), min(2**failures, 30))
                    except asyncio.TimeoutError:
                        pass
                    continue
                if response.session_id:
                    await self.checkpoint(job_id, response.session_id)
                if event.is_set() or response.interrupted:
                    response.interrupted = True
                    await self._finish(job, response)
                    return
                # Cost caps and denied tools require user intervention, not retries.
                if (
                    response.result_subtype != "error_max_turns"
                    and response.terminal_reason != "max_turns"
                ) or response.permission_denials:
                    await self._finish(job, response)
                    return
                fingerprint = hashlib.sha256(
                    json.dumps(
                        [
                            response.content,
                            [
                                {"name": t.get("name"), "input": t.get("input")}
                                for t in response.tools_used
                            ],
                        ],
                        sort_keys=True,
                    ).encode()
                ).hexdigest()
                async with aiosqlite.connect(self.path) as db:
                    await db.execute(
                        "UPDATE background_tasks SET failures = 0, "
                        "repeats = CASE WHEN fingerprint = ? THEN repeats + 1 "
                        "ELSE 0 END, fingerprint = ? WHERE id = ?",
                        (fingerprint, fingerprint, job_id),
                    )
                    cursor = await db.execute(
                        "SELECT repeats FROM background_tasks WHERE id = ?", (job_id,)
                    )
                    repeats = (await cursor.fetchone())[0]
                    await db.commit()
                if repeats >= 2:
                    await self._finish(
                        job,
                        self._notice(
                            "The same result repeated without progress. "
                            "Automatic continuation paused; please check the task."
                        ),
                    )
                    return
        except asyncio.CancelledError:
            # Leave running/delivering in SQLite for recovery, not user-stopped.
            raise
        except Exception:
            # Execution errors pause; delivery errors leave 'delivering' recoverable.
            logger.exception("Background task failed", job_id=job_id)
            job = await self.get(job_id)
            if job.state != "delivering":
                await self._update(job_id, state="paused")
                try:
                    await self.deliver(
                        TaskJob(
                            job.id,
                            job.user_id,
                            job.payload,
                            job.session_id,
                            job.attempts,
                            "paused",
                            self._notice(
                                "Task paused after an unexpected error. "
                                "Send a message to continue."
                            ),
                        )
                    )
                except Exception:
                    logger.exception("Could not deliver task error", job_id=job_id)
        finally:
            self.workers.pop(user_id, None)
            self.events.pop(user_id, None)

    @staticmethod
    def _notice(content: str) -> ClaudeResponse:
        return ClaudeResponse(content, "", 0, 0, 0)

    async def close(self) -> None:
        """Cancel processes but preserve checkpoints for the next startup."""
        self.closing = True
        workers = list(self.workers.values())
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
