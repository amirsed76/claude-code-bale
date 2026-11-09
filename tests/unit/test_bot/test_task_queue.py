"""Crash recovery, continuation, stop persistence, and delivery isolation."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from telegram.error import NetworkError

from src.bot.task_queue import TaskQueue
from src.claude.exceptions import ClaudeTimeoutError
from src.claude.sdk_integration import ClaudeResponse


def response(**kwargs):
    return ClaudeResponse(
        **{
            "content": "Finished",
            "session_id": "real-cli-session",
            "cost": 0.1,
            "duration_ms": 10,
            "num_turns": 1,
            "result_subtype": "success",
            **kwargs,
        }
    )


async def drain(queue):
    await asyncio.wait_for(asyncio.gather(*list(queue.workers.values())), 5)


@pytest.fixture
async def queue(tmp_path):
    q = TaskQueue(
        tmp_path / "tasks.sqlite",
        AsyncMock(return_value=response()),
        AsyncMock(),
        AsyncMock(return_value=True),
    )
    await q.initialize()
    yield q
    await q.close()


async def test_turn_limit_continues_same_session_and_delivers_only_final(queue):
    queue.execute.side_effect = [
        response(content="Partial", result_subtype="error_max_turns"),
        response(),
    ]
    assert await queue.submit(42, {"original_task": "Build it"})
    await drain(queue)
    assert queue.execute.await_count == 2
    first, second = [call.args[0] for call in queue.execute.await_args_list]
    assert first.attempts == 0
    assert second.attempts == 1
    assert second.session_id == "real-cli-session"
    assert second.payload == first.payload
    queue.deliver.assert_awaited_once()
    assert queue.deliver.call_args.args[0].response.content == "Finished"
    assert (await queue.get(first.id)).state == "done"


@pytest.mark.parametrize(
    "stop_response",
    [
        response(result_subtype="error_max_budget_usd"),
        response(content="Which repository should I use?"),
        response(
            result_subtype="error_max_turns", permission_denials=[{"tool_name": "Bash"}]
        ),
    ],
)
async def test_budget_question_and_denied_tools_do_not_auto_continue(
    queue, stop_response
):
    queue.execute.return_value = stop_response
    await queue.submit(42, {})
    await drain(queue)
    queue.execute.assert_awaited_once()
    queue.deliver.assert_awaited_once()


async def test_overlapping_user_tasks_rejected(queue):
    entered = asyncio.Event()

    async def execute(job, event):
        entered.set()
        await event.wait()
        return response(interrupted=True)

    queue.execute.side_effect = execute
    await queue.submit(42, {})
    await entered.wait()
    assert not await queue.submit(42, {})
    assert await queue.stop(42)
    await drain(queue)
    assert (await queue.get(1)).state == "stopped"


async def test_restart_resumes_checkpoint_even_without_final_result(tmp_path):
    path = tmp_path / "tasks.sqlite"
    started = asyncio.Event()

    async def execute(job, event):
        await first.checkpoint(job.id, "session-from-init")
        started.set()
        await asyncio.Event().wait()

    first = TaskQueue(path, execute, AsyncMock(), AsyncMock(return_value=True))
    await first.initialize()
    await first.submit(42, {"original_task": "Build it"})
    await started.wait()
    await first.close()
    assert (await first.get(1)).state == "running"
    second = TaskQueue(
        path,
        AsyncMock(return_value=response()),
        AsyncMock(),
        AsyncMock(return_value=True),
    )
    await second.initialize()
    await second.recover()
    await drain(second)
    job = second.execute.call_args.args[0]
    assert job.session_id == "session-from-init"
    assert job.attempts == 1
    await second.close()


async def test_stop_committed_before_crash_does_not_resume(queue):
    started = asyncio.Event()

    async def execute(job, event):
        await queue.checkpoint(job.id, "real-session")
        started.set()
        await asyncio.Event().wait()

    queue.execute.side_effect = execute
    await queue.submit(42, {})
    await started.wait()
    assert await queue.stop(42)
    await queue.close()
    recovered = TaskQueue(
        queue.path, AsyncMock(), AsyncMock(), AsyncMock(return_value=True)
    )
    await recovered.initialize()
    await recovered.recover()
    assert recovered.workers == {}
    assert (await recovered.get(1)).state == "stopped"
    await recovered.close()


async def test_timeout_resumes_saved_session(queue):
    calls = 0

    async def execute(job, event):
        nonlocal calls
        calls += 1
        if calls == 1:
            await queue.checkpoint(job.id, "saved-before-timeout")
            raise ClaudeTimeoutError("time limit")
        assert job.session_id == "saved-before-timeout"
        return response()

    queue.execute.side_effect = execute
    await queue.submit(42, {})
    await drain(queue)
    assert calls == 2


async def test_recovery_without_session_does_not_replay_ambiguous_work(queue):
    await queue.submit(42, {})
    await drain(queue)
    await queue._update(1, state="running", session_id=None, attempts=1)
    queue.execute.reset_mock()
    await queue.recover()
    await drain(queue)
    queue.execute.assert_not_awaited()
    assert "inspect the workspace" in queue.deliver.call_args.args[0].response.content


async def test_delivery_network_failure_never_reexecutes(queue):
    queue.deliver.side_effect = [NetworkError("offline"), None]
    with patch.object(queue, "_delivery_delay", new=AsyncMock()):
        await queue.submit(42, {})
        await drain(queue)
    queue.execute.assert_awaited_once()
    assert queue.deliver.await_count == 2
    assert (await queue.get(1)).state == "done"


async def test_pending_delivery_survives_restart(queue):
    await queue.submit(42, {})
    await drain(queue)
    await queue._update(1, state="delivering")
    queue.execute.reset_mock()
    queue.deliver.reset_mock()
    await queue.recover()
    await drain(queue)
    queue.execute.assert_not_awaited()
    queue.deliver.assert_awaited_once()


async def test_repeated_identical_tool_attempts_pause(queue):
    queue.execute.side_effect = [
        response(
            result_subtype="error_max_turns",
            tools_used=[{"id": str(i), "name": "Bash", "input": {"command": "false"}}],
        )
        for i in range(3)
    ]
    await queue.submit(42, {})
    await drain(queue)
    assert queue.execute.await_count == 3
    assert "without progress" in queue.deliver.call_args.args[0].response.content


async def test_recovery_rechecks_authorization(queue):
    await queue.submit(42, {})
    await drain(queue)
    await queue._update(1, state="running")
    queue.authorize.return_value = False
    queue.execute.reset_mock()
    await queue.recover()
    await drain(queue)
    queue.execute.assert_not_awaited()
    assert (await queue.get(1)).state == "paused"


async def test_background_orchestrator_uses_isolated_context_and_continuation(tmp_path):
    from collections import defaultdict
    from types import SimpleNamespace

    from telegram import Bot, Update

    from src.bot.orchestrator import MessageOrchestrator
    from src.bot.task_queue import CONTINUE_PROMPT, TaskJob
    from src.config.settings import Settings

    bot = Bot("123:fake-token", request=AsyncMock(), get_updates_request=AsyncMock())
    update = Update.de_json(
        {
            "update_id": 5,
            "message": {
                "message_id": 10,
                "date": 1700000000,
                "chat": {"id": 42, "type": "private"},
                "from": {"id": 42, "is_bot": False, "first_name": "Test"},
                "text": "Build the app",
            },
        },
        bot,
    )
    settings = Settings(
        telegram_bot_token="test:token",
        telegram_bot_username="testbot",
        approved_directory=tmp_path,
        enable_background_tasks=True,
    )
    orchestrator = MessageOrchestrator(settings, {})
    shared = defaultdict(dict)
    shared[42]["force_new_session"] = True
    orchestrator._task_application = SimpleNamespace(bot=bot, user_data=shared)
    orchestrator._execute_agentic_text = AsyncMock(return_value=response())
    job = TaskJob(
        1,
        42,
        {
            "update": update.to_dict(),
            "directory": str(tmp_path),
            "user_data": {"force_new_session": True},
        },
        "checkpointed-cli",
        2,
        "running",
    )
    await orchestrator._execute_background_task(job, asyncio.Event())
    args = orchestrator._execute_agentic_text.call_args
    assert args.kwargs["prompt"] == CONTINUE_PROMPT
    isolated_context = args.args[1]
    assert isolated_context.user_data["claude_session_id"] == "checkpointed-cli"
    assert isolated_context.user_data["force_new_session"] is False
    assert shared[42]["force_new_session"] is True


async def test_stop_while_delivery_offline_is_prompt_and_persistent(queue):
    entered = asyncio.Event()

    async def offline(job):
        entered.set()
        raise NetworkError("offline")

    queue.deliver.side_effect = offline
    await queue.submit(42, {})
    await entered.wait()
    assert await queue.stop(42)
    await asyncio.wait_for(drain(queue), 1)
    assert (await queue.get(1)).state == "stopped"
    queue.execute.assert_awaited_once()
