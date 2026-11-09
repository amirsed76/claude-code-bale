# Persistent background text tasks

Set `ENABLE_BACKGROUND_TASKS=true` in the bot's environment and restart the
service. This applies to text messages in agentic mode; file, image, and voice
handlers retain their existing execution model.

Each accepted text request is committed to the `background_tasks` table in the
configured SQLite database, then executed by a background worker. The update
handler returns immediately, so `/status`, `/stop`, and other users' messages
remain responsive. Each user can have one active task. Send `/stop` or press its
Stop button before starting another task or changing projects/sessions.

`CLAUDE_MAX_TURNS` and `CLAUDE_TIMEOUT_SECONDS` limit an individual batch, not the
whole task. A turn-limit result or interrupted connection/timeout with a saved
session resumes that same CLI session with a continuation instruction. There is
no fixed maximum number of successful batches. A normal final response ends the
task, including a question requiring user input. The bot does not infer whether
an ordinary final response actually fulfilled the user's request.

The CLI's session ID is persisted when its initialization event arrives, rather
than waiting for the final result. After a restart, queued/running tasks resume;
finished tasks waiting for message delivery only resend their saved response.
Authorization and project directory/topic access are checked again. Shutdown
cancels the running CLI connection and preserves the task checkpoint. Stop is
committed before signalling cancellation and remains stopped after a restart.

Cost budgets and tool permission checks remain in effect. A budget result or
permission denial is delivered to the user without automatic continuation. Five
consecutive execution failures or three identical turn-limit results pause
continuation and explain the problem. New successful batches reset the failure
counter. Connection retries use increasing delays; `/stop` remains available.
If execution was interrupted before any CLI session ID was saved, the bot asks
the user to inspect the workspace rather than replaying uncertain operations.

Result delivery retries transient platform errors without executing Claude
again. Delivery is at least once: a crash after a successful send but before the
delivery checkpoint may produce a duplicate message. Resuming an interrupted CLI
session cannot guarantee exactly-once external effects; continuation instructions
require inspecting prior work before repeating commands. Do not run two bot
processes against the same queue database. Back up the database together with
Claude's local session data and the workspace.

Intermediate tool progress and Stop buttons still appear while batches run.
Intermediate limit warnings are not sent as final responses. `/stop` works
between batches and during reconnect delays, even without a current button.
