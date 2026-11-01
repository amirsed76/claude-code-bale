"""Exercise Bale through PTB serialization without external API calls."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import ValidationError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.request import BaseRequest

from src.bot.bale import BaleBot, create_bale_bot, plain_html
from src.bot.core import ClaudeCodeBot
from src.config import create_test_config


class BaleRequest(BaseRequest):
    def __init__(self):
        self.calls = []

    @property
    def read_timeout(self):
        return 30

    async def initialize(self):
        pass

    async def shutdown(self):
        pass

    async def do_request(self, url, method, request_data=None, **kwargs):
        payload = request_data.json_parameters if request_data else {}
        files = request_data.multipart_data if request_data else {}
        self.calls.append((url, payload, files))
        endpoint = url.rsplit("/", 1)[-1]
        message = {
            "message_id": 10,
            "date": 1,
            "chat": {"id": 123, "type": "private"},
            "text": "response",
        }
        if endpoint == "getMe":
            result = {"id": 1, "is_bot": True, "first_name": "Bale"}
        elif endpoint == "getUpdates":
            result = []
        elif endpoint == "getFile":
            result = {"file_id": "f", "file_unique_id": "u", "file_path": "docs/f"}
        elif endpoint == "sendMediaGroup":
            result = [message]
        elif endpoint in {"answerCallbackQuery", "deleteWebhook"}:
            result = True
        else:
            result = message
        return 200, json.dumps({"ok": True, "result": result}).encode()


@pytest.fixture
async def bale():
    request = BaleRequest()
    polling = BaleRequest()
    bot = BaleBot(
        token="123:fake-token",
        base_url="https://tapi.bale.ai/bot",
        base_file_url="https://tapi.bale.ai/file/bot",
        request=request,
        get_updates_request=polling,
    )
    async with bot:
        yield bot, request, polling


async def test_message_reply_buttons_and_edits(bale):
    bot, request, _ = bale
    await bot.send_message(
        123,
        "<b>اجرا</b> <code>&lt;x&gt; &amp; $HOME</code>",
        parse_mode="HTML",
        reply_to_message_id=8,
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("Allow", callback_data="approve:1")]]
        ),
    )
    url, payload, _ = request.calls[-1]
    assert url == "https://tapi.bale.ai/bot123:fake-token/sendMessage"
    assert payload["text"] == "اجرا <x> & $HOME"
    assert payload["reply_to_message_id"] == "8"
    assert "reply_parameters" not in payload
    assert "parse_mode" not in payload
    assert (
        json.loads(payload["reply_markup"])["inline_keyboard"][0][0]["callback_data"]
        == "approve:1"
    )
    await bot.edit_message_text("<b>Done</b>", 123, 10, parse_mode="HTML")
    assert request.calls[-1][1]["text"] == "Done"
    await bot.answer_callback_query("callback-123")
    assert request.calls[-1][0].endswith("/answerCallbackQuery")


async def test_files_and_album_keep_uploads(bale):
    bot, request, _ = bale
    file = await bot.get_file("f")
    assert file.file_path == "https://tapi.bale.ai/file/bot123:fake-token/docs/f"
    await bot.send_document(
        123, b"file bytes", caption="<b>File</b>", parse_mode="HTML"
    )
    assert request.calls[-1][1]["caption"] == "File"
    assert request.calls[-1][2]
    await bot.send_media_group(
        123,
        [InputMediaPhoto(b"image bytes", caption="<b>Photo</b>", parse_mode="HTML")],
    )
    payload = json.loads(request.calls[-1][1]["media"])[0]
    assert payload["caption"] == "Photo"
    assert "parse_mode" not in payload
    assert request.calls[-1][2]


async def test_polling_uses_bale_without_telegram_update_types(bale):
    bot, request, polling = bale
    await bot.get_updates(
        offset=12, timeout=5, allowed_updates=["message", "callback_query"]
    )
    assert polling.calls[-1][0].startswith("https://tapi.bale.ai/")
    assert polling.calls[-1][1] == {"offset": "12", "timeout": "5"}
    await bot.delete_webhook(drop_pending_updates=True)
    assert request.calls[-1][1] == {}


def test_plain_text_preserves_literals_and_links():
    assert plain_html("<code>&amp;lt; &lt;tag&gt;</code>") == "&lt; <tag>"
    assert (
        plain_html('<a href="https://example.com">docs</a>')
        == "docs (https://example.com)"
    )


@pytest.mark.parametrize("field", ["enable_stream_drafts", "enable_project_threads"])
def test_unsupported_features_rejected(field):
    with pytest.raises(ValidationError, match="Bale does not support"):
        create_test_config(bot_platform="bale", **{field: True})


def test_bale_factory_and_endpoint_overrides():
    settings = create_test_config(bot_platform="bale")
    bot = create_bale_bot(settings)
    assert bot.base_url.startswith("https://tapi.bale.ai/bot")
    assert bot.base_file_url.startswith("https://tapi.bale.ai/file/bot")
    settings.bot_base_url = "https://example.com/bot"
    assert create_bale_bot(settings).base_url.startswith("https://example.com/bot")


async def test_startup_uses_bale_and_skips_command_registration():
    settings = create_test_config(bot_platform="bale")
    bot = ClaudeCodeBot(settings, {"storage": MagicMock(), "security": MagicMock()})
    # Exercise the real Application builder and Bale bot, mocking only transport
    # initialization so no token or external service is needed.
    with (
        patch.object(BaleBot, "initialize", new_callable=AsyncMock),
        patch.object(BaleBot, "set_my_commands", new_callable=AsyncMock) as register,
        patch("src.bot.core.FeatureRegistry"),
        patch.object(bot, "_register_handlers"),
        patch.object(bot, "_add_middleware"),
    ):
        await bot.initialize()
        assert isinstance(bot.app.bot, BaleBot)
        register.assert_not_called()
