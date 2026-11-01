"""Bale compatibility at the shared Bot API boundary.

All handlers, approvals, media and notifications use this bot. Keep the
translation here so a new handler cannot accidentally send Telegram HTML.
"""

from copy import copy
from html.parser import HTMLParser
from typing import Any, Dict, List, Tuple

from telegram import InputMedia
from telegram.ext import AIORateLimiter, Defaults, ExtBot
from telegram.request import HTTPXRequest

from ..config.settings import Settings


class _HTMLText(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: List[str] = []
        self.links: List[Tuple[str, int]] = []

    def handle_starttag(self, tag: str, attrs: Any) -> None:
        if tag == "br":
            self.parts.append("\n")
        elif tag == "a":
            self.links.append((dict(attrs).get("href", ""), len(self.parts)))

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self.links:
            href, start = self.links.pop()
            if href.startswith(("https://", "http://")):
                if "".join(self.parts[start:]) != href:
                    self.parts.append(f" ({href})")

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def plain_html(text: str) -> str:
    """Decode HTML once, preserving literal code and HTTP link targets."""
    parser = _HTMLText()
    parser.feed(text)
    parser.close()
    return "".join(parser.parts)


def normalize_payload(endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Translate PTB 22 payloads to Bale's documented API format."""
    result = dict(data)
    mode = result.pop("parse_mode", None)
    if mode:
        if str(mode).lower() != "html":
            raise ValueError(f"Unsupported Bale output parse mode: {mode}")
        for field in ("text", "caption"):
            if isinstance(result.get(field), str):
                result[field] = plain_html(result[field])

    # PTB converts reply_to_message_id into ReplyParameters before this point.
    reply = result.pop("reply_parameters", None)
    if reply is not None:
        reply_id = (
            reply.get("message_id")
            if isinstance(reply, dict)
            else getattr(reply, "message_id", None)
        )
        if reply_id is not None:
            result["reply_to_message_id"] = reply_id

    for field in (
        "entities",
        "caption_entities",
        "link_preview_options",
        "message_thread_id",
        "business_connection_id",
        "direct_messages_topic_id",
    ):
        result.pop(field, None)

    # Media groups serialize each InputMedia independently of the top-level mode.
    if "media" in result and isinstance(result["media"], (list, tuple)):
        media = []
        for item in result["media"]:
            if isinstance(item, InputMedia):
                # Preserve the InputMedia type: PTB needs it to extract uploads.
                # Copy rather than changing an object a handler may reuse.
                translated = normalize_payload("media", item.to_dict())
                cloned = copy(item)
                with cloned._unfrozen():
                    cloned.caption = translated.get("caption")
                    cloned.parse_mode = None
                    cloned.caption_entities = ()
                media.append(cloned)
            else:
                media.append(normalize_payload("media", item))
        result["media"] = media
    if endpoint == "getUpdates":
        result = {
            k: v for k, v in result.items() if k in {"offset", "limit", "timeout"}
        }
    elif endpoint == "deleteWebhook":
        result = {}
    elif endpoint == "setWebhook":
        result = {"url": result["url"]}
    return result


class BaleBot(ExtBot):
    """Apply compatibility after PTB inserts defaults, before serialization.

    _do_post is a PTB internal extension point; wire-level tests guard it on
    dependency upgrades within the project's supported PTB 22 range.
    """

    async def _do_post(self, endpoint: str, data: Dict[str, Any], **kwargs: Any) -> Any:
        return await super()._do_post(
            endpoint, normalize_payload(endpoint, data), **kwargs
        )


def create_bale_bot(settings: Settings) -> BaleBot:
    """Use separate HTTP pools for polling and regular calls, without a proxy.

    The Claude SDK keeps its own environment/proxy settings. Bale requests go
    directly to Bale rather than inheriting a Telegram/Anthropic proxy.
    """

    def request() -> HTTPXRequest:
        return HTTPXRequest(
            connection_pool_size=8,
            connect_timeout=30,
            read_timeout=30,
            write_timeout=30,
            pool_timeout=30,
            httpx_kwargs={"trust_env": False},
        )

    return BaleBot(
        token=settings.telegram_token_str,
        base_url=settings.bot_base_url or "https://tapi.bale.ai/bot",
        base_file_url=settings.bot_base_file_url or "https://tapi.bale.ai/file/bot",
        request=request(),
        get_updates_request=request(),
        defaults=Defaults(do_quote=settings.reply_quote),
        rate_limiter=AIORateLimiter(max_retries=1),
    )
