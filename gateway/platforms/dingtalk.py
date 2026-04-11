"""
DingTalk platform adapter using Stream Mode.

Uses dingtalk-stream SDK for real-time message reception without webhooks.
Responses are sent via DingTalk's session webhook (markdown format).

Requires:
    pip install dingtalk-stream httpx
    DINGTALK_CLIENT_ID and DINGTALK_CLIENT_SECRET env vars

Configuration in config.yaml:
    platforms:
      dingtalk:
        enabled: true
        extra:
          client_id: "your-app-key"      # or DINGTALK_CLIENT_ID env var
          client_secret: "your-secret"   # or DINGTALK_CLIENT_SECRET env var
"""

import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import dingtalk_stream
    from dingtalk_stream import ChatbotHandler, ChatbotMessage
    DINGTALK_STREAM_AVAILABLE = True
except ImportError:
    DINGTALK_STREAM_AVAILABLE = False
    dingtalk_stream = None  # type: ignore[assignment]

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False
    httpx = None  # type: ignore[assignment]

try:
    import mutagen
    MUTAGEN_AVAILABLE = True
except ImportError:
    MUTAGEN_AVAILABLE = False
    mutagen = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
    cache_image_from_bytes,
    cache_audio_from_bytes,
    cache_document_from_bytes,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 20000
DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 1000
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]
MAX_RECONNECT_ATTEMPTS = 10
RECONNECT_JITTER_FACTOR = 0.2       # ±20% jitter applied to backoff delays

# OAuth token management
_DINGTALK_API_BASE = "https://api.dingtalk.com/v1.0"
_TOKEN_REFRESH_BUFFER = 60      # seconds before expiry to proactively refresh
_TOKEN_RETRY_ATTEMPTS = 3
_TOKEN_RETRY_BASE_DELAY = 0.1   # seconds (100 ms), doubles each retry

# Module-level cache: clientId → (access_token, expires_at_unix_float)
# Shared across adapter instances within the same process (survives reconnects).
_TOKEN_CACHE: Dict[str, tuple] = {}
# Per-clientId asyncio.Lock to prevent concurrent duplicate token fetches.
_TOKEN_LOCKS: Dict[str, asyncio.Lock] = {}


def check_dingtalk_requirements() -> bool:
    """Check if DingTalk dependencies are available and configured."""
    if not DINGTALK_STREAM_AVAILABLE or not HTTPX_AVAILABLE:
        return False
    if not os.getenv("DINGTALK_CLIENT_ID") or not os.getenv("DINGTALK_CLIENT_SECRET"):
        return False
    return True


class DingTalkAdapter(BasePlatformAdapter):
    """DingTalk chatbot adapter using Stream Mode.

    The dingtalk-stream SDK maintains a long-lived WebSocket connection.
    Incoming messages arrive via a ChatbotHandler callback. Replies are
    sent via the incoming message's session_webhook URL using httpx.
    """

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: PlatformConfig):
        super().__init__(config, Platform.DINGTALK)

        extra = config.extra or {}
        self._client_id: str = extra.get("client_id") or os.getenv("DINGTALK_CLIENT_ID", "")
        self._client_secret: str = extra.get("client_secret") or os.getenv("DINGTALK_CLIENT_SECRET", "")

        self._stream_client: Any = None
        self._stream_task: Optional[asyncio.Task] = None
        self._http_client: Optional["httpx.AsyncClient"] = None

        self._last_message_at: float = 0.0
        self._consecutive_failures: int = 0

        # Message deduplication: msg_id -> timestamp
        self._seen_messages: Dict[str, float] = {}
        # Map chat_id -> session_webhook for reply routing
        self._session_webhooks: Dict[str, str] = {}
        # Proactive routing: chat type and user_id learned from inbound messages
        self._chat_types: Dict[str, str] = {}      # chat_id → "group" | "dm"
        self._dm_user_ids: Dict[str, str] = {}     # DM chat_id → sender_id

        # Emoji reaction acknowledgment (issue #6)
        # Disable with config.extra.ack_reaction = "none"
        ack_cfg = (extra.get("ack_reaction") or "emoji").strip().lower()
        self._ack_reaction_enabled: bool = ack_cfg != "none"
        self._ACK_PENDING = "⏳"    # added on message receipt
        self._ACK_SUCCESS = "✅"    # swapped in after successful send
        self._ACK_ERROR   = "❌"    # swapped in after failed send
        # Track in-flight reactions: msg_id → (chat_id, pending_emoji)
        self._pending_reactions: Dict[str, tuple] = {}

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self) -> bool:
        """Connect to DingTalk via Stream Mode."""
        if not DINGTALK_STREAM_AVAILABLE:
            logger.warning("[%s] dingtalk-stream not installed. Run: pip install dingtalk-stream", self.name)
            return False
        if not HTTPX_AVAILABLE:
            logger.warning("[%s] httpx not installed. Run: pip install httpx", self.name)
            return False
        if not self._client_id or not self._client_secret:
            logger.warning("[%s] DINGTALK_CLIENT_ID and DINGTALK_CLIENT_SECRET required", self.name)
            return False

        try:
            self._http_client = httpx.AsyncClient(timeout=30.0)
            self._last_message_at = time.monotonic()
            self._consecutive_failures = 0

            self._stream_task = asyncio.create_task(self._run_stream())
            self._mark_connected()
            logger.info("[%s] Connected via Stream Mode", self.name)

            # Warm-up: validate credentials early so misconfiguration surfaces at start-up.
            try:
                await self._get_access_token()
                logger.info("[%s] Access token validated successfully", self.name)
            except Exception as e:
                logger.warning("[%s] Could not fetch initial access token: %s", self.name, e)
                # Non-fatal — token will be retried on first proactive API call.

            return True
        except Exception as e:
            logger.error("[%s] Failed to connect: %s", self.name, e)
            return False

    async def _run_stream(self) -> None:
        """Run the stream client with auto-reconnection, jitter, and failure cap.

        Rebuilds a fresh DingTalkStreamClient on each attempt (warm reconnect).
        Applies ±20% jitter to backoff delays to avoid thundering herd.
        Calls _set_fatal_error() after MAX_RECONNECT_ATTEMPTS consecutive failures.

        NOTE: DingTalkStreamClient.start() already has an internal while-True reconnect
        loop and spawns a keepalive() task that sends WebSocket pings every 60 seconds.
        We do NOT need an external health check based on idle message time — that approach
        caused spurious reconnects during quiet periods (issue #17). If start() exits
        cleanly (rare), or raises an exception that escapes the SDK's own try/catch,
        this outer loop rebuilds a fresh client and retries.
        """
        backoff_idx = 0
        while self._running:
            try:
                logger.info("[%s] Starting stream client (consecutive failures: %d)...",
                            self.name, self._consecutive_failures)
                # Rebuild fresh client on each attempt — clears any stale internal state.
                credential = dingtalk_stream.Credential(self._client_id, self._client_secret)
                self._stream_client = dingtalk_stream.DingTalkStreamClient(credential)
                handler = _IncomingHandler(self, None)
                self._stream_client.register_callback_handler(
                    dingtalk_stream.ChatbotMessage.TOPIC, handler
                )

                await self._stream_client.start()
                # Clean exit from SDK — reset failure counter and backoff.
                self._consecutive_failures = 0
                backoff_idx = 0

            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                self._consecutive_failures += 1
                logger.warning(
                    "[%s] Stream error (failure %d/%d): %s",
                    self.name, self._consecutive_failures, MAX_RECONNECT_ATTEMPTS, e,
                )
                if self._consecutive_failures >= MAX_RECONNECT_ATTEMPTS:
                    logger.error("[%s] Max reconnect attempts reached, giving up", self.name)
                    self._set_fatal_error(
                        "dingtalk_stream_error",
                        f"Stream failed {MAX_RECONNECT_ATTEMPTS} consecutive times: {e}",
                        retryable=True,
                    )
                    return

            if not self._running:
                return

            raw_delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
            jitter = raw_delay * RECONNECT_JITTER_FACTOR * (2 * random.random() - 1)
            delay = max(0.1, raw_delay + jitter)
            logger.info("[%s] Reconnecting in %.1fs...", self.name, delay)
            await asyncio.sleep(delay)
            backoff_idx += 1

    async def disconnect(self) -> None:
        """Disconnect from DingTalk."""
        self._running = False
        self._mark_disconnected()

        if self._stream_task:
            self._stream_task.cancel()
            try:
                await self._stream_task
            except asyncio.CancelledError:
                pass
            self._stream_task = None

        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None

        self._stream_client = None
        self._consecutive_failures = 0
        self._session_webhooks.clear()
        self._seen_messages.clear()
        self._chat_types.clear()
        self._dm_user_ids.clear()
        self._pending_reactions.clear()
        logger.info("[%s] Disconnected", self.name)

    # -- Inbound message processing -----------------------------------------

    async def _on_message(self, message: "ChatbotMessage") -> None:
        """Process an incoming DingTalk chatbot message."""
        self._last_message_at = time.monotonic()
        msg_id = getattr(message, "message_id", None) or uuid.uuid4().hex
        if self._is_duplicate(msg_id):
            logger.debug("[%s] Duplicate message %s, skipping", self.name, msg_id)
            return

        # Parse message content (text + media) based on msgtype
        text, message_type, media_urls, media_types = await self._parse_inbound_message(message)
        if not text and not media_urls:
            logger.debug("[%s] Empty message, skipping", self.name)
            return

        # Quoted/replied message context
        reply_to_text, reply_to_message_id = self._extract_quoted_context(message)
        if reply_to_text:
            # Extract the original sender from repliedMsg (not the current sender)
            raw_text = getattr(message, "text", None)
            # Normalise to dict (SDK may return TextContent object)
            if hasattr(raw_text, "extensions"):
                raw_dict = {"content": getattr(raw_text, "content", "") or ""}
                raw_dict.update(raw_text.extensions or {})
            elif isinstance(raw_text, dict):
                raw_dict = raw_text
            else:
                raw_dict = {}
            replied = raw_dict.get("repliedMsg") or {}
            quoted_sender = replied.get("msgSenderNick", "") if isinstance(replied, dict) else ""
            prefix = f'[Replying to {quoted_sender}: "{reply_to_text}"]\n' if quoted_sender else f'[Replying to: "{reply_to_text}"]\n'
            text = prefix + text

        # Chat context
        conversation_id = getattr(message, "conversation_id", "") or ""
        conversation_type = getattr(message, "conversation_type", "1")
        is_group = str(conversation_type) == "2"
        sender_id = getattr(message, "sender_id", "") or ""
        sender_nick = getattr(message, "sender_nick", "") or sender_id
        sender_staff_id = getattr(message, "sender_staff_id", "") or ""

        chat_id = conversation_id or sender_id
        chat_type = "group" if is_group else "dm"

        # Store session webhook for reply routing
        session_webhook = getattr(message, "session_webhook", None) or ""
        if session_webhook and chat_id:
            self._session_webhooks[chat_id] = session_webhook

        # Track chat type and DM user_id for proactive sends
        self._chat_types[chat_id] = chat_type
        if chat_type == "dm" and sender_id:
            self._dm_user_ids[chat_id] = sender_id

        source = self.build_source(
            chat_id=chat_id,
            chat_name=getattr(message, "conversation_title", None),
            chat_type=chat_type,
            user_id=sender_id,
            user_name=sender_nick,
            user_id_alt=sender_staff_id if sender_staff_id else None,
        )

        # Parse timestamp
        create_at = getattr(message, "create_at", None)
        try:
            timestamp = datetime.fromtimestamp(int(create_at) / 1000, tz=timezone.utc) if create_at else datetime.now(tz=timezone.utc)
        except (ValueError, OSError, TypeError):
            timestamp = datetime.now(tz=timezone.utc)

        event = MessageEvent(
            text=text,
            message_type=message_type,
            source=source,
            message_id=msg_id,
            raw_message=message,
            timestamp=timestamp,
            reply_to_message_id=reply_to_message_id,
            reply_to_text=reply_to_text,
            media_urls=media_urls,
            media_types=media_types,
        )

        logger.debug("[%s] Message from %s in %s (%s): %s",
                     self.name, sender_nick, chat_id[:20] if chat_id else "?",
                     message_type.value, text[:50])

        # Acknowledge receipt with a reaction before the agent starts (issue #6)
        if self._ack_reaction_enabled and chat_id:
            await self._add_reaction(msg_id, chat_id, self._ACK_PENDING)
            self._pending_reactions[msg_id] = (chat_id, self._ACK_PENDING)

        await self.handle_message(event)

    async def _parse_inbound_message(
        self, message: "ChatbotMessage"
    ) -> "tuple[str, MessageType, list, list]":
        """Parse an inbound DingTalk message into (text, message_type, media_urls, media_types).

        Handles msgtype: text, picture, audio, video, file, richText.
        Downloads media via _download_media() and caches locally.
        On download failure falls back to text-only (never drops the message).
        """
        msgtype = getattr(message, "message_type", None) or getattr(message, "msgtype", None) or "text"
        # The SDK only parses text/picture/richText into typed fields.
        # For audio/video/file the raw `content` dict lands in extensions["content"].
        extensions = getattr(message, "extensions", {}) or {}
        raw_content = extensions.get("content") or {}
        if isinstance(raw_content, str):
            try:
                import json as _json
                raw_content = _json.loads(raw_content)
            except Exception:
                raw_content = {}
        if not isinstance(raw_content, dict):
            raw_content = {}

        media_urls: list = []
        media_types: list = []

        if msgtype == "picture":
            image_content = getattr(message, "image_content", None)
            download_code = (
                getattr(image_content, "download_code", None)
                or raw_content.get("downloadCode", "")
            )
            if download_code:
                path, mime = await self._download_media(download_code, "image/jpeg", ".jpg")
                if path:
                    media_urls.append(path)
                    media_types.append(mime)
            text = raw_content.get("caption", "") or ""
            return text, MessageType.PHOTO, media_urls, media_types

        if msgtype == "audio":
            download_code = raw_content.get("downloadCode", "")
            if download_code:
                path, mime = await self._download_media(download_code, "audio/amr", ".amr")
                if path:
                    media_urls.append(path)
                    media_types.append(mime)
            return "", MessageType.VOICE, media_urls, media_types

        if msgtype == "video":
            download_code = raw_content.get("downloadCode", "")
            if download_code:
                path, mime = await self._download_media(download_code, "video/mp4", ".mp4")
                if path:
                    media_urls.append(path)
                    media_types.append(mime)
            return "", MessageType.VIDEO, media_urls, media_types

        if msgtype == "file":
            download_code = raw_content.get("downloadCode", "")
            file_name = raw_content.get("fileName", "document") or "document"
            if download_code:
                path, mime = await self._download_media(
                    download_code, "application/octet-stream", None, file_name
                )
                if path:
                    media_urls.append(path)
                    media_types.append(mime)
            return file_name, MessageType.DOCUMENT, media_urls, media_types

        if msgtype == "richText":
            # SDK stores richText in rich_text_content.rich_text_list
            rich_text_content = getattr(message, "rich_text_content", None)
            rich_parts = []
            if rich_text_content and hasattr(rich_text_content, "rich_text_list"):
                rich_parts = rich_text_content.rich_text_list or []
            elif isinstance(raw_content.get("richText"), list):
                rich_parts = raw_content["richText"]
            text_parts = []
            for part in rich_parts:
                if not isinstance(part, dict):
                    continue
                part_type = part.get("type", "text")
                if part_type in ("text", None) and part.get("text"):
                    text_parts.append(part["text"])
                elif part_type == "picture":
                    download_code = part.get("downloadCode", "")
                    if download_code:
                        path, mime = await self._download_media(
                            download_code, "image/jpeg", ".jpg"
                        )
                        if path:
                            media_urls.append(path)
                            media_types.append(mime)
            text = "".join(text_parts).strip()
            msg_type = MessageType.PHOTO if media_urls else MessageType.TEXT
            return text, msg_type, media_urls, media_types

        # Default: plain text (msgtype == "text" or unknown)
        text = self._extract_text(message)
        return text, MessageType.TEXT, [], []

    async def _download_media(
        self,
        download_code: str,
        default_mime: str,
        default_ext: "Optional[str]",
        filename: str = "",
    ) -> "tuple[Optional[str], str]":
        """Download a DingTalk media file and cache it locally.

        POSTs downloadCode to the DingTalk API to obtain a temporary download URL,
        then GETs the file bytes and caches them via the base class helpers.

        Returns (local_path, mime_type) on success, (None, default_mime) on failure.
        """
        if not self._http_client:
            logger.warning("[%s] HTTP client not available for media download", self.name)
            return None, default_mime

        try:
            headers = await self._dingtalk_headers()
        except RuntimeError as e:
            logger.warning("[%s] Cannot download media — token error: %s", self.name, e)
            return None, default_mime

        try:
            resp = await self._http_client.post(
                f"{_DINGTALK_API_BASE}/robot/messageFiles/download",
                json={"downloadCode": download_code, "robotCode": self._client_id},
                headers=headers,
                timeout=15.0,
            )
            if resp.status_code >= 300:
                logger.warning(
                    "[%s] Media download exchange failed HTTP %d: %s",
                    self.name, resp.status_code, resp.text[:200],
                )
                return None, default_mime

            data = resp.json()
            download_url = data.get("downloadUrl") or (data.get("data") or {}).get("downloadUrl")
            if not download_url:
                logger.warning("[%s] No downloadUrl in media response: %s", self.name, str(data)[:200])
                return None, default_mime

        except Exception as e:
            logger.warning("[%s] Media download exchange error: %s", self.name, e)
            return None, default_mime

        try:
            file_resp = await self._http_client.get(download_url, timeout=30.0, follow_redirects=True)
            if file_resp.status_code >= 300:
                logger.warning(
                    "[%s] Media file fetch failed HTTP %d", self.name, file_resp.status_code
                )
                return None, default_mime

            file_bytes = file_resp.content
            content_type = file_resp.headers.get("content-type", default_mime).split(";")[0].strip()

            # Choose cache helper based on MIME type
            if content_type.startswith("image/"):
                ext = default_ext or ("." + content_type.split("/")[-1]) or ".jpg"
                path = cache_image_from_bytes(file_bytes, ext)
            elif content_type.startswith("audio/"):
                ext = default_ext or ("." + content_type.split("/")[-1]) or ".amr"
                path = cache_audio_from_bytes(file_bytes, ext)
            else:
                safe_name = filename or f"media{default_ext or ''}"
                path = cache_document_from_bytes(file_bytes, safe_name)

            logger.debug("[%s] Media cached at %s (%s)", self.name, path, content_type)
            return path, content_type

        except Exception as e:
            logger.warning("[%s] Media file download error: %s", self.name, e)
            return None, default_mime

    @staticmethod
    def _extract_text(message: "ChatbotMessage") -> str:
        """Extract plain text from a DingTalk chatbot message."""
        text = getattr(message, "text", None) or ""
        if hasattr(text, "content"):
            # SDK returns a TextContent object when msgtype == "text"
            content = (text.content or "").strip()
        elif isinstance(text, dict):
            content = text.get("content", "").strip()
        else:
            content = str(text).strip()

        # Fall back to rich_text_content (SDK object) or legacy rich_text list
        if not content:
            rich_text_content = getattr(message, "rich_text_content", None)
            if rich_text_content and hasattr(rich_text_content, "rich_text"):
                parts = [
                    item.get("text", "") for item in (rich_text_content.rich_text or [])
                    if isinstance(item, dict) and item.get("text")
                ]
                content = " ".join(parts).strip()
        if not content:
            rich_text = getattr(message, "rich_text", None)
            if rich_text and isinstance(rich_text, list):
                parts = [item["text"] for item in rich_text
                         if isinstance(item, dict) and item.get("text")]
                content = " ".join(parts).strip()
        return content

    @staticmethod
    def _extract_quoted_context(message: "ChatbotMessage") -> tuple:
        """Extract quoted/replied message context from a DingTalk inbound message.

        Returns (reply_to_text, reply_to_message_id). Both are None if the
        message is not a reply or if quoted fields are absent/malformed.

        The SDK parses msgtype=text into a TextContent object; extra fields
        such as isReplyMsg and repliedMsg land in TextContent.extensions.
        We also handle the legacy dict format for test compatibility.
        """
        raw_text = getattr(message, "text", None)

        # Normalise to a plain dict for unified processing
        if hasattr(raw_text, "extensions"):
            # SDK TextContent object: merge content + extensions into one dict
            raw_dict: dict = {"content": raw_text.content or ""}
            raw_dict.update(raw_text.extensions or {})
        elif isinstance(raw_text, dict):
            raw_dict = raw_text
        else:
            return None, None

        if not raw_dict.get("isReplyMsg"):
            return None, None

        replied = raw_dict.get("repliedMsg")
        if not replied or not isinstance(replied, dict):
            return None, None

        msg_content = replied.get("msgContent") or ""
        msg_id = replied.get("msgId") or None
        return (msg_content.strip() or None), msg_id

    # -- OAuth token management ---------------------------------------------

    async def _get_access_token(self) -> str:
        """Return a valid DingTalk access token, fetching/refreshing as needed.

        Uses a module-level cache keyed by clientId so tokens survive adapter
        reconnects within the same process. Proactively refreshes 60 s before
        expiry. Retries up to 3 times on transient failures (401, 429, 5xx)
        with exponential backoff starting at 100 ms.

        Raises RuntimeError if all retries fail or credentials are rejected.
        """
        client_id = self._client_id

        if client_id not in _TOKEN_LOCKS:
            _TOKEN_LOCKS[client_id] = asyncio.Lock()
        lock = _TOKEN_LOCKS[client_id]

        async with lock:
            cached = _TOKEN_CACHE.get(client_id)
            if cached:
                token, expires_at = cached
                if time.time() < expires_at - _TOKEN_REFRESH_BUFFER:
                    return token

            last_exc: Optional[Exception] = None
            for attempt in range(1, _TOKEN_RETRY_ATTEMPTS + 1):
                try:
                    resp = await self._http_client.post(
                        f"{_DINGTALK_API_BASE}/oauth2/accessToken",
                        json={"appKey": client_id, "appSecret": self._client_secret},
                        timeout=10.0,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        token = data["accessToken"]
                        expires_in = int(data.get("expireIn", 7200))
                        expires_at = time.time() + expires_in
                        _TOKEN_CACHE[client_id] = (token, expires_at)
                        logger.debug("[%s] Access token refreshed (expires in %ds)",
                                     self.name, expires_in)
                        return token

                    status = resp.status_code
                    if status not in (401, 429) and status < 500:
                        raise RuntimeError(
                            f"DingTalk token fetch failed HTTP {status}: {resp.text[:200]}"
                        )
                    last_exc = RuntimeError(f"HTTP {status}: {resp.text[:100]}")
                except (httpx.TimeoutException, httpx.NetworkError) as exc:
                    last_exc = exc

                if attempt < _TOKEN_RETRY_ATTEMPTS:
                    delay = _TOKEN_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                    logger.warning(
                        "[%s] Token fetch attempt %d/%d failed, retrying in %.2fs: %s",
                        self.name, attempt, _TOKEN_RETRY_ATTEMPTS, delay, last_exc,
                    )
                    await asyncio.sleep(delay)

            raise RuntimeError(
                f"[{self.name}] Failed to obtain DingTalk access token after "
                f"{_TOKEN_RETRY_ATTEMPTS} attempts: {last_exc}"
            )

    async def _dingtalk_headers(self) -> Dict[str, str]:
        """Return HTTP headers required for DingTalk REST API calls."""
        token = await self._get_access_token()
        return {
            "x-acs-dingtalk-access-token": token,
            "Content-Type": "application/json",
        }

    # -- Deduplication ------------------------------------------------------

    def _is_duplicate(self, msg_id: str) -> bool:
        """Check and record a message ID. Returns True if already seen."""
        now = time.time()
        if len(self._seen_messages) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            self._seen_messages = {k: v for k, v in self._seen_messages.items() if v > cutoff}

        if msg_id in self._seen_messages:
            return True
        self._seen_messages[msg_id] = now
        return False

    # -- Outbound messaging -------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to a DingTalk chat.

        Prefers session_webhook (immediate reply path). Falls back to the
        proactive robot API when no webhook is cached for the chat_id, enabling
        cron delivery and agent-initiated sends.

        After sending, updates the emoji reaction on the triggering message
        to ✅ (success) or ❌ (failure). reply_to carries the inbound msg_id
        so the correct reaction can be found in _pending_reactions.
        """
        metadata = metadata or {}

        session_webhook = metadata.get("session_webhook") or self._session_webhooks.get(chat_id)

        if session_webhook:
            if not self._http_client:
                return SendResult(success=False, error="HTTP client not initialized")
            payload = {
                "msgtype": "markdown",
                "markdown": {"title": "Hermes", "text": content[:self.MAX_MESSAGE_LENGTH]},
            }
            try:
                resp = await self._http_client.post(session_webhook, json=payload, timeout=15.0)
                if resp.status_code < 300:
                    result = SendResult(success=True, message_id=uuid.uuid4().hex[:12])
                else:
                    body = resp.text
                    logger.warning("[%s] Send failed HTTP %d: %s", self.name, resp.status_code, body[:200])
                    result = SendResult(success=False, error=f"HTTP {resp.status_code}: {body[:200]}")
            except httpx.TimeoutException:
                result = SendResult(success=False, error="Timeout sending message to DingTalk")
            except Exception as e:
                logger.error("[%s] Send error: %s", self.name, e)
                result = SendResult(success=False, error=str(e))
        else:
            # No session_webhook — use proactive robot API
            logger.debug("[%s] No session_webhook for %s, using proactive API", self.name, chat_id[:20])
            result = await self._send_proactive(chat_id, content)

        await self._finalize_reaction(reply_to, result)
        return result

    async def _send_proactive(self, chat_id: str, content: str) -> SendResult:
        """Send via DingTalk proactive robot API (no session_webhook needed).

        Determines group vs DM from chat type cached during inbound processing,
        falling back to the 'cid' prefix heuristic used by DingTalk group IDs.
        Chunks messages at 3800 chars. Returns the last chunk's SendResult.
        """
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")

        chat_type = self._chat_types.get(chat_id)
        if chat_type is None:
            chat_type = "group" if chat_id.startswith("cid") else "dm"

        is_group = chat_type == "group"
        user_id: Optional[str] = None
        if not is_group:
            user_id = self._dm_user_ids.get(chat_id) or chat_id

        try:
            headers = await self._dingtalk_headers()
        except RuntimeError as e:
            return SendResult(success=False, error=f"Token error: {e}")

        chunks = self.truncate_message(content, 3800)
        last_result: SendResult = SendResult(success=False, error="No content")

        for chunk in chunks:
            msg_param = json.dumps({"title": "Hermes", "text": chunk})
            payload: Dict[str, Any] = {
                "robotCode": self._client_id,
                "msgKey": "sampleMarkdown",
                "msgParam": msg_param,
            }
            if is_group:
                payload["openConversationId"] = chat_id
                endpoint = f"{_DINGTALK_API_BASE}/robot/groupMessages/send"
            else:
                payload["userIds"] = [user_id]
                endpoint = f"{_DINGTALK_API_BASE}/robot/oToMessages/batchSend"

            try:
                resp = await self._http_client.post(
                    endpoint, json=payload, headers=headers, timeout=15.0
                )
                if resp.status_code < 300:
                    last_result = SendResult(success=True, message_id=uuid.uuid4().hex[:12])
                else:
                    body = resp.text
                    logger.warning(
                        "[%s] Proactive send failed HTTP %d: %s",
                        self.name, resp.status_code, body[:200],
                    )
                    last_result = SendResult(
                        success=False, error=f"HTTP {resp.status_code}: {body[:200]}"
                    )
            except httpx.TimeoutException:
                last_result = SendResult(
                    success=False, error="Timeout on proactive send", retryable=True
                )
            except Exception as e:
                logger.error("[%s] Proactive send error: %s", self.name, e)
                last_result = SendResult(success=False, error=str(e))

        return last_result

    # -- Outbound media ---------------------------------------------------------

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image to a DingTalk chat.

        Downloads the image from image_url, uploads to DingTalk, then sends
        via sampleImageMsg template. Falls back to a text link on any failure.
        """
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")

        # Download image bytes
        try:
            resp = await self._http_client.get(image_url, timeout=30.0, follow_redirects=True)
            if resp.status_code >= 300:
                raise RuntimeError(f"HTTP {resp.status_code}")
            image_bytes = resp.content
            content_type = resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
            ext = "." + content_type.split("/")[-1] if "/" in content_type else ".jpg"
            if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
                ext = ".jpg"
            filename = f"image{ext}"
        except Exception as e:
            logger.warning("[%s] send_image: failed to download %s: %s", self.name, image_url[:80], e)
            return await self.send(chat_id, f"[Image]({image_url})" + (f"\n{caption}" if caption else ""))

        # Upload to DingTalk
        media_id = await self._upload_media(image_bytes, "image", filename)
        if not media_id:
            return await self.send(chat_id, f"[Image]({image_url})" + (f"\n{caption}" if caption else ""))

        # Send via sampleImageMsg using the uploaded mediaId.
        result = await self._send_media_proactive(chat_id, "sampleImageMsg", {"photoURL": media_id})
        # If there is a caption, send it as a follow-up markdown message.
        if caption and result.success:
            await self.send(chat_id, caption, reply_to=reply_to, metadata=metadata)
        return result

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send an audio/voice message to a DingTalk chat.

        Uploads the file and sends via sampleAudio template.
        Duration is parsed via mutagen when available; falls back to 0.
        See issue #9 for tracking accurate duration without mutagen.
        """
        try:
            with open(audio_path, "rb") as f:
                audio_bytes = f.read()
        except OSError as e:
            return SendResult(success=False, error=f"Cannot read audio file: {e}")

        filename = os.path.basename(audio_path) or "audio.amr"
        media_id = await self._upload_media(audio_bytes, "voice", filename)
        if not media_id:
            return await self.send(chat_id, f"🔊 Audio: {audio_path}" + (f"\n{caption}" if caption else ""))

        duration_ms = self._get_audio_duration_ms(audio_path)
        return await self._send_media_proactive(
            chat_id, "sampleAudio",
            {"mediaId": media_id, "duration": str(duration_ms)},
        )

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a document/file to a DingTalk chat via sampleFile template."""
        try:
            with open(file_path, "rb") as f:
                file_bytes = f.read()
        except OSError as e:
            return SendResult(success=False, error=f"Cannot read file: {e}")

        filename = file_name or os.path.basename(file_path) or "document"
        media_id = await self._upload_media(file_bytes, "file", filename)
        if not media_id:
            return await self.send(chat_id, f"📎 File: {file_path}" + (f"\n{caption}" if caption else ""))

        file_type = os.path.splitext(filename)[1].lstrip(".") or "bin"
        return await self._send_media_proactive(
            chat_id, "sampleFile",
            {"mediaId": media_id, "fileName": filename, "fileType": file_type},
        )

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        **kwargs,
    ) -> SendResult:
        """Send a video to a DingTalk chat via sampleVideo template."""
        try:
            with open(video_path, "rb") as f:
                video_bytes = f.read()
        except OSError as e:
            return SendResult(success=False, error=f"Cannot read video file: {e}")

        filename = os.path.basename(video_path) or "video.mp4"
        media_id = await self._upload_media(video_bytes, "video", filename)
        if not media_id:
            return await self.send(chat_id, f"🎬 Video: {video_path}" + (f"\n{caption}" if caption else ""))

        duration_ms = self._get_audio_duration_ms(video_path)
        video_size = len(video_bytes)
        return await self._send_media_proactive(
            chat_id, "sampleVideo",
            {
                "mediaId": media_id,
                "videoThumbnailURL": "",
                "duration": str(duration_ms),
                "videoSize": str(video_size),
            },
        )

    async def _upload_media(
        self,
        data: bytes,
        media_type: str,
        filename: str,
    ) -> "Optional[str]":
        """Upload media bytes to DingTalk and return the mediaId.

        media_type must be one of: image, voice, video, file.
        Returns None on failure (caller should fall back to text).
        """
        if not self._http_client:
            return None
        try:
            headers = await self._dingtalk_headers()
        except RuntimeError as e:
            logger.warning("[%s] _upload_media: token error: %s", self.name, e)
            return None

        # Remove Content-Type from headers — we set it manually with the boundary
        upload_headers = {k: v for k, v in headers.items() if k.lower() != "content-type"}

        try:
            body, boundary = self._build_multipart(data, media_type, filename)
            upload_headers["Content-Type"] = f"multipart/form-data; boundary={boundary}"
            resp = await self._http_client.post(
                f"{_DINGTALK_API_BASE}/robot/messageFiles/upload",
                headers=upload_headers,
                content=body,
                timeout=30.0,
            )
            if resp.status_code >= 300:
                logger.warning("[%s] Media upload failed HTTP %d: %s",
                               self.name, resp.status_code, resp.text[:200])
                return None
            result = resp.json()
            media_id = result.get("mediaId")
            if not media_id:
                logger.warning("[%s] No mediaId in upload response: %s", self.name, str(result)[:200])
            return media_id
        except Exception as e:
            logger.warning("[%s] Media upload error: %s", self.name, e)
            return None

    def _build_multipart(self, data: bytes, media_type: str, filename: str) -> bytes:
        """Build a multipart/form-data body for the DingTalk media upload API.

        Fields: robotCode, mediaType, uploadedFileName, media (binary).
        Returns raw bytes with a unique boundary; the caller must set the
        Content-Type header to multipart/form-data; boundary=<boundary>.
        We return (body_bytes, boundary) so the caller can set the header.
        """
        boundary = uuid.uuid4().hex

        def field(name: str, value: str) -> bytes:
            return (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n'
                f"\r\n"
                f"{value}\r\n"
            ).encode()

        body = b""
        body += field("robotCode", self._client_id)
        body += field("mediaType", media_type)
        body += field("uploadedFileName", filename)
        body += (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="media"; filename="{filename}"\r\n'
            f"Content-Type: application/octet-stream\r\n"
            f"\r\n"
        ).encode()
        body += data
        body += f"\r\n--{boundary}--\r\n".encode()
        return body, boundary

    async def _send_media_proactive(
        self,
        chat_id: str,
        msg_key: str,
        msg_param: Dict[str, Any],
    ) -> SendResult:
        """Send a media message via DingTalk proactive robot API.

        Reuses the group/DM routing logic from _send_proactive().
        """
        if not self._http_client:
            return SendResult(success=False, error="HTTP client not initialized")

        chat_type = self._chat_types.get(chat_id)
        if chat_type is None:
            chat_type = "group" if chat_id.startswith("cid") else "dm"
        is_group = chat_type == "group"
        user_id = None if is_group else (self._dm_user_ids.get(chat_id) or chat_id)

        try:
            headers = await self._dingtalk_headers()
        except RuntimeError as e:
            return SendResult(success=False, error=f"Token error: {e}")

        payload: Dict[str, Any] = {
            "robotCode": self._client_id,
            "msgKey": msg_key,
            "msgParam": json.dumps(msg_param),
        }
        if is_group:
            payload["openConversationId"] = chat_id
            endpoint = f"{_DINGTALK_API_BASE}/robot/groupMessages/send"
        else:
            payload["userIds"] = [user_id]
            endpoint = f"{_DINGTALK_API_BASE}/robot/oToMessages/batchSend"

        try:
            resp = await self._http_client.post(endpoint, json=payload, headers=headers, timeout=15.0)
            if resp.status_code < 300:
                return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
            logger.warning("[%s] Media send failed HTTP %d: %s",
                           self.name, resp.status_code, resp.text[:200])
            return SendResult(success=False, error=f"HTTP {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            logger.error("[%s] Media send error: %s", self.name, e)
            return SendResult(success=False, error=str(e))

    @staticmethod
    def _get_audio_duration_ms(path: str) -> int:
        """Return audio/video duration in milliseconds using mutagen (soft dependency).

        DingTalk sampleAudio/sampleVideo templates expect duration in milliseconds.
        Returns 0 if mutagen is not installed or the file cannot be parsed.
        See issue #9 for tracking accurate duration support.
        """
        if not MUTAGEN_AVAILABLE:
            return 0
        try:
            audio = mutagen.File(path)  # type: ignore[union-attr]
            if audio and hasattr(audio, "info") and hasattr(audio.info, "length"):
                return max(1, int(audio.info.length * 1000))
        except Exception:
            pass
        return 0

    # -- Emoji reaction acknowledgment (issue #6) ---------------------------

    async def _add_reaction(self, msg_id: str, chat_id: str, emoji: str) -> None:
        """Add an emoji reaction to an inbound DingTalk message.

        Calls POST /v1.0/robot/emotion/reply. Failures are logged as warnings
        and never propagate — reactions are purely cosmetic feedback.
        """
        if not self._http_client:
            return
        try:
            headers = await self._dingtalk_headers()
            resp = await self._http_client.post(
                f"{_DINGTALK_API_BASE}/robot/emotion/reply",
                json={"robotCode": self._client_id, "openMsgId": msg_id, "openConversationId": chat_id, "emojiType": emoji},
                headers=headers,
                timeout=5.0,
            )
            if resp.status_code >= 300:
                logger.warning("[%s] _add_reaction HTTP %d: %s", self.name, resp.status_code, resp.text[:200])
            else:
                logger.debug("[%s] _add_reaction OK HTTP %d", self.name, resp.status_code)
        except Exception as e:
            logger.warning("[%s] _add_reaction failed (non-fatal): %s", self.name, e)

    async def _remove_reaction(self, msg_id: str, chat_id: str, emoji: str) -> None:
        """Remove an emoji reaction from an inbound DingTalk message.

        Calls DELETE /v1.0/robot/emotion/recall. Failures are non-fatal.
        """
        if not self._http_client:
            return
        try:
            headers = await self._dingtalk_headers()
            await self._http_client.request(
                "DELETE",
                f"{_DINGTALK_API_BASE}/robot/emotion/recall",
                json={"robotCode": self._client_id, "openMsgId": msg_id, "openConversationId": chat_id, "emojiType": emoji},
                headers=headers,
                timeout=5.0,
            )
        except Exception as e:
            logger.warning("[%s] _remove_reaction failed (non-fatal): %s", self.name, e)

    async def _finalize_reaction(self, msg_id: Optional[str], result: SendResult) -> None:
        """Swap the pending ⏳ reaction to ✅ or ❌ after a send completes.

        Looks up the pending reaction by msg_id. If found, removes the pending
        emoji and adds the outcome emoji. No-op when reactions are disabled or
        msg_id is not tracked.
        """
        if not self._ack_reaction_enabled or not msg_id:
            return
        pending = self._pending_reactions.pop(msg_id, None)
        if not pending:
            return
        chat_id, pending_emoji = pending
        await self._remove_reaction(msg_id, chat_id, pending_emoji)
        outcome_emoji = self._ACK_SUCCESS if result.success else self._ACK_ERROR
        await self._add_reaction(msg_id, chat_id, outcome_emoji)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """DingTalk does not support typing indicators."""
        pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return basic info about a DingTalk conversation."""
        return {"name": chat_id, "type": "group" if "group" in chat_id.lower() else "dm"}


# ---------------------------------------------------------------------------
# Internal stream handler
# ---------------------------------------------------------------------------

class _IncomingHandler(ChatbotHandler if DINGTALK_STREAM_AVAILABLE else object):
    """dingtalk-stream ChatbotHandler that forwards messages to the adapter.

    The SDK calls process() as an async coroutine, passing a CallbackMessage.
    The actual chatbot payload lives in callback_message.data (a JSON string
    or dict) which we parse into a ChatbotMessage and hand to the adapter.
    """

    def __init__(self, adapter: DingTalkAdapter, loop: asyncio.AbstractEventLoop):
        if DINGTALK_STREAM_AVAILABLE:
            super().__init__()
        self._adapter = adapter
        self._loop = loop

    async def process(self, callback_message: "Any") -> "tuple":
        """Called by dingtalk-stream when a chatbot message arrives."""
        try:
            data = getattr(callback_message, "data", {})
            if isinstance(data, str):
                import json as _json
                data = _json.loads(data)
            message = dingtalk_stream.ChatbotMessage.from_dict(data)
            await self._adapter._on_message(message)
        except Exception:
            logger.exception("[DingTalk] Error processing incoming message")

        return dingtalk_stream.AckMessage.STATUS_OK, "OK"
