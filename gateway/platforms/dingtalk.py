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

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 20000
DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 1000
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]

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

        # Message deduplication: msg_id -> timestamp
        self._seen_messages: Dict[str, float] = {}
        # Map chat_id -> session_webhook for reply routing
        self._session_webhooks: Dict[str, str] = {}
        # Proactive routing: chat type and user_id learned from inbound messages
        self._chat_types: Dict[str, str] = {}      # chat_id → "group" | "dm"
        self._dm_user_ids: Dict[str, str] = {}     # DM chat_id → sender_id

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

            credential = dingtalk_stream.Credential(self._client_id, self._client_secret)
            self._stream_client = dingtalk_stream.DingTalkStreamClient(credential)

            # Capture the current event loop for cross-thread dispatch
            loop = asyncio.get_running_loop()
            handler = _IncomingHandler(self, loop)
            self._stream_client.register_callback_handler(
                dingtalk_stream.ChatbotMessage.TOPIC, handler
            )

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
        """Run the blocking stream client with auto-reconnection."""
        backoff_idx = 0
        while self._running:
            try:
                logger.debug("[%s] Starting stream client...", self.name)
                await asyncio.to_thread(self._stream_client.start)
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("[%s] Stream client error: %s", self.name, e)

            if not self._running:
                return

            delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
            logger.info("[%s] Reconnecting in %ds...", self.name, delay)
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
        self._session_webhooks.clear()
        self._seen_messages.clear()
        self._chat_types.clear()
        self._dm_user_ids.clear()
        logger.info("[%s] Disconnected", self.name)

    # -- Inbound message processing -----------------------------------------

    async def _on_message(self, message: "ChatbotMessage") -> None:
        """Process an incoming DingTalk chatbot message."""
        msg_id = getattr(message, "message_id", None) or uuid.uuid4().hex
        if self._is_duplicate(msg_id):
            logger.debug("[%s] Duplicate message %s, skipping", self.name, msg_id)
            return

        text = self._extract_text(message)
        if not text:
            logger.debug("[%s] Empty message, skipping", self.name)
            return

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
            message_type=MessageType.TEXT,
            source=source,
            message_id=msg_id,
            raw_message=message,
            timestamp=timestamp,
        )

        logger.debug("[%s] Message from %s in %s: %s",
                      self.name, sender_nick, chat_id[:20] if chat_id else "?", text[:50])
        await self.handle_message(event)

    @staticmethod
    def _extract_text(message: "ChatbotMessage") -> str:
        """Extract plain text from a DingTalk chatbot message."""
        text = getattr(message, "text", None) or ""
        if isinstance(text, dict):
            content = text.get("content", "").strip()
        else:
            content = str(text).strip()

        # Fall back to rich text if present
        if not content:
            rich_text = getattr(message, "rich_text", None)
            if rich_text and isinstance(rich_text, list):
                parts = [item["text"] for item in rich_text
                         if isinstance(item, dict) and item.get("text")]
                content = " ".join(parts).strip()
        return content

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
                    return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
                body = resp.text
                logger.warning("[%s] Send failed HTTP %d: %s", self.name, resp.status_code, body[:200])
                return SendResult(success=False, error=f"HTTP {resp.status_code}: {body[:200]}")
            except httpx.TimeoutException:
                return SendResult(success=False, error="Timeout sending message to DingTalk")
            except Exception as e:
                logger.error("[%s] Send error: %s", self.name, e)
                return SendResult(success=False, error=str(e))

        # No session_webhook — use proactive robot API
        logger.debug("[%s] No session_webhook for %s, using proactive API", self.name, chat_id[:20])
        return await self._send_proactive(chat_id, content)

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
    """dingtalk-stream ChatbotHandler that forwards messages to the adapter."""

    def __init__(self, adapter: DingTalkAdapter, loop: asyncio.AbstractEventLoop):
        if DINGTALK_STREAM_AVAILABLE:
            super().__init__()
        self._adapter = adapter
        self._loop = loop

    def process(self, message: "ChatbotMessage"):
        """Called by dingtalk-stream in its thread when a message arrives.

        Schedules the async handler on the main event loop.
        """
        loop = self._loop
        if loop is None or loop.is_closed():
            logger.error("[DingTalk] Event loop unavailable, cannot dispatch message")
            return dingtalk_stream.AckMessage.STATUS_OK, "OK"

        future = asyncio.run_coroutine_threadsafe(self._adapter._on_message(message), loop)
        try:
            future.result(timeout=60)
        except Exception:
            logger.exception("[DingTalk] Error processing incoming message")

        return dingtalk_stream.AckMessage.STATUS_OK, "OK"
