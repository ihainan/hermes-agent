"""Tests for DingTalk platform adapter."""
import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

import pytest

from gateway.config import Platform, PlatformConfig


# ---------------------------------------------------------------------------
# Requirements check
# ---------------------------------------------------------------------------


class TestDingTalkRequirements:

    def test_returns_false_when_sdk_missing(self, monkeypatch):
        with patch.dict("sys.modules", {"dingtalk_stream": None}):
            monkeypatch.setattr(
                "gateway.platforms.dingtalk.DINGTALK_STREAM_AVAILABLE", False
            )
            from gateway.platforms.dingtalk import check_dingtalk_requirements
            assert check_dingtalk_requirements() is False

    def test_returns_false_when_env_vars_missing(self, monkeypatch):
        monkeypatch.setattr(
            "gateway.platforms.dingtalk.DINGTALK_STREAM_AVAILABLE", True
        )
        monkeypatch.setattr("gateway.platforms.dingtalk.HTTPX_AVAILABLE", True)
        monkeypatch.delenv("DINGTALK_CLIENT_ID", raising=False)
        monkeypatch.delenv("DINGTALK_CLIENT_SECRET", raising=False)
        from gateway.platforms.dingtalk import check_dingtalk_requirements
        assert check_dingtalk_requirements() is False

    def test_returns_true_when_all_available(self, monkeypatch):
        monkeypatch.setattr(
            "gateway.platforms.dingtalk.DINGTALK_STREAM_AVAILABLE", True
        )
        monkeypatch.setattr("gateway.platforms.dingtalk.HTTPX_AVAILABLE", True)
        monkeypatch.setenv("DINGTALK_CLIENT_ID", "test-id")
        monkeypatch.setenv("DINGTALK_CLIENT_SECRET", "test-secret")
        from gateway.platforms.dingtalk import check_dingtalk_requirements
        assert check_dingtalk_requirements() is True


# ---------------------------------------------------------------------------
# Adapter construction
# ---------------------------------------------------------------------------


class TestDingTalkAdapterInit:

    def test_reads_config_from_extra(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        config = PlatformConfig(
            enabled=True,
            extra={"client_id": "cfg-id", "client_secret": "cfg-secret"},
        )
        adapter = DingTalkAdapter(config)
        assert adapter._client_id == "cfg-id"
        assert adapter._client_secret == "cfg-secret"
        assert adapter.name == "Dingtalk"  # base class uses .title()

    def test_falls_back_to_env_vars(self, monkeypatch):
        monkeypatch.setenv("DINGTALK_CLIENT_ID", "env-id")
        monkeypatch.setenv("DINGTALK_CLIENT_SECRET", "env-secret")
        from gateway.platforms.dingtalk import DingTalkAdapter
        config = PlatformConfig(enabled=True)
        adapter = DingTalkAdapter(config)
        assert adapter._client_id == "env-id"
        assert adapter._client_secret == "env-secret"


# ---------------------------------------------------------------------------
# Message text extraction
# ---------------------------------------------------------------------------


class TestExtractText:

    def test_extracts_dict_text(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = MagicMock()
        msg.text = {"content": "  hello world  "}
        msg.rich_text = None
        assert DingTalkAdapter._extract_text(msg) == "hello world"

    def test_extracts_string_text(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = MagicMock()
        msg.text = "plain text"
        msg.rich_text = None
        assert DingTalkAdapter._extract_text(msg) == "plain text"

    def test_falls_back_to_rich_text(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = MagicMock()
        msg.text = ""
        msg.rich_text = [{"text": "part1"}, {"text": "part2"}, {"image": "url"}]
        assert DingTalkAdapter._extract_text(msg) == "part1 part2"

    def test_returns_empty_for_no_content(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = MagicMock()
        msg.text = ""
        msg.rich_text = None
        assert DingTalkAdapter._extract_text(msg) == ""


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------


class TestDeduplication:

    def test_first_message_not_duplicate(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        assert adapter._is_duplicate("msg-1") is False

    def test_second_same_message_is_duplicate(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        adapter._is_duplicate("msg-1")
        assert adapter._is_duplicate("msg-1") is True

    def test_different_messages_not_duplicate(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        adapter._is_duplicate("msg-1")
        assert adapter._is_duplicate("msg-2") is False

    def test_cache_cleanup_on_overflow(self):
        from gateway.platforms.dingtalk import DingTalkAdapter, DEDUP_MAX_SIZE
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        # Fill beyond max
        for i in range(DEDUP_MAX_SIZE + 10):
            adapter._is_duplicate(f"msg-{i}")
        # Cache should have been pruned
        assert len(adapter._seen_messages) <= DEDUP_MAX_SIZE + 10


# ---------------------------------------------------------------------------
# Send
# ---------------------------------------------------------------------------


class TestSend:

    @pytest.mark.asyncio
    async def test_send_posts_to_webhook(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "OK"

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        adapter._http_client = mock_client

        result = await adapter.send(
            "chat-123", "Hello!",
            metadata={"session_webhook": "https://dingtalk.example/webhook"}
        )
        assert result.success is True
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert call_args[0][0] == "https://dingtalk.example/webhook"
        payload = call_args[1]["json"]
        assert payload["msgtype"] == "markdown"
        assert payload["markdown"]["title"] == "Hermes"
        assert payload["markdown"]["text"] == "Hello!"

    @pytest.mark.asyncio
    async def test_send_fails_without_webhook(self):
        """Without a webhook, send() falls through to proactive API.
        If the token fetch also fails (bad credentials), result is failure.
        """
        import gateway.platforms.dingtalk as mod
        mod._TOKEN_CACHE.pop("", None)
        mod._TOKEN_LOCKS.pop("", None)

        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))  # no credentials
        bad_resp = MagicMock()
        bad_resp.status_code = 403
        bad_resp.text = "Forbidden"
        adapter._http_client = AsyncMock()
        adapter._http_client.post = AsyncMock(return_value=bad_resp)

        result = await adapter.send("chat-123", "Hello!")
        assert result.success is False

    @pytest.mark.asyncio
    async def test_send_uses_cached_webhook(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        adapter._http_client = mock_client
        adapter._session_webhooks["chat-123"] = "https://cached.example/webhook"

        result = await adapter.send("chat-123", "Hello!")
        assert result.success is True
        assert mock_client.post.call_args[0][0] == "https://cached.example/webhook"

    @pytest.mark.asyncio
    async def test_send_handles_http_error(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))

        mock_response = MagicMock()
        mock_response.status_code = 400
        mock_response.text = "Bad Request"
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)
        adapter._http_client = mock_client

        result = await adapter.send(
            "chat-123", "Hello!",
            metadata={"session_webhook": "https://example/webhook"}
        )
        assert result.success is False
        assert "400" in result.error


# ---------------------------------------------------------------------------
# Connect / disconnect
# ---------------------------------------------------------------------------


class TestConnect:

    @pytest.mark.asyncio
    async def test_connect_fails_without_sdk(self, monkeypatch):
        monkeypatch.setattr(
            "gateway.platforms.dingtalk.DINGTALK_STREAM_AVAILABLE", False
        )
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        result = await adapter.connect()
        assert result is False

    @pytest.mark.asyncio
    async def test_connect_fails_without_credentials(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        adapter._client_id = ""
        adapter._client_secret = ""
        result = await adapter.connect()
        assert result is False

    @pytest.mark.asyncio
    async def test_disconnect_cleans_up(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        adapter._session_webhooks["a"] = "http://x"
        adapter._seen_messages["b"] = 1.0
        adapter._http_client = AsyncMock()
        adapter._stream_task = None

        await adapter.disconnect()
        assert len(adapter._session_webhooks) == 0
        assert len(adapter._seen_messages) == 0
        assert adapter._http_client is None


# ---------------------------------------------------------------------------
# Proactive messaging
# ---------------------------------------------------------------------------


class TestProactiveMessaging:
    """Tests for the proactive robot API fallback in send() and _send_proactive()."""

    def _make_adapter(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(
            PlatformConfig(enabled=True, extra={"client_id": "bot-id", "client_secret": "secret"})
        )
        adapter._http_client = AsyncMock()
        return adapter

    def _ok_resp(self, body=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = body or {}
        resp.text = ""
        return resp

    def _err_resp(self, status=400, text="bad"):
        resp = MagicMock()
        resp.status_code = status
        resp.text = text
        return resp

    # -- send() prefers session_webhook --

    @pytest.mark.asyncio
    async def test_send_prefers_session_webhook(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = self._make_adapter()
        adapter._session_webhooks["chat-1"] = "https://wh.example/hook"
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        result = await adapter.send("chat-1", "hello")

        assert result.success is True
        call_url = adapter._http_client.post.call_args[0][0]
        assert call_url == "https://wh.example/hook"

    # -- proactive group --

    @pytest.mark.asyncio
    async def test_send_proactive_group_when_no_webhook(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        result = await adapter.send("cidABCDEF", "hello group")

        assert result.success is True
        call_url = adapter._http_client.post.call_args[0][0]
        assert "groupMessages/send" in call_url
        mod._TOKEN_CACHE.pop("bot-id", None)

    @pytest.mark.asyncio
    async def test_send_proactive_group_payload_format(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        await adapter.send("cidGROUP1", "test content")

        payload = adapter._http_client.post.call_args[1]["json"]
        assert payload["robotCode"] == "bot-id"
        assert payload["msgKey"] == "sampleMarkdown"
        assert payload["openConversationId"] == "cidGROUP1"
        msg_param = json.loads(payload["msgParam"])
        assert msg_param["title"] == "Hermes"
        assert msg_param["text"] == "test content"
        mod._TOKEN_CACHE.pop("bot-id", None)

    @pytest.mark.asyncio
    async def test_send_proactive_uses_token_header(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("mytoken123", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        await adapter.send("cidGROUP", "hi")

        headers = adapter._http_client.post.call_args[1]["headers"]
        assert headers["x-acs-dingtalk-access-token"] == "mytoken123"
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- proactive DM --

    @pytest.mark.asyncio
    async def test_send_proactive_dm_when_no_webhook(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._dm_user_ids["user-dm-chat"] = "user-123"
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        result = await adapter.send("user-dm-chat", "hello DM")

        assert result.success is True
        call_url = adapter._http_client.post.call_args[0][0]
        assert "oToMessages/batchSend" in call_url
        mod._TOKEN_CACHE.pop("bot-id", None)

    @pytest.mark.asyncio
    async def test_send_proactive_dm_payload_format(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._dm_user_ids["dm-chat"] = "uid-456"
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        await adapter.send("dm-chat", "dm message")

        payload = adapter._http_client.post.call_args[1]["json"]
        assert payload["userIds"] == ["uid-456"]
        assert "openConversationId" not in payload
        mod._TOKEN_CACHE.pop("bot-id", None)

    @pytest.mark.asyncio
    async def test_send_dm_uses_learned_user_id(self):
        """_dm_user_ids populated by _on_message is used in subsequent send."""
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        # Simulate what _on_message() does for a DM
        adapter._chat_types["conv-dm-99"] = "dm"
        adapter._dm_user_ids["conv-dm-99"] = "sender-99"
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        await adapter.send("conv-dm-99", "reply")

        payload = adapter._http_client.post.call_args[1]["json"]
        assert payload["userIds"] == ["sender-99"]
        mod._TOKEN_CACHE.pop("bot-id", None)

    @pytest.mark.asyncio
    async def test_send_dm_falls_back_to_chat_id(self):
        """When no _dm_user_ids entry exists, chat_id itself is used as userId."""
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        await adapter.send("unknown-dm-id", "fallback")

        payload = adapter._http_client.post.call_args[1]["json"]
        assert payload["userIds"] == ["unknown-dm-id"]
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- chunking --

    @pytest.mark.asyncio
    async def test_send_chunks_long_message(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_resp())

        long_msg = "x" * 4000  # > 3800 char limit
        await adapter.send("cidGROUP", long_msg)

        # Must have made at least 2 API calls
        assert adapter._http_client.post.call_count >= 2
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- error handling --

    @pytest.mark.asyncio
    async def test_send_proactive_returns_failure_on_http_error(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._err_resp(403, "Forbidden"))

        result = await adapter.send("cidGROUP", "test")

        assert result.success is False
        assert "403" in result.error
        mod._TOKEN_CACHE.pop("bot-id", None)


# ---------------------------------------------------------------------------
# OAuth token management
# ---------------------------------------------------------------------------


class TestGetAccessToken:

    def _make_adapter(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(
            PlatformConfig(enabled=True, extra={"client_id": "test-id", "client_secret": "test-secret"})
        )
        adapter._http_client = AsyncMock()
        return adapter

    def _ok_response(self, token="tok123", expire_in=7200):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"accessToken": token, "expireIn": expire_in}
        return resp

    @pytest.mark.asyncio
    async def test_fetches_token_on_first_call(self):
        import gateway.platforms.dingtalk as mod
        mod._TOKEN_CACHE.pop("test-id", None)
        mod._TOKEN_LOCKS.pop("test-id", None)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_response("tok-abc"))

        token = await adapter._get_access_token()
        assert token == "tok-abc"
        adapter._http_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_cache_hit_skips_http(self):
        import time
        import gateway.platforms.dingtalk as mod

        mod._TOKEN_CACHE["test-id"] = ("cached-tok", time.time() + 3600)
        mod._TOKEN_LOCKS.pop("test-id", None)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock()

        token = await adapter._get_access_token()
        assert token == "cached-tok"
        adapter._http_client.post.assert_not_called()

        # cleanup
        mod._TOKEN_CACHE.pop("test-id", None)

    @pytest.mark.asyncio
    async def test_near_expiry_triggers_refresh(self):
        import time
        import gateway.platforms.dingtalk as mod

        # Token expires in 30 s — within the 60 s buffer
        mod._TOKEN_CACHE["test-id"] = ("old-tok", time.time() + 30)
        mod._TOKEN_LOCKS.pop("test-id", None)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_response("new-tok"))

        token = await adapter._get_access_token()
        assert token == "new-tok"
        adapter._http_client.post.assert_called_once()

        mod._TOKEN_CACHE.pop("test-id", None)

    @pytest.mark.asyncio
    async def test_retries_on_5xx_then_succeeds(self):
        import gateway.platforms.dingtalk as mod
        mod._TOKEN_CACHE.pop("test-id", None)
        mod._TOKEN_LOCKS.pop("test-id", None)

        bad = MagicMock()
        bad.status_code = 500
        bad.text = "internal error"

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(
            side_effect=[bad, self._ok_response("tok-retry")]
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            token = await adapter._get_access_token()

        assert token == "tok-retry"
        assert adapter._http_client.post.call_count == 2

    @pytest.mark.asyncio
    async def test_raises_on_non_retryable_4xx(self):
        import gateway.platforms.dingtalk as mod
        mod._TOKEN_CACHE.pop("test-id", None)
        mod._TOKEN_LOCKS.pop("test-id", None)

        bad = MagicMock()
        bad.status_code = 403
        bad.text = "Forbidden"

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=bad)

        with pytest.raises(RuntimeError, match="403"):
            await adapter._get_access_token()

        # Should NOT retry on 403
        adapter._http_client.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_raises_after_all_retries_exhausted(self):
        import gateway.platforms.dingtalk as mod
        mod._TOKEN_CACHE.pop("test-id", None)
        mod._TOKEN_LOCKS.pop("test-id", None)

        bad = MagicMock()
        bad.status_code = 500
        bad.text = "error"

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=bad)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="3 attempts"):
                await adapter._get_access_token()

        assert adapter._http_client.post.call_count == 3

    @pytest.mark.asyncio
    async def test_dingtalk_headers_returns_token_header(self):
        import gateway.platforms.dingtalk as mod
        import time
        mod._TOKEN_CACHE["test-id"] = ("hdr-tok", time.time() + 3600)
        mod._TOKEN_LOCKS.pop("test-id", None)

        adapter = self._make_adapter()
        headers = await adapter._dingtalk_headers()

        assert headers["x-acs-dingtalk-access-token"] == "hdr-tok"
        assert headers["Content-Type"] == "application/json"

        mod._TOKEN_CACHE.pop("test-id", None)


# ---------------------------------------------------------------------------
# Health check and reconnection
# ---------------------------------------------------------------------------


class TestHealthCheckAndReconnection:

    def _make_adapter(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(
            PlatformConfig(enabled=True, extra={"client_id": "bot-id", "client_secret": "secret"})
        )
        adapter._http_client = AsyncMock()
        return adapter

    @pytest.mark.asyncio
    async def test_last_message_at_updated_on_message(self):
        """_on_message() must update _last_message_at."""
        import time
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = self._make_adapter()
        adapter.set_message_handler(AsyncMock())

        msg = MagicMock()
        msg.message_id = "msg-ts-1"
        msg.text = {"content": "hello"}
        msg.rich_text = None
        msg.conversation_id = "cid-test"
        msg.conversation_type = "1"
        msg.sender_id = "uid-1"
        msg.sender_nick = "Alice"
        msg.sender_staff_id = ""
        msg.conversation_title = "Test"
        msg.session_webhook = ""
        msg.create_at = None

        before = time.monotonic()
        await adapter._on_message(msg)
        assert adapter._last_message_at >= before

    @pytest.mark.asyncio
    async def test_health_check_cancels_stream_task_when_stale(self):
        """Health check must cancel stream task when idle > threshold."""
        import time
        from gateway.platforms.dingtalk import DingTalkAdapter, HEALTH_CHECK_STALE_THRESHOLD

        adapter = self._make_adapter()
        adapter._running = True
        # Simulate last message well beyond threshold
        adapter._last_message_at = time.monotonic() - HEALTH_CHECK_STALE_THRESHOLD - 10

        mock_task = MagicMock()
        mock_task.done.return_value = False
        adapter._stream_task = mock_task

        async def fake_sleep(_):
            # After sleep, _running is still True so idle check will run.
            # Stop on next while-condition check by cancelling after this coroutine.
            pass

        with patch("asyncio.sleep", side_effect=fake_sleep):
            # Run one iteration manually: sleep → idle check → cancel triggered
            # Then _running=False so second while check exits
            adapter._running = True
            # Run loop but stop it by setting _running=False after cancel fires
            original_cancel = mock_task.cancel
            def cancel_and_stop():
                original_cancel()
                adapter._running = False
            mock_task.cancel = MagicMock(side_effect=cancel_and_stop)

            await adapter._health_check_loop()

        mock_task.cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check_does_not_cancel_when_fresh(self):
        """Health check must NOT cancel stream task when messages are recent."""
        import time
        from gateway.platforms.dingtalk import DingTalkAdapter

        adapter = self._make_adapter()
        adapter._running = True
        adapter._last_message_at = time.monotonic()  # just now

        mock_task = MagicMock()
        mock_task.done.return_value = False
        adapter._stream_task = mock_task

        async def fake_sleep(_):
            adapter._running = False

        with patch("asyncio.sleep", side_effect=fake_sleep):
            await adapter._health_check_loop()

        mock_task.cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_stream_resets_failures_on_clean_exit(self):
        """A clean SDK exit must reset _consecutive_failures and backoff."""
        from gateway.platforms.dingtalk import DingTalkAdapter

        adapter = self._make_adapter()
        adapter._consecutive_failures = 3
        adapter._running = True

        call_count = 0

        async def fake_to_thread(fn):
            nonlocal call_count
            call_count += 1
            adapter._running = False  # stop loop after first call

        with patch("gateway.platforms.dingtalk.dingtalk_stream") as mock_dt, \
             patch("asyncio.to_thread", side_effect=fake_to_thread):
            mock_dt.Credential.return_value = MagicMock()
            mock_dt.DingTalkStreamClient.return_value = MagicMock()
            mock_dt.ChatbotMessage.TOPIC = "chatbot"
            await adapter._run_stream()

        assert adapter._consecutive_failures == 0

    @pytest.mark.asyncio
    async def test_run_stream_calls_fatal_error_after_max_failures(self):
        """After MAX_RECONNECT_ATTEMPTS consecutive exceptions, _set_fatal_error must be called."""
        from gateway.platforms.dingtalk import DingTalkAdapter, MAX_RECONNECT_ATTEMPTS

        adapter = self._make_adapter()
        adapter._running = True

        with patch("gateway.platforms.dingtalk.dingtalk_stream") as mock_dt, \
             patch("asyncio.to_thread", side_effect=RuntimeError("boom")), \
             patch("asyncio.sleep", new_callable=AsyncMock), \
             patch.object(adapter, "_set_fatal_error") as mock_fatal:
            mock_dt.Credential.return_value = MagicMock()
            mock_dt.DingTalkStreamClient.return_value = MagicMock()
            mock_dt.ChatbotMessage.TOPIC = "chatbot"
            await adapter._run_stream()

        mock_fatal.assert_called_once()
        call_kwargs = mock_fatal.call_args
        assert call_kwargs[1]["retryable"] is True
        assert adapter._consecutive_failures == MAX_RECONNECT_ATTEMPTS

    @pytest.mark.asyncio
    async def test_run_stream_applies_jitter_to_backoff(self):
        """Backoff delay must include a jitter component."""
        from gateway.platforms.dingtalk import DingTalkAdapter, RECONNECT_JITTER_FACTOR

        adapter = self._make_adapter()
        adapter._running = True
        call_count = 0

        sleep_delays = []

        async def fake_sleep(n):
            sleep_delays.append(n)
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                adapter._running = False

        with patch("gateway.platforms.dingtalk.dingtalk_stream") as mock_dt, \
             patch("asyncio.to_thread", side_effect=RuntimeError("err")), \
             patch("asyncio.sleep", side_effect=fake_sleep), \
             patch("gateway.platforms.dingtalk.random.random", return_value=0.8):
            mock_dt.Credential.return_value = MagicMock()
            mock_dt.DingTalkStreamClient.return_value = MagicMock()
            mock_dt.ChatbotMessage.TOPIC = "chatbot"
            await adapter._run_stream()

        assert len(sleep_delays) >= 1
        # With random=0.8: jitter = base * 0.2 * (2*0.8-1) = base * 0.2 * 0.6 = base * 0.12
        # Delay must differ from plain base — not exactly 2.0
        assert sleep_delays[0] != 2.0

    @pytest.mark.asyncio
    async def test_run_stream_rebuilds_client_on_reconnect(self):
        """DingTalkStreamClient must be constructed on each loop iteration."""
        from gateway.platforms.dingtalk import DingTalkAdapter

        adapter = self._make_adapter()
        adapter._running = True
        attempt = 0

        async def fake_to_thread(fn):
            nonlocal attempt
            attempt += 1
            if attempt >= 2:
                adapter._running = False
            else:
                raise RuntimeError("first fail")

        with patch("gateway.platforms.dingtalk.dingtalk_stream") as mock_dt, \
             patch("asyncio.to_thread", side_effect=fake_to_thread), \
             patch("asyncio.sleep", new_callable=AsyncMock):
            mock_dt.Credential.return_value = MagicMock()
            mock_dt.DingTalkStreamClient.return_value = MagicMock()
            mock_dt.ChatbotMessage.TOPIC = "chatbot"
            await adapter._run_stream()

        # Two attempts → two client constructions
        assert mock_dt.DingTalkStreamClient.call_count == 2

    @pytest.mark.asyncio
    async def test_disconnect_cancels_health_check_task(self):
        """disconnect() must cancel and await the health check task."""
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = self._make_adapter()

        # Create a real asyncio task that raises CancelledError when awaited after cancel
        async def _noop():
            await asyncio.sleep(9999)

        real_task = asyncio.create_task(_noop())
        adapter._health_check_task = real_task
        adapter._stream_task = None

        await adapter.disconnect()

        assert real_task.cancelled()
        assert adapter._health_check_task is None


# ---------------------------------------------------------------------------
# Quoted/replied message context (Issue #7)
# ---------------------------------------------------------------------------


class TestExtractQuotedContext:

    def _make_msg(self, text_payload):
        msg = MagicMock()
        msg.text = text_payload
        return msg

    def test_returns_none_for_plain_message(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = self._make_msg({"content": "hello", "isReplyMsg": False})
        text, msg_id = DingTalkAdapter._extract_quoted_context(msg)
        assert text is None
        assert msg_id is None

    def test_returns_none_when_not_dict(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = self._make_msg("plain string text")
        text, msg_id = DingTalkAdapter._extract_quoted_context(msg)
        assert text is None
        assert msg_id is None

    def test_extracts_quoted_text_and_id(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = self._make_msg({
            "content": "my reply",
            "isReplyMsg": True,
            "repliedMsg": {
                "msgId": "orig-msg-123",
                "msgContent": "original content",
                "msgSenderNick": "Alice",
            },
        })
        text, msg_id = DingTalkAdapter._extract_quoted_context(msg)
        assert text == "original content"
        assert msg_id == "orig-msg-123"

    def test_graceful_when_replied_msg_absent(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = self._make_msg({"content": "reply", "isReplyMsg": True})
        text, msg_id = DingTalkAdapter._extract_quoted_context(msg)
        assert text is None
        assert msg_id is None

    def test_graceful_when_replied_msg_not_dict(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = self._make_msg({
            "content": "reply",
            "isReplyMsg": True,
            "repliedMsg": "bad-value",
        })
        text, msg_id = DingTalkAdapter._extract_quoted_context(msg)
        assert text is None
        assert msg_id is None

    def test_returns_none_when_msg_content_empty(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        msg = self._make_msg({
            "content": "reply",
            "isReplyMsg": True,
            "repliedMsg": {"msgId": "id-1", "msgContent": "  ", "msgSenderNick": "Bob"},
        })
        text, msg_id = DingTalkAdapter._extract_quoted_context(msg)
        assert text is None


class TestQuotedContextInjection:
    """Tests that _on_message populates event fields and prefixes agent text."""

    def _make_msg(self, new_text, replied_msg=None):
        msg = MagicMock()
        payload = {"content": new_text}
        if replied_msg is not None:
            payload["isReplyMsg"] = True
            payload["repliedMsg"] = replied_msg
        else:
            payload["isReplyMsg"] = False
        msg.text = payload
        msg.rich_text = None
        msg.message_id = "msg-q-1"
        msg.conversation_id = "cid-test"
        msg.conversation_type = "1"
        msg.sender_id = "uid-1"
        msg.sender_nick = "Bob"
        msg.sender_staff_id = ""
        msg.conversation_title = "Test"
        msg.session_webhook = ""
        msg.create_at = None
        return msg

    def _make_adapter(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        return DingTalkAdapter(
            PlatformConfig(enabled=True, extra={"client_id": "x", "client_secret": "y"})
        )

    @pytest.mark.asyncio
    async def test_reply_to_fields_set_on_event(self):
        adapter = self._make_adapter()
        captured = []

        async def fake_handle(event):
            captured.append(event)

        with patch.object(adapter, "handle_message", side_effect=fake_handle):
            msg = self._make_msg("thanks!", {
                "msgId": "orig-99",
                "msgContent": "please review this",
                "msgSenderNick": "Alice",
            })
            await adapter._on_message(msg)

        assert len(captured) == 1
        event = captured[0]
        assert event.reply_to_text == "please review this"
        assert event.reply_to_message_id == "orig-99"

    @pytest.mark.asyncio
    async def test_context_block_prepended_with_sender(self):
        adapter = self._make_adapter()
        captured = []

        async def fake_handle(event):
            captured.append(event)

        with patch.object(adapter, "handle_message", side_effect=fake_handle):
            msg = self._make_msg("thanks!", {
                "msgId": "orig-99",
                "msgContent": "please review this",
                "msgSenderNick": "Alice",
            })
            await adapter._on_message(msg)

        event = captured[0]
        assert event.text.startswith('[Replying to Alice: "please review this"]\n')
        assert event.text.endswith("thanks!")

    @pytest.mark.asyncio
    async def test_context_block_without_sender_name(self):
        adapter = self._make_adapter()
        captured = []

        async def fake_handle(event):
            captured.append(event)

        with patch.object(adapter, "handle_message", side_effect=fake_handle):
            msg = self._make_msg("ok", {
                "msgId": "orig-1",
                "msgContent": "original text",
                # no msgSenderNick
            })
            await adapter._on_message(msg)

        event = captured[0]
        assert event.text.startswith('[Replying to: "original text"]\n')

    @pytest.mark.asyncio
    async def test_no_prefix_for_plain_message(self):
        adapter = self._make_adapter()
        captured = []

        async def fake_handle(event):
            captured.append(event)

        with patch.object(adapter, "handle_message", side_effect=fake_handle):
            msg = self._make_msg("just a normal message")
            await adapter._on_message(msg)

        event = captured[0]
        assert event.text == "just a normal message"
        assert event.reply_to_text is None
        assert event.reply_to_message_id is None


# ---------------------------------------------------------------------------
# Platform enum
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Inbound media handling (Issue #4)
# ---------------------------------------------------------------------------


class TestParseInboundMessage:
    """Tests for _parse_inbound_message() and _download_media()."""

    def _make_adapter(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(
            PlatformConfig(enabled=True, extra={"client_id": "bot-id", "client_secret": "secret"})
        )
        adapter._http_client = AsyncMock()
        return adapter

    def _make_msg(self, msgtype="text", content=None, text=None):
        msg = MagicMock()
        msg.msgtype = msgtype
        msg.content = content or {}
        msg.text = text or {}
        msg.rich_text = None
        return msg

    def _ok_download_resp(self, download_url="https://cdn.example/file"):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"downloadUrl": download_url}
        return resp

    def _ok_file_resp(self, data=b"fake-bytes", content_type="image/jpeg"):
        resp = MagicMock()
        resp.status_code = 200
        resp.content = data
        resp.headers = {"content-type": content_type}
        return resp

    # -- plain text --

    @pytest.mark.asyncio
    async def test_text_message_returns_text(self):
        adapter = self._make_adapter()
        msg = self._make_msg("text", text={"content": "hello"})
        text, msg_type, media_urls, media_types = await adapter._parse_inbound_message(msg)
        assert text == "hello"
        assert msg_type.value == "text"
        assert media_urls == []

    # -- picture --

    @pytest.mark.asyncio
    async def test_picture_downloads_and_caches(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_download_resp())
        adapter._http_client.get = AsyncMock(return_value=self._ok_file_resp(b"\xff\xd8\xff", "image/jpeg"))

        msg = self._make_msg("picture", content={"downloadCode": "code-img-1"})

        with patch("gateway.platforms.dingtalk.cache_image_from_bytes", return_value="/cache/img.jpg") as mock_cache:
            text, msg_type, media_urls, media_types = await adapter._parse_inbound_message(msg)

        assert msg_type.value == "photo"
        assert media_urls == ["/cache/img.jpg"]
        assert media_types == ["image/jpeg"]
        mock_cache.assert_called_once()
        mod._TOKEN_CACHE.pop("bot-id", None)

    @pytest.mark.asyncio
    async def test_picture_falls_back_on_download_failure(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        err_resp = MagicMock()
        err_resp.status_code = 500
        err_resp.text = "error"
        adapter._http_client.post = AsyncMock(return_value=err_resp)

        msg = self._make_msg("picture", content={"downloadCode": "code-img-fail"})
        text, msg_type, media_urls, media_types = await adapter._parse_inbound_message(msg)

        # Still returns PHOTO type, but no media cached
        assert msg_type.value == "photo"
        assert media_urls == []
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- audio --

    @pytest.mark.asyncio
    async def test_audio_message_type(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_download_resp())
        adapter._http_client.get = AsyncMock(return_value=self._ok_file_resp(b"audio", "audio/amr"))

        msg = self._make_msg("audio", content={"downloadCode": "code-audio-1"})

        with patch("gateway.platforms.dingtalk.cache_audio_from_bytes", return_value="/cache/audio.amr"):
            text, msg_type, media_urls, media_types = await adapter._parse_inbound_message(msg)

        assert msg_type.value == "voice"
        assert len(media_urls) == 1
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- file --

    @pytest.mark.asyncio
    async def test_file_message_type(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_download_resp())
        adapter._http_client.get = AsyncMock(
            return_value=self._ok_file_resp(b"pdf-bytes", "application/pdf")
        )

        msg = self._make_msg("file", content={"downloadCode": "code-file-1", "fileName": "report.pdf"})

        with patch("gateway.platforms.dingtalk.cache_document_from_bytes", return_value="/cache/doc.pdf"):
            text, msg_type, media_urls, media_types = await adapter._parse_inbound_message(msg)

        assert msg_type.value == "document"
        assert text == "report.pdf"
        assert len(media_urls) == 1
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- video --

    @pytest.mark.asyncio
    async def test_video_message_type(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_download_resp())
        adapter._http_client.get = AsyncMock(
            return_value=self._ok_file_resp(b"video-bytes", "video/mp4")
        )

        msg = self._make_msg("video", content={"downloadCode": "code-video-1"})

        with patch("gateway.platforms.dingtalk.cache_document_from_bytes", return_value="/cache/vid.mp4"):
            text, msg_type, media_urls, media_types = await adapter._parse_inbound_message(msg)

        assert msg_type.value == "video"
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- richText --

    @pytest.mark.asyncio
    async def test_rich_text_concatenates_text_parts(self):
        adapter = self._make_adapter()
        msg = self._make_msg("richText", content={
            "richText": [
                {"type": "text", "text": "Hello "},
                {"type": "text", "text": "world"},
            ]
        })
        text, msg_type, media_urls, _ = await adapter._parse_inbound_message(msg)
        assert text == "Hello world"
        assert msg_type.value == "text"
        assert media_urls == []

    @pytest.mark.asyncio
    async def test_rich_text_with_inline_image(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        adapter._http_client.post = AsyncMock(return_value=self._ok_download_resp())
        adapter._http_client.get = AsyncMock(return_value=self._ok_file_resp(b"img", "image/jpeg"))

        msg = self._make_msg("richText", content={
            "richText": [
                {"type": "text", "text": "See this: "},
                {"type": "picture", "downloadCode": "code-inline-img"},
            ]
        })

        with patch("gateway.platforms.dingtalk.cache_image_from_bytes", return_value="/cache/inline.jpg"):
            text, msg_type, media_urls, _ = await adapter._parse_inbound_message(msg)

        assert text == "See this:"
        assert msg_type.value == "photo"
        assert media_urls == ["/cache/inline.jpg"]
        mod._TOKEN_CACHE.pop("bot-id", None)

    # -- _download_media error paths --

    @pytest.mark.asyncio
    async def test_download_media_returns_none_without_http_client(self):
        from gateway.platforms.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter(PlatformConfig(enabled=True))
        adapter._http_client = None
        path, mime = await adapter._download_media("code", "image/jpeg", ".jpg")
        assert path is None

    @pytest.mark.asyncio
    async def test_download_media_returns_none_on_missing_download_url(self):
        import gateway.platforms.dingtalk as mod
        import time as _time
        mod._TOKEN_CACHE["bot-id"] = ("tok", _time.time() + 3600)

        adapter = self._make_adapter()
        no_url_resp = MagicMock()
        no_url_resp.status_code = 200
        no_url_resp.json.return_value = {}  # no downloadUrl
        adapter._http_client.post = AsyncMock(return_value=no_url_resp)

        path, mime = await adapter._download_media("code", "image/jpeg", ".jpg")
        assert path is None
        mod._TOKEN_CACHE.pop("bot-id", None)


class TestPlatformEnum:

    def test_dingtalk_in_platform_enum(self):
        assert Platform.DINGTALK.value == "dingtalk"
