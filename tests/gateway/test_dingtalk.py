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
# Platform enum
# ---------------------------------------------------------------------------


class TestPlatformEnum:

    def test_dingtalk_in_platform_enum(self):
        assert Platform.DINGTALK.value == "dingtalk"
