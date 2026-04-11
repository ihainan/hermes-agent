# CLAUDE.md

## Gateway Process Management

The gateway writes a JSON PID file at `$HERMES_HOME/gateway.pid` (default: `~/.hermes/gateway.pid`).
The PID file is JSON, not a plain integer — always parse it.

### Stop the gateway

```bash
# Graceful stop via PID file (preferred)
kill $(python3 -c "import json; print(json.load(open('/home/ihainan/.hermes/gateway.pid'))['pid'])")

# Force kill if graceful stop fails
kill -9 $(python3 -c "import json; print(json.load(open('/home/ihainan/.hermes/gateway.pid'))['pid'])") && \
  rm -f /home/ihainan/.hermes/gateway.pid
```

### Start the gateway

```bash
.venv/bin/python -u -m gateway.run --verbose > /tmp/hermes-gw.log 2>&1 &
```

### Restart (stop then start)

```bash
kill $(python3 -c "import json; print(json.load(open('/home/ihainan/.hermes/gateway.pid'))['pid'])") 2>/dev/null || true
.venv/bin/python -u -m gateway.run --verbose > /tmp/hermes-gw.log 2>&1 &
```

Note: `--replace` is NOT a valid CLI argument for `run.py` (it exists only as a `start_gateway()` function parameter). Always use the PID file approach above.

### Check logs

```bash
# Startup logs only (stderr from --verbose, stops being updated after startup)
cat /tmp/hermes-gw.log

# Full runtime logs — message processing, reactions, send errors (INFO+)
tail -f ~/.hermes/logs/gateway.log
```

`/tmp/hermes-gw.log` only captures the early startup phase. All message-handling logs
(inbound messages, reactions, LLM calls, send results) go to `~/.hermes/logs/gateway.log`.

## DingTalk: Real-Device Testing Pitfalls

### After kill -9, wait ~30–60s before testing

When the gateway is force-killed (`kill -9`), the DingTalk server does not immediately
know the WebSocket is dead. It continues routing messages to the old (dead) connection
for ~30–60 seconds until its own keepalive timeout fires. The new process will be
connected and logged "✓ dingtalk connected" but will receive NO messages during this window.

**Always prefer `kill` (SIGTERM) over `kill -9`.** SIGTERM lets the SDK close the WebSocket
cleanly, which tells DingTalk to switch routing immediately. With SIGTERM the routing
cutover is near-instant.

### Log location

- Message processing, reactions, LLM calls: `~/.hermes/logs/gateway.log` (INFO+)
- Debug-level logs (reaction OK, token refresh): only visible with `--verbose` to stderr

## Configuration

- Config file: `~/.hermes/config.yaml`
- Env vars / secrets: `~/.hermes/.env`

### DingTalk

DingTalk requires credentials in `.env` **and** `config.yaml`. `check_dingtalk_requirements()`
in `gateway/platforms/dingtalk.py:89` reads only env vars, so both files are needed:

```bash
# ~/.hermes/.env
DINGTALK_CLIENT_ID=ding4gcfp8uozm5hhejo
DINGTALK_CLIENT_SECRET=sNvDALCayUVa1Xv9YWc4KcTE8FWzWBo6YBgEm8eFwsxD6rboi_GawBCXXJ8R0Ew0
```

```yaml
# ~/.hermes/config.yaml
platforms:
  dingtalk:
    enabled: true
    extra:
      client_id: "ding4gcfp8uozm5hhejo"
      client_secret: "sNvDALCayUVa1Xv9YWc4KcTE8FWzWBo6YBgEm8eFwsxD6rboi_GawBCXXJ8R0Ew0"
      robot_code: "ding4gcfp8uozm5hhejo"
      ack_reaction: "emoji"
```

### LLM (MiniMax)

```yaml
# ~/.hermes/config.yaml
model:
  provider: "minimax-cn"
  default: "MiniMax-M2.7"
```

```bash
# ~/.hermes/.env
MINIMAX_CN_API_KEY=sk-cp-...
```

## Running Tests

```bash
# Full DingTalk adapter test suite
PYTHONPATH=. .venv/bin/python -m pytest tests/gateway/test_dingtalk.py -q

# All tests
PYTHONPATH=. .venv/bin/python -m pytest -q
```
