# CLAUDE.md

## Gateway Process Management

The gateway writes a JSON PID file at `$HERMES_HOME/gateway.pid` (default: `~/.hermes/gateway.pid`).

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
# Live log (tailed from file)
cat /tmp/hermes-gw.log

# Or the persistent log
tail -f ~/.hermes/logs/gateway.log
```

## Configuration

- Config file: `~/.hermes/config.yaml`
- Env vars / secrets: `~/.hermes/.env`

### DingTalk

DingTalk requires credentials in `.env` (not just `config.yaml`), because `check_dingtalk_requirements()` in `gateway/platforms/dingtalk.py:89` only reads env vars:

```bash
# ~/.hermes/.env
DINGTALK_CLIENT_ID=ding4gcfp8uozm5hhejo
DINGTALK_CLIENT_SECRET=sNvDALCayUVa1Xv9YWc4KcTE8FWzWBo6YBgEm8eFwsxD6rboi_GawBCXXJ8R0Ew0
```

`config.yaml` still needs the `platforms.dingtalk` block for `robot_code` and other settings:

```yaml
platforms:
  dingtalk:
    enabled: true
    extra:
      client_id: "ding4gcfp8uozm5hhejo"
      client_secret: "sNvDALCayUVa1Xv9YWc4KcTE8FWzWBo6YBgEm8eFwsxD6rboi_GawBCXXJ8R0Ew0"
      robot_code: "ding4gcfp8uozm5hhejo"
      ack_reaction: "emoji"
```
