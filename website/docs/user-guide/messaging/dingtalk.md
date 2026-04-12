---
sidebar_position: 10
title: "DingTalk"
description: "Set up Hermes Agent as a DingTalk chatbot"
---

# DingTalk Setup

Hermes Agent integrates with DingTalk (钉钉) as a chatbot. Once connected, you can chat with your AI assistant through direct messages or group chats. The bot connects via DingTalk's Stream Mode — a long-lived WebSocket connection initiated from your machine — so no public IP, domain name, or webhook server is needed.

## How Hermes Behaves

| Context | Behavior |
|---------|----------|
| **DMs (1:1 chat)** | Hermes responds to every message. No `@mention` needed. Each DM has its own session. |
| **Group chats** | Hermes responds only when `@mentioned`. Messages without a mention are not delivered to the bot by DingTalk's platform. |
| **Multiple users in a group** | By default, each user gets their own isolated session inside the group. Two users in the same group do not share a conversation history. |
| **ACK reaction** | When your message arrives, Hermes adds a permanent "收到" reaction as a delivery receipt. |

### Supported media types

Hermes can send and receive the following in DingTalk:

- **Images** — inline display
- **Files** — arbitrary file attachments
- **Video** — video file attachments
- **Voice messages** — outbound only; inbound voice transcription is not yet supported

## Step 1: Create a DingTalk Robot App (创建机器人应用)

1. Go to [DingTalk Developer Console](https://open-dev.dingtalk.com/fe/app) and log in.

2. At the top of the page, find the banner **"一键自动创建OpenClaw机器人应用，开启智能协作之旅"** and click **"立刻创建" (Create Now)** on the right.

   ![DingTalk developer console showing the 立刻创建 button](/img/docs/dingtalk/console-home.webp)

3. A dialog appears. Fill in:
   - **机器人名称 (Robot name)** — e.g., `Hermes`
   - **机器人简介 (Description)** — a short description
   - **机器人图标 (Icon)** — upload a 1:1 square JPG/PNG, at least 240×240 px, under 2 MB

   ![Robot creation dialog](/img/docs/dingtalk/create-robot-dialog.webp)

   Click **确认 (Confirm)** to create the app.

4. After creation, go to **凭证与基础信息 (Credentials & Basic Info)** in the left sidebar. Copy your **Client ID (AppKey)** and **Client Secret (AppSecret)**.


## Step 2: Configure Who Can Access the Robot (配置访问范围)

When `DINGTALK_ALLOW_ALL_USERS=true` is set (as recommended below), Hermes delegates access control to DingTalk's admin console rather than maintaining its own allowlist. By default, only the app creator can interact with the robot. To grant access to other users or departments:

1. In your app's left sidebar, click **版本管理与发布 (Version Management & Release)**.
2. Click **新建版本 (Create Version)**, enter a version number and description.
3. Under **应用可见范围 (App Availability Scope)**, select the appropriate scope:
   - **全部员工 (All employees)**
   - **部分员工 (Some employees)** — select specific people or departments
   - **仅限管理员 (Admins only)**
   - **仅供内部 (Internal only)**

   ![App Availability Scope configuration](/img/docs/dingtalk/access-scope.webp)

4. Click **发布 (Publish)** to apply the changes.

:::tip
For personal use, the default (creator only) is fine — no version publishing needed. For team deployments, publish a version with the appropriate scope before sharing the bot.
:::

## Step 3: Configure Hermes

### Option A: Interactive Setup (Recommended)

```bash
hermes gateway setup
```

Select **DingTalk** when prompted. The wizard walks you through entering your Client ID and Client Secret, explains the access control model, and writes the configuration for you.

### Option B: Manual Configuration

Add the following to `~/.hermes/.env`:

```bash
DINGTALK_CLIENT_ID=your-client-id
DINGTALK_CLIENT_SECRET=your-client-secret
DINGTALK_ALLOW_ALL_USERS=true
```

`DINGTALK_ALLOW_ALL_USERS=true` tells Hermes to trust DingTalk's platform-level access control (the App Availability Scope you configured in Step 2) rather than maintaining a separate user allowlist.

### Start the Gateway

```bash
hermes gateway
```

The bot connects to DingTalk's Stream Mode within a few seconds. Send it a direct message to verify it responds.

## AI Card Streaming (Optional)

AI Card lets Hermes display tool call progress in real time inside a rich card UI, updating in place as each tool runs. Without this, tool progress updates are sent as markdown messages.

To enable it, you first need to create a card template in the [DingTalk Card Platform](https://open-dev.dingtalk.com/fe/card):

1. Go to the [Card Platform](https://open-dev.dingtalk.com/fe/card) and click **新建AI卡片 (New AI Card)**, or reuse an existing one.
2. Define a streaming text variable in the template (the field that will receive the live output). Note its **variable name** — this becomes the `card_template_key`.
3. Copy the **模板 ID (Template ID)** from the template detail page.

Reference: [AI Card Template documentation](https://open.dingtalk.com/document/development/ai-card-template)

Then add the following to `~/.hermes/config.yaml`:

```yaml
platforms:
  dingtalk:
    extra:
      card_template_id: "your-template-id"
      card_template_key: "content"   # variable name in your template; default is "content"
```

Restart the gateway after making this change. When active, Hermes creates a live-updating card for tool call progress updates instead of sending markdown progress messages.

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Bot is not responding | Verify `DINGTALK_CLIENT_ID` and `DINGTALK_CLIENT_SECRET` are correct in `~/.hermes/.env`. Check `~/.hermes/logs/gateway.log` for errors. |
| `dingtalk-stream not installed` error | Run `pip install "hermes-agent[dingtalk]"` or `pip install dingtalk-stream`. |
| Bot responds in group only when `@mentioned` | This is expected behavior — DingTalk only delivers group messages to the bot when it is `@mentioned`. |
| Stream disconnects repeatedly | The adapter reconnects automatically with exponential backoff (base delays: 2 s, 5 s, 10 s, 30 s, 60 s, with ±20% jitter). Check that your credentials are valid and your network allows outbound WebSocket connections. |
| User cannot reach the bot | Check the App Availability Scope (Step 2) — the user or their department may not be included in the published version. |
| AI Card not updating live | Verify `card_template_id` is set correctly in `config.yaml` and that the template has been published in the Card Platform. |

