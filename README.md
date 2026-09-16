# n8n-nodes-twitch

This is an n8n community node package for [Twitch](https://twitch.tv). It lets you use the Twitch API and Twitch EventSub in your n8n workflows.

This package is based on [yuniruyuni/n8n-nodes-twitch](https://github.com/yuniruyuni/n8n-nodes-twitch), with one key fix: **the original `Twitch Trigger` node opened a new Twitch EventSub WebSocket connection per trigger node**. Twitch limits WebSocket EventSub connections to 3 per (Client ID + User ID), so any workflow using more than 3 `Twitch Trigger` nodes failed to activate with:

```
Failed to create Twitch EventSub subscription: number of websocket transports limit exceeded
```

This fork fixes that by sharing a single EventSub WebSocket connection (and its subscriptions) across every `Twitch Trigger` node using the same credential, in-process, instead of opening one connection per node. You can now add as many `Twitch Trigger` nodes as you want on the same credential — they multiplex over the same connection instead of each requesting a new one.

n8n is a [fair-code licensed](https://docs.n8n.io/reference/license/) workflow automation platform.

## Installation

### Install from n8n (Recommended)

1. Go to **Settings** > **Community Nodes**
2. Click **Install**
3. Enter `@justonemonkey/n8n-nodes-twitch` in the package name field
4. Click **Install**

For other installation methods, follow the n8n [community nodes installation guide](https://docs.n8n.io/integrations/community-nodes/installation/).

## Nodes

This package provides two nodes:

### Twitch

An action node for calling the Twitch Helix API, covering the following resources:

- Ad
- Analytics
- Announcement
- Badge
- Ban
- Bits Leaderboard
- Channel
- Charity
- Chat Message
- Chat Settings
- Chatter
- Cheermote
- Clip
- Custom Reward
- Emote
- Game
- Goal
- Hype Train
- Moderation
- Moderator
- Poll
- Prediction
- Raid
- Redemption
- Schedule
- Search
- Shoutout
- Stream
- Subscription
- Team
- User
- Video
- VIP
- Whisper

### Twitch Trigger

A trigger node that listens to Twitch EventSub notifications over a shared WebSocket connection. Supported event categories:

- Automod events
- Channel events (update, follow, moderator changes, etc.)
- Channel chat events
- Channel points events (automatic rewards and custom reward redemptions)
- Charity events
- Goal events
- Hype Train events
- Poll events
- Prediction events
- Raid events
- Shared chat events
- Stream events (online/offline)
- User events
- Drop entitlement, extension, and conduit events

You can add multiple `Twitch Trigger` nodes to the same workflow (or across different workflows using the same credential) — each one subscribes to its chosen event, and all of them share a single underlying WebSocket connection per credential to stay within Twitch's connection limit.

## Credentials

This package uses **Twitch User OAuth2 API** credentials. You'll need a Twitch application (Client ID + Client Secret) from the [Twitch Developer Console](https://dev.twitch.tv/console/apps) to authenticate. See the [Twitch OAuth documentation](https://dev.twitch.tv/docs/authentication/getting-tokens-oauth/) for details.

## Development

This package is built with [@n8n/node-cli](https://www.npmjs.com/package/@n8n/node-cli).

```bash
npm install       # install dependencies
npm run dev        # start n8n with this node loaded and hot reload enabled
npm run lint        # check for errors and style issues
npm run build       # compile TypeScript to dist/
```

## Resources

- [n8n community nodes documentation](https://docs.n8n.io/integrations/community-nodes/)
- [Twitch API documentation](https://dev.twitch.tv/docs/api/)
- [Twitch EventSub documentation](https://dev.twitch.tv/docs/eventsub/)
- [Original project: yuniruyuni/n8n-nodes-twitch](https://github.com/yuniruyuni/n8n-nodes-twitch)

## License

[MIT](LICENSE.md)
