# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Baileys is a WebSocket-based TypeScript library (v6.7.18) for interacting with the WhatsApp Web API directly — no browser or Selenium required. It communicates with WhatsApp's servers over a raw WebSocket using the Noise protocol for transport encryption and the Signal protocol for end-to-end message encryption.

Published to npm as both `baileys` and `@whiskeysockets/baileys`. Node.js ≥20 required.

## Commands

```bash
yarn                        # install dependencies
yarn build:tsc              # compile TypeScript → lib/
yarn lint                   # ESLint
yarn lint:fix               # Prettier + ESLint --fix
yarn format                 # Prettier only
yarn test                   # all Jest tests
yarn test --testPathPattern=test.messages   # single test file
yarn example                # run Example/example.ts via ts-node
yarn gen:protobuf           # regenerate WAProto/index.js + index.d.ts from WAProto.proto
```

The compiled output goes to `lib/` (gitignored). The package ships `lib/`, `WAProto/`, and `engine-requirements.js`.

> **Note:** Running tests is not required when working in this repo. Use `yarn build:tsc` for type-checking and rely on static code analysis. The test suite requires a live WhatsApp connection for most cases and is not part of the normal development validation flow.

## Architecture

### Socket Composition Chain

The public entry point `makeWASocket` is built by composing socket layers — each layer calls the one below it and spreads its returned object, adding new methods:

```
makeSocket (socket.ts)           — WebSocket, Noise handshake, tag/callback system, keepalive
  └─ makeUSyncSocket (usync.ts)  — USync batch user-info queries
      └─ makeChatsSocket (chats.ts) — app-state sync (LTHash patches), chat/presence/privacy ops
          └─ makeGroupsSocket (groups.ts) — group CRUD and participant management
              └─ makeNewsletterSocket (newsletter.ts) — WhatsApp Channels/Newsletters (WMex GraphQL-like queries)
                  └─ makeMessagesSocket (messages-send.ts) — message composition, encryption, relay
                      └─ makeMessagesRecvSocket (messages-recv.ts) — inbound decryption, retries, receipts
                          └─ makeBusinessSocket (business.ts) — catalog, orders, business profile
                              └─ makeWASocket (Socket/index.ts) — merges DEFAULT_CONNECTION_CONFIG and exports
```

This is pure functional composition, not class inheritance. Each layer returns a plain object; callers spread it and add their own methods.

### Binary Protocol Layers

WhatsApp uses two serialization formats:

1. **WABinary** (`src/WABinary/`) — Custom binary encoding for the `BinaryNode` type used in all server ↔ client frames.  
   `BinaryNode = { tag: string; attrs: { [key: string]: string }; content?: BinaryNode[] | string | Uint8Array }`  
   Utility functions for navigating nodes: `getBinaryNodeChild`, `getBinaryNodeChildren`, `assertNodeErrorFree`, etc.

2. **WAProto** (`WAProto/`) — Compiled protobuf (protobufjs static module) for `proto.Message`, `proto.IWebMessageInfo`, and the rest of WhatsApp's data structures. Regenerated with `yarn gen:protobuf`.

### Transport Security (Noise Protocol)

`src/Utils/noise-handler.ts` implements the Noise XX handshake (`Noise_XX_25519_AESGCM_SHA256`). All frames are encrypted/decrypted here before being handed to the WABinary layer. An ephemeral key pair is generated per connection.

### End-to-End Encryption (Signal Protocol)

Individual messages: handled by the `libsignal` dependency via `src/Signal/libsignal.ts` which implements `SignalRepository`.  
Group messages: sender-key scheme implemented in TypeScript in `src/Signal/Group/` (mirrored as legacy JS in `WASignalGroup/`).

The `makeSignalRepository` config option lets callers swap the Signal backend.

### Auth State

`AuthenticationState` has two parts:
- `creds` — long-lived key material (`AuthenticationCreds`): Noise key pair, Signal identity, prekeys, device registration info.
- `keys` — `SignalKeyStore`: per-session Signal keys, sender keys, app-state keys.

`useMultiFileAuthState(folder)` is the reference file-system implementation. It uses per-file mutexes to avoid async write races. For production, implement a DB-backed `AuthenticationState` directly.

`makeCacheableSignalKeyStore` wraps any `SignalKeyStore` with an in-memory NodeCache to reduce I/O.

### Event System

`makeEventBuffer` (wraps Node's `EventEmitter`) is the internal event bus. Key properties:
- `.process(handler)` — registers a batch handler; receives a `BaileysEventData` map of all events fired since the last flush.
- `.buffer()` / `.flush()` — batches events across async operations (used during app-state sync to coalesce many small updates into one emission).
- Events that are bufferable: `messages.upsert`, `chats.upsert`, `contacts.upsert`, `messaging-history.set`, and related update/delete events.

Listen with `.ev.on('messages.upsert', handler)` or prefer `.ev.process(handler)` for efficient batch processing.

### JID Format

WhatsApp uses JIDs (Jabber IDs) as addresses:
- Users: `<number>@s.whatsapp.net`
- Groups: `<id>@g.us`
- Broadcast/Status: `status@broadcast`
- Newsletters: `<id>@newsletter`
- LID (linked identity): `<lid>@lid`

Utility functions in `src/WABinary/jid-utils.ts`: `jidDecode`, `jidEncode`, `jidNormalizedUser`, `isJidGroup`, `isJidUser`, `isJidNewsletter`, `areJidsSameUser`.

### WAUSync

`src/WAUSync/` provides a builder pattern for USync queries (batch user-info lookups). Use `new USyncQuery().withUser(...).withProtocol(...)` to query device lists, contact info, disappearing mode, status, and bot profiles in a single round-trip.

### WAM

`src/WAM/` encodes WhatsApp Analytics Metrics (WAM) binary format. `encodeWAM` produces the binary blob sent in `<ack>` frames.

### Media

Media upload/download is in `src/Utils/messages-media.ts`. Files are AES-GCM encrypted with HKDF-derived keys (key derivation info strings are in `MEDIA_HKDF_KEY_MAPPING`). Thumbnails use `jimp` or `sharp` (both optional peer deps). Media is uploaded to WhatsApp CDN hosts discovered via a `<media_conn>` query.

## Key Conventions

- **Tabs** for indentation (not spaces), no semicolons, single quotes — enforced by Prettier.
- `BufferJSON.replacer` / `BufferJSON.reviver` must be used when serializing/deserializing any object that contains `Buffer` values (e.g., auth state JSON).
- `@typescript-eslint/no-explicit-any` is a warning (not an error); use it sparingly.
- Tests live in `src/Tests/test.*.ts` and are excluded from the main `tsconfig.json` compilation.
- The `lib/` directory is the compiled output and is committed to git for consumers who install from GitHub directly.

## Configuration Defaults

`DEFAULT_CONNECTION_CONFIG` in `src/Defaults/index.ts` documents every configurable option. Commonly overridden:
- `auth` — required; no default
- `version` — loaded from `src/Defaults/baileys-version.json`; use `fetchLatestBaileysVersion()` to get the current WA Web version
- `getMessage` — required for message retry to work; return the stored `WAMessage` by key
- `cachedGroupMetadata` — avoid redundant group-info fetches on startup
- `syncFullHistory` — set `true` to request full message history on first link
- `shouldIgnoreJid` — filter out unwanted JIDs (e.g., broadcast) from all events

## Release Process

Releases are triggered by pushing a `v*` tag. CI publishes to npm as both `baileys` and `@whiskeysockets/baileys`, then creates a GitHub release with a generated changelog.
