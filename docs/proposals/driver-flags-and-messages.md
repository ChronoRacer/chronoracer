# Proposal - Operator flags and messages for the driver

Status: **proposed, not implemented.** Replaces the driver message convention that came with Telemetry's Driver Radio
(`event` topic, key `MSG | BOX | FLAG | PACE`, plain-text body). The car side will be built in the
`chronoracer-dashboard` repository against this file. The Telemetry side (producer and panel) is open. Once both sides
exist, the wire contract moves to [Kafka](../component-kafka.md) (`event`) and the panel behaviour to
[Telemetry](../component-telemetry.md); this file is then deleted.

## Why

- The operator decides how long a message stays on the driver's display, and can take it down: one message or all.
- The driver does not acknowledge anything. The driver can only minimise a message, never remove it.
- A flag needs its colour, so `FLAG` plus free text is not enough.
- Several messages can be active at once.

## Wire contract

One Kafka record per operator action, on the topic named by
[`platform.json`](../../infra/contract/platform.json) `kafka_event_topic`.

| Part      | Value                                                                                    |
|-----------|------------------------------------------------------------------------------------------|
| Key       | Message type from the table below, or `ALL` (only with `"action": "clear"`).             |
| Value     | UTF-8 JSON object, fields below.                                                         |
| Timestamp | Producer `CreateTime` (the client default). The car uses it to drop late timed messages. |

### Types (record key)

| Key         | Group   | `text`   | Car display background                         |
|-------------|---------|----------|------------------------------------------------|
| `YELLOW`    | flag    | optional | yellow                                         |
| `RED`       | flag    | optional | red                                            |
| `GREEN`     | flag    | optional | green                                          |
| `BLUE`      | flag    | optional | blue                                           |
| `SAFETY`    | flag    | optional | bright yellow; shows "SAFETY CAR" without text |
| `CHECKERED` | flag    | optional | chequered                                      |
| `BOX`       | message | required | brown                                          |
| `PACE`      | message | required | navy                                           |
| `MSG`       | message | required | dark                                           |

A flag is identified by its colour on the display, so the car shows no flag name. With no `text` a flag is a plain
colour field.

### Value fields

| Field        | Type    | Required                                         | Meaning                                                                                                   |
|--------------|---------|--------------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| `action`     | string  | yes                                              | `"show"` or `"clear"`.                                                                                    |
| `text`       | string  | `show` of `BOX`, `PACE`, `MSG`; optional for flags | One line for the display: 1 to 200 characters, no control characters. Ignored on `clear`.                 |
| `duration_s` | integer | no                                               | Seconds on the driver's display: `1` to `3600`, or `-1` for "until cleared". Default `5`. Ignored on `clear`. |

### Examples

```
key "YELLOW"   value {"action": "show", "text": "Wypadek w zakręcie 3", "duration_s": -1}
key "RED"      value {"action": "show", "duration_s": 30}
key "BOX"      value {"action": "show", "text": "BOX BOX — zjedź w tym okrążeniu"}
key "YELLOW"   value {"action": "clear"}
key "ALL"      value {"action": "clear"}
```

### Rules

1. At most one message per type is active. A `show` for a type that is already active replaces it: new text, time
   counted again.
2. `clear` with a type key removes that type. `clear` with key `ALL` removes every active message. Clearing a type
   that is not active does nothing.
3. Order is the contract: a `clear` must reach the car after the `show` it cancels. `event` has one partition, so one
   producer with the delivery settings Telemetry already uses for this topic keeps the order.
4. A receiver ignores unknown JSON fields. It skips a record with an unknown key, a value that is not a JSON object,
   a missing or unknown `action`, a `duration_s` outside the range, or invalid `text`. A plain-text record in the old
   convention is therefore skipped, not shown.
5. The ten keys above are reserved on `event`. Other producers on this topic (analysis alerts and predictions, Relay
   operator events) must not use them.

## What the car does with it

Owner: `docs/specs/2026-11-16-flagi-i-komunikaty-operatora.md` in `chronoracer-dashboard`. Summary, so the panel can
be designed around it:

- The newest message takes the whole gauge screen. Older active messages sit in a strip above it, oldest on the left.
  When the full-screen message ends, the newest one from the strip that the driver has not minimised returns.
- Time runs from the moment the record reaches the car.
- The driver can press a button to move every active message to the strip and see the gauges again. The driver cannot
  remove a message; only its time or the operator ends it.
- A timed message that reaches the car more than 10 seconds late is skipped. An "until cleared" message is shown
  however late it arrives.
- The car sends nothing back: no acknowledgement, no read receipt. After the car application restarts its display is
  empty and earlier messages are not restored.

## What Telemetry needs

Routes, validation, and UI stay with their owner, [Telemetry](../component-telemetry.md). Required behaviour:

| Area      | Today (Driver Radio)                     | Needed                                                                                                    |
|-----------|------------------------------------------|-----------------------------------------------------------------------------------------------------------|
| Types     | `MSG`, `BOX`, `FLAG`, `PACE`             | The nine types above.                                                                                     |
| Request   | `{key, text}`                            | Show: type, optional or required `text`, optional `duration_s`. Clear one type. Clear all.                |
| Producer  | value is the text                        | value is the JSON object above; key is the type or `ALL`.                                                 |
| State     | history of sent messages                 | Also the active set: one entry per type that was shown, not cleared, and whose time has not run out.      |
| Flags UI  | preset texts under `FLAG`                | One latching button per flag type: a click shows the flag, a second click clears it.                      |
| Time UI   | none                                     | 5 s (default), 10 s, 30 s, until cleared, or a custom value from 1 to 3600 s. Applies to the next send.   |
| Active UI | none                                     | List of active messages, oldest first, each with a clear button, plus one "clear all" button.             |

"Active" in the panel is the operator's own view, computed from what was sent. Every signed-in operator sees the same
active set and may clear a message sent by another operator.

## Open points

- The 10 second lateness rule compares the record timestamp with the car's clock, which can be wrong after a start
  without network. The car side decides how to measure it.
- A restarted car shows nothing until the operator sends again. Re-sending the active set periodically from Telemetry
  would close this gap; not in the first version.
- 200 characters do not fit the 1280x400 car display at a readable size. A lower limit or a truncation rule is open.
- Relay operator events accept any key, so an operator could send a reserved key by hand.
