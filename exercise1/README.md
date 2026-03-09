# Exercise 1 — Distributed Router & Memory Bot

## Overview

Exercise 1 implements a distributed chatbot in which every component is a separate Kafka microservice. No service calls another service directly — all communication happens exclusively through Kafka topics.

The system supports four types of user requests:

- **Weather queries** — mock weather data for major cities
- **Currency exchange** — static exchange rates between common currencies
- **Math calculations** — safe arithmetic expression evaluator
- **General chat** — rule-based conversation with fallback responses

Conversation history is persisted to disk (`history.json`) and shared across services via a dedicated Kafka topic. The `/reset` command clears a user's history.

---

## Architecture

```
[User types in terminal]
         │
         ▼
   UserInterface
         │
  user-input-events ◀── also: user-control-events (/reset)
         │
         ├──────────────────────────┐
         ▼                          ▼
  MemoryService               RouterService
  (persists history)          (classifies intent)
         │                          │
  conversation-history-update       │
         │                ┌─────────┼──────────┬────────────┐
         └──▶ RouterService         ▼           ▼            ▼            ▼
                          intent-math  intent-weather  intent-exchange  intent-general-chat
                               │           │                │                 │
                            MathApp    WeatherApp      ExchangeApp     GeneralChatApp
                               │           │                │                 │
                               └───────────┴────────────────┴─────────────────┘
                                                    │
                                               app-results
                                                    │
                                          ResponseAggregator
                                                    │
                                             bot-responses
                                                    │
                                           UserInterface (prints reply)
```

---

## Services

### UserInterface
**File:** `services/userInterface/userInterface.ts`

Reads user input from stdin. Publishes a `UserInputEvent` to `user-input-events` for every message, and a `UserControlEvent` to `user-control-events` when the user types `/reset`. Subscribes to `bot-responses` and prints the bot reply to stdout.

The active user ID is set via the `USER_ID` environment variable (defaults to `user-1`).

---

### MemoryService
**File:** `services/memoryService/memoryService.ts`

Maintains conversation history for all users. Persists history to `services/memoryService/history.json` using the Bun File API. Listens on three topics simultaneously:

- `user-input-events` → appends user message to history
- `app-results` → appends successful bot responses to history
- `user-control-events` → deletes history on `/reset`

After every change, publishes a `ConversationHistoryUpdateEvent` to `conversation-history-update` so the RouterService can keep its local cache current.

---

### RouterService
**File:** `services/routerService/routerService.ts`

Classifies user intent using regex rules applied in priority order:

| Priority | Intent | Signal |
|---|---|---|
| 1 | Math | Digit–operator–digit pattern |
| 2 | Weather | Keywords: weather, temperature, forecast, rain, sunny, hot, cold |
| 3 | Currency exchange | Currency codes: USD, EUR, ILS, GBP, JPY, CHF, CAD, AUD |
| 4 | General chat | Default fallback |

Maintains a local `historyCache` updated by `conversation-history-update` events, which is passed to the GeneralChatApp as conversation context.

---

### MathApp
**File:** `services/mathApp/mathApp.ts`

Evaluates arithmetic expressions using a safe recursive descent parser. Accepts only digits and the operators `+ - * / ( )`. No `eval()` is used. Supports operator precedence and parentheses.

Publishes an `AppResultEvent` with `success: true` and the numeric result, or `success: false` with an error message on invalid input.

---

### WeatherApp
**File:** `services/weatherApp/weatherApp.ts`

Returns mock weather data for a fixed set of cities. Supported cities include Tel Aviv, Jerusalem, Haifa, Eilat, New York, London, Paris, Berlin, Tokyo, and Dubai. Returns a default `20°C, clear` for any unrecognised city.

---

### ExchangeApp
**File:** `services/exchangeApp/exchangeApp.ts`

Returns exchange rates using a static table anchored to ILS. Supported currencies: USD, EUR, GBP, CHF, JPY, CAD, AUD, ILS. Calculates cross-rates by converting through ILS.

---

### GeneralChatApp
**File:** `services/generalChatApp/generalChatApp.ts`

Produces rule-based responses matched by keyword regex. Covers greetings, Kafka questions, AI topics, jokes, and general conversation. Falls back to one of five rotating responses using conversation history length as a seed.

---

### ResponseAggregator
**File:** `services/responseAggregator/responseAggregator.ts`

Consumes every `AppResultEvent` from `app-results` and formats a final message. On `success: false`, formats a user-facing error string. Publishes the result as a `BotResponseEvent` to `bot-responses`.

---

## Kafka Topics

| Topic | Constant | Producer | Consumers |
|---|---|---|---|
| `user-input-events` | `USER_INPUT` | UserInterface | MemoryService, RouterService |
| `user-control-events` | `USER_CONTROL` | UserInterface | MemoryService |
| `intent-math` | `INTENT_MATH` | RouterService | MathApp |
| `intent-weather` | `INTENT_WEATHER` | RouterService | WeatherApp |
| `intent-exchange` | `INTENT_EXCHANGE` | RouterService | ExchangeApp |
| `intent-general-chat` | `INTENT_CHAT` | RouterService | GeneralChatApp |
| `app-results` | `APP_RESULTS` | All apps | ResponseAggregator, MemoryService |
| `bot-responses` | `BOT_RESPONSES` | ResponseAggregator | UserInterface |
| `conversation-history-update` | `HISTORY_UPDATE` | MemoryService | RouterService |

All messages use `userId` as the Kafka message key to preserve per-user ordering within each partition.

---

## Event Flow

```
1. User types "weather in London"
2. UserInterface → user-input-events
3. MemoryService consumes event → saves to history.json → publishes conversation-history-update
4. RouterService consumes event → classifies as "weather" → publishes to intent-weather
5. WeatherApp consumes event → returns "Weather in London is 12°C and rainy." → publishes to app-results
6. ResponseAggregator consumes app-results → publishes to bot-responses
7. UserInterface consumes bot-responses → prints reply
```

---

## Running the System

### 1. Start Kafka

Exercise 1 assumes Kafka is already running locally on `localhost:9092`. The simplest way is to use the Exercise 2 docker-compose from the `exercise2/` directory:

```bash
cd ../exercise2
docker compose up -d
bash topics.sh
cd ../exercise1
```

Alternatively, start any Kafka instance on port 9092 and create the nine topics listed above manually.

### 2. Install dependencies

```bash
cd exercise1
bun install
```

### 3. Start all services

Open a separate terminal for each service and run:

```bash
# Terminal 1
bun services/memoryService/memoryService.ts

# Terminal 2
bun services/routerService/routerService.ts

# Terminal 3
bun services/mathApp/mathApp.ts

# Terminal 4
bun services/weatherApp/weatherApp.ts

# Terminal 5
bun services/exchangeApp/exchangeApp.ts

# Terminal 6
bun services/generalChatApp/generalChatApp.ts

# Terminal 7
bun services/responseAggregator/responseAggregator.ts

# Terminal 8 — interact here
bun services/userInterface/userInterface.ts
```

To run as a different user:
```bash
USER_ID=alice bun services/userInterface/userInterface.ts
```

---

## Example Interactions

All examples are typed into the UserInterface terminal (Terminal 8).

### Weather query
```
> What is the weather in London?

Bot [weather]: Weather in London is 12°C and rainy.
```

### Currency exchange query
```
> USD to ILS

Bot [exchange]: 1 USD = 3.7 ILS
```

### Math calculation
```
> 42 * 7

Bot [math]: 294
```

```
> (100 + 50) / 3

Bot [math]: 50
```

### General chat
```
> Hello

Bot [chat]: Hello! How can I help you today?
```

```
> Tell me a joke

Bot [chat]: Why do programmers prefer dark mode? Because light attracts bugs!
```

### Memory persistence

Conversation history is stored in `services/memoryService/history.json`. When the service restarts it loads the existing file and resumes where it left off. The GeneralChatApp receives the full history as context with each request.

### Reset command

```
> /reset

[ui] Reset command sent.
```

This deletes the current user's history from `history.json`. The next message starts a fresh conversation.
