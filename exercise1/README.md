# Exercise 1 — Microservices Chat System

## Overview

Exercise 1 implements a distributed chatbot where every component is a separate Kafka microservice. No service calls another directly — all communication happens exclusively through Kafka topics.

The system handles four types of user requests:

- **Weather queries** — mock weather data for major cities
- **Currency exchange** — static exchange rates between common currencies
- **Math calculations** — safe arithmetic expression evaluator
- **General chat** — rule-based responses with conversation history

Conversation history is persisted to disk (`services/memoryService/history.json`) and shared across services via a dedicated Kafka topic. The `/reset` command clears a user's history.

---

## Architecture

```
[User types in terminal]
         │
         ▼
   UserInterface
         │
  user-input-events
         │
         ├──────────────────────────┐
         ▼                          ▼
  MemoryService               RouterService
  (persists history)          (classifies intent)
         │                          │
  conversation-history-update       │
         │                ┌─────────┼────────────┬────────────────┐
         └──▶ RouterService         ▼             ▼                ▼                ▼
                          intent-math  intent-weather  intent-exchange  intent-general-chat
                               │             │                │                  │
                            MathApp    WeatherApp      ExchangeApp       GeneralChatApp
                               │             │                │                  │
                               └─────────────┴────────────────┴──────────────────┘
                                                      │
                                                 app-results
                                                      │
                                            ResponseAggregator
                                                      │
                                               bot-responses
                                                      │
                                            UserInterface (stdout)
```

---

## Components

### UserInterface
**File:** `services/userInterface/userInterface.ts`

Reads user input from stdin. Publishes a `UserInputEvent` to `user-input-events` for every message, and a `UserControlEvent` to `user-control-events` when the user types `/reset`. Subscribes to `bot-responses` and prints the bot reply to stdout. The active user ID is set via the `USER_ID` environment variable (default: `user-1`).

---

### MemoryService
**File:** `services/memoryService/memoryService.ts`

Maintains conversation history for all users. Persists history to `services/memoryService/history.json`. Listens on three topics simultaneously — `user-input-events`, `app-results`, and `user-control-events` — and publishes a `ConversationHistoryUpdateEvent` after every change so RouterService keeps its local cache current.

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

---

### MathApp
**File:** `services/mathApp/mathApp.ts`

Evaluates arithmetic expressions using a safe recursive descent parser. Accepts only digits and the operators `+ - * / ( )`. No `eval()` is used. Supports operator precedence and parentheses.

---

### WeatherApp
**File:** `services/weatherApp/weatherApp.ts`

Returns mock weather data for a fixed set of cities including Tel Aviv, Jerusalem, London, Paris, Berlin, Tokyo, and others. Returns a default response for unrecognised cities.

---

### ExchangeApp
**File:** `services/exchangeApp/exchangeApp.ts`

Returns exchange rates using a static table anchored to ILS. Supported currencies: USD, EUR, GBP, CHF, JPY, CAD, AUD, ILS. Calculates cross-rates by converting through ILS.

---

### GeneralChatApp
**File:** `services/generalChatApp/generalChatApp.ts`

Produces rule-based responses matched by keyword regex. Covers greetings, Kafka questions, AI topics, and general conversation. Falls back to rotating responses seeded by conversation history length.

---

### ResponseAggregator
**File:** `services/responseAggregator/responseAggregator.ts`

Consumes every `AppResultEvent` from `app-results`, formats a final message, and publishes a `BotResponseEvent` to `bot-responses` for the UserInterface to display.

---

## Topics

| Topic | Producer | Consumer(s) | Description |
|---|---|---|---|
| `user-input-events` | UserInterface | MemoryService, RouterService | Inbound user messages |
| `user-control-events` | UserInterface | MemoryService | Control commands (`/reset`) |
| `intent-math` | RouterService | MathApp | Math evaluation requests |
| `intent-weather` | RouterService | WeatherApp | Weather query requests |
| `intent-exchange` | RouterService | ExchangeApp | Currency exchange requests |
| `intent-general-chat` | RouterService | GeneralChatApp | General chat requests |
| `app-results` | All apps | ResponseAggregator, MemoryService | Completed app responses |
| `bot-responses` | ResponseAggregator | UserInterface | Final formatted reply |
| `conversation-history-update` | MemoryService | RouterService | History sync events |

All messages use `userId` as the Kafka key to preserve per-user ordering within each partition.

---

## Running the Exercise

### 1. Start Kafka

Exercise 1 requires a Kafka broker on `localhost:9092`. Use the Exercise 2 docker-compose if you do not have one running:

```bash
cd ../exercise2
docker compose up -d
bash topics.sh
cd ../exercise1
```

Or start any Kafka instance on port 9092 and create the nine topics listed above.

### 2. Install dependencies

```bash
cd exercise1
bun install
```

### 3. Start services

Open a separate terminal for each service:

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

## Example Output

```
> What is the weather in London?
Bot [weather]: Weather in London is 12°C and rainy.

> USD to ILS
Bot [exchange]: 1 USD = 3.7 ILS

> 42 * 7
Bot [math]: 294

> (100 + 50) / 3
Bot [math]: 50

> Hello
Bot [chat]: Hello! How can I help you today?

> /reset
[ui] Reset command sent.
```

---

## Submission Artifacts

The following items are required for submission:

- `docker-compose.yml`
- All TypeScript service files
- Execution log showing the pipeline handling at least one message of each type
- Architecture diagram
- Written explanation of challenges encountered during implementation
