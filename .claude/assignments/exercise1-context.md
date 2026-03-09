# Exercise 1 — Distributed Router & Memory Bot (Context Summary)

## Stack
- Runtime: Bun (TypeScript)
- Messaging: Kafka via KafkaJS (`localhost:9092`)
- Infrastructure: Docker Compose (`bitnami/kafka:3.5`, KRaft mode)
- Root: `exercise1/`

## Project Structure
```
exercise1/
├── package.json / tsconfig.json
├── shared/
│   ├── topics.ts               # TOPICS constants
│   ├── types/conversation.ts   # ConversationMessage, ConversationHistory
│   ├── types/events.ts         # All Kafka event interfaces
│   └── kafka/client.ts         # Singleton Kafka, createProducer/Consumer, sendMessage, subscribeAndRun, registerShutdown
└── services/
    ├── userInterface/          # Console I/O
    ├── memoryService/          # Persist history.json
    ├── routerService/          # Intent classification + routing
    ├── mathApp/                # Math evaluator
    ├── weatherApp/             # Mock weather
    ├── exchangeApp/            # Static currency rates
    ├── generalChatApp/         # Rule-based chat fallback
    └── responseAggregator/     # Formats final bot response
```

## Kafka Topics (TOPICS constant → topic name)
| Constant          | Topic name                    |
|-------------------|-------------------------------|
| USER_INPUT        | user-input-events             |
| USER_CONTROL      | user-control-events           |
| INTENT_MATH       | intent-math                   |
| INTENT_WEATHER    | intent-weather                |
| INTENT_EXCHANGE   | intent-exchange               |
| INTENT_CHAT       | intent-general-chat           |
| APP_RESULTS       | app-results                   |
| BOT_RESPONSES     | bot-responses                 |
| HISTORY_UPDATE    | conversation-history-update   |

## Event Types (shared/types/events.ts)
- `UserInputEvent`              — userId, userInput, timestamp
- `UserControlEvent`            — userId, command ("reset"), timestamp
- `IntentMathEvent`             — userId, expression, timestamp
- `IntentWeatherEvent`          — userId, city, timestamp
- `IntentExchangeEvent`         — userId, currencyCode, targetCurrency, timestamp
- `IntentGeneralChatEvent`      — userId, userInput, context: ConversationHistory[], timestamp
- `AppResultEvent`              — userId, type, result, success, error?, timestamp
- `BotResponseEvent`            — userId, message, sourceType, timestamp
- `ConversationHistoryUpdateEvent` — userId, history: ConversationHistory[], timestamp

## Services: Consumes → Produces
| Service              | Group ID             | Consumes                                        | Produces                  |
|----------------------|----------------------|-------------------------------------------------|---------------------------|
| userInterface        | ui-service           | BOT_RESPONSES                                   | USER_INPUT, USER_CONTROL  |
| memoryService        | memory-service       | USER_INPUT, APP_RESULTS, USER_CONTROL           | HISTORY_UPDATE            |
| routerService        | router-service       | USER_INPUT, HISTORY_UPDATE                      | INTENT_*                  |
| mathApp              | math-service         | INTENT_MATH                                     | APP_RESULTS               |
| weatherApp           | weather-service      | INTENT_WEATHER                                  | APP_RESULTS               |
| exchangeApp          | exchange-service     | INTENT_EXCHANGE                                 | APP_RESULTS               |
| generalChatApp       | chat-service         | INTENT_CHAT                                     | APP_RESULTS               |
| responseAggregator   | response-aggregator  | APP_RESULTS                                     | BOT_RESPONSES             |

## Message Flow
```
[User types]
     ↓
userInterface  →  user-input-events
                        ↓
          memoryService + routerService (parallel)
                        ↓
    intent-math / intent-weather / intent-exchange / intent-general-chat
                        ↓
    mathApp / weatherApp / exchangeApp / generalChatApp
                        ↓
                   app-results
                        ↓
              responseAggregator
                        ↓
                  bot-responses
                        ↓
              userInterface (prints to console)

memoryService → conversation-history-update → routerService (history cache)
```

## Reset Flow
```
/reset → userInterface → user-control-events → memoryService → deletes history.json entry
```

## Key Implementation Details
- All Kafka message keys = `userId` (ensures per-user ordering)
- `memoryService` persists to `services/memoryService/history.json` (Bun File API)
- `routerService` keeps a local `historyCache` updated via HISTORY_UPDATE
- Intent classification uses regex (math > weather > currency > chat fallback)
- `generalChatApp` receives conversation context embedded in `IntentGeneralChatEvent`
- `AppResultEvent.success=false` → responseAggregator formats a user-facing error message

## How to Run
```bash
# 1. Start Kafka
docker-compose up -d

# 2. Install dependencies (once)
cd exercise1 && bun install

# 3. Start all services (each in a separate terminal)
bun services/memoryService/memoryService.ts
bun services/routerService/routerService.ts
bun services/mathApp/mathApp.ts
bun services/weatherApp/weatherApp.ts
bun services/exchangeApp/exchangeApp.ts
bun services/generalChatApp/generalChatApp.ts
bun services/responseAggregator/responseAggregator.ts
bun services/userInterface/userInterface.ts   # ← interact here

# Optional: run as a different user
USER_ID=alice bun services/userInterface/userInterface.ts
```
