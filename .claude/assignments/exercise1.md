

# Exercise 1 — Distributed Router & Memory Bot

## Goal

Upgrade the basic chatbot into a **distributed intelligent agent** built using **Kafka-based microservices**.

The purpose of this exercise is to understand the architecture of complex LLM systems using a distributed approach, focusing on:

- Intent recognition
- Logical routing
- Working with structured data
- Conversation state management
- Kafka as the communication backbone

The system should simulate a modern LLM orchestration architecture.

---

# Technologies

The implementation must use:

- Bun (JavaScript / TypeScript runtime)
- TypeScript
- Kafka
- Kafka client library (`kafkajs` or `node-rdkafka`)
- Docker Compose

---

# System Architecture

The monolithic chatbot must be split into **microservices** that communicate through Kafka topics.

Services in the system:

1. `userInterface.ts`
2. `memoryService.ts`
3. `routerService.ts`
4. `mathApp.ts`
5. `weatherApp.ts`
6. `exchangeApp.ts`
7. `generalChatApp.ts`
8. `responseAggregator.ts`

Optional utility:

9. `resetService` (logic may also be handled in memory service)

---

# Kafka Topics

The following topics are required for communication between services:

## Core Topics

```
user-input-events
router-intents
app-results
bot-responses
user-control-events
```

## Optional Topic

```
conversation-history
```

This topic may be used to distribute updated conversation history.

---

# Microservices Specification

## 1. userInterface.ts

Role: Entry and exit point of the system.

Consumes:

```
bot-responses
```

Produces:

```
user-input-events
```

Responsibilities:

- Accept user input from console
- Send input to Kafka
- Display bot responses
- Detect `/reset` command and send control event

---

## 2. memoryService.ts

Role: Persistent conversation memory manager.

Consumes:

```
user-input-events
app-results
user-control-events
```

Produces:

```
conversation-history-request
conversation-history-update
```

Responsibilities:

- Maintain conversation history per user
- Persist history to `history.json`
- Load history on startup
- Update history after each interaction
- Delete history on `/reset`

Implementation requirements:

- Use Bun File API
- Maintain a map: `userId -> ConversationHistory[]`

---

## 3. routerService.ts

Role: Intent recognition and request routing.

Consumes:

```
user-input-events
conversation-history-update
```

Produces:

```
intent-math
intent-weather
intent-exchange
intent-general-chat
```

Responsibilities:

- Detect the user's intent
- Extract parameters from input
- Route request to the appropriate application

Intent recognition may be implemented using:

- LLM classification prompt
- Simple regex rules (acceptable for this exercise)

Special case:

`/reset` must be forwarded to `user-control-events`.

---

## 4. mathApp.ts

Consumes:

```
intent-math
```

Produces:

```
app-results
```

Responsibilities:

- Evaluate mathematical expressions

Example:

```
"150 + 20"
```

Output example:

```
{ type: "math", result: "170" }
```

---

## 5. weatherApp.ts

Consumes:

```
intent-weather
```

Produces:

```
app-results
```

Responsibilities:

- Retrieve weather information for a city

Possible implementation:

- External API such as OpenWeatherMap

---

## 6. exchangeApp.ts

Consumes:

```
intent-exchange
```

Produces:

```
app-results
```

Responsibilities:

- Return currency exchange rate

Simplified implementation:

- Static mapping of currency values

---

## 7. generalChatApp.ts

Consumes:

```
intent-general-chat
```

Produces:

```
app-results
```

Responsibilities:

- Handle general conversation using an LLM
- Combine:

  - user input
  - conversation history

Input example:

```
{
  context: ConversationHistory[],
  userInput: string
}
```

---

## 8. responseAggregator.ts

Consumes:

```
app-results
```

Produces:

```
bot-responses
```

Responsibilities:

- Build the final message returned to the user
- Route response back to the UI

---

# Memory Persistence

Conversation history must be stored in:

```
history.json
```

Behavior:

- Load history at service startup
- Persist after each interaction
- Survive service restarts

---

# Reset Command

The system must support the command:

```
/reset
```

Flow:

```
UserInterface
   ↓
user-control-events
   ↓
MemoryService
   ↓
delete conversation history
```

---

# Infrastructure

Kafka must run using Docker Compose.

Example image:

```
bitnami/kafka:3.5
```

Start infrastructure with:

```
docker-compose up -d
```

---

# Running Services

Each microservice runs independently:

```
bun userInterface.ts
bun memoryService.ts
bun routerService.ts
bun mathApp.ts
bun weatherApp.ts
bun exchangeApp.ts
bun generalChatApp.ts
bun responseAggregator.ts
```

---

# Deliverables

The submission repository must include:

## Infrastructure

```
docker-compose.yml
```

## Microservices

```
userInterface.ts
memoryService.ts
routerService.ts
mathApp.ts
weatherApp.ts
exchangeApp.ts
generalChatApp.ts
responseAggregator.ts
```

## Execution Log

Provide proof of execution showing:

1. First run (no history detected)
2. Weather query
3. Currency exchange query
4. Math calculation
5. General LLM question
6. Restart services and continue conversation
7. Reset conversation using `/reset`

## README.md

Include:

- Full name
- Student ID
- Description of each microservice
- Kafka topics used
- Architecture diagram
- Challenges encountered and solutions