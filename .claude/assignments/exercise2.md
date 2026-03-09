

# Exercise 2 — LLM Prompt Engineering over Kafka Microservices

## Context

Exercise 1 implemented a distributed chatbot system using Kafka and multiple microservices.

Architecture from Exercise 1:

UserInterface  
→ RouterService  
→ Apps (mathApp / weatherApp / exchangeApp / generalChatApp)  
→ ResponseAggregator  
→ UserInterface

Each service communicates via Kafka topics using KafkaJS.

The system currently performs intent detection using simple regex rules in the router.

Exercise 2 extends this architecture by adding **LLM-powered routing and prompt engineering techniques**.

The goal is **NOT to redesign the architecture** but to **add an LLM layer on top of the existing Kafka system**.

---

# Architecture Extension

The new architecture should become:

UserInterface  
↓  
user_input_events  
↓  
GuardrailService  
↓  
LLMRouterService (Few-Shot Prompting)  
↓  
router_decision_events  
↓  
LLMExtractionService (Structured JSON output)  
↓  
llm_response_events  
↓  
JSONParserService  
↓  
function_execution_requests  
↓  
Apps (mathApp / weatherApp / exchangeApp / generalChatApp)  
↓  
bot_output_events  
↓  
ResponseAggregator  
↓  
UserInterface

---

# New Services

The following services must be implemented in Exercise 2.

### guardrailService

Consumes:

user_input_events

Detects unsafe inputs:

politics  
malware

If violation detected:

publish to

guardrail_violation_events

Return message:

"I cannot process this request due to safety protocols."

---

### llmRouterService

Consumes:

user_input_events

Uses **Few-Shot prompting** to classify user intent.

Supported intents:

getWeather  
calculateMath  
currencyExchange  
generalChat  

Produces:

router_decision_events

Event structure:

```
{
  userId: string
  input: string
  intent: string
  parameters: object
  confidence: number
  timestamp: string
}
```

---

### llmExtractionService

Consumes:

router_decision_events

Uses an LLM prompt that forces **structured JSON output**.

Expected format:

```
{
  intent: string
  parameters: object
  confidence: number
}
```

Produces:

llm_response_events

---

### jsonParserService

Consumes:

llm_response_events

Responsibilities:

Parse the JSON response safely.

If valid:

publish to

function_execution_requests

If invalid:

publish an error event.

---

### cotMathService

Consumes:

router_decision_events

If intent = calculateMath and the input is a **word problem**, use **Chain-of-Thought prompting**.

Example:

Input:

"If I have 5 apples and eat 2 then buy 10"

Output:

5 - 2 + 10

Produces:

cot_math_expression_events

The math expression will then be processed by the existing mathApp.

---

# Prompt Engineering Requirements

The system must demonstrate the following prompt engineering techniques:

### Few-Shot Prompting

Used in the router to classify intents.

Provide at least 3 examples per intent.

---

### Structured JSON Output

LLM responses must return strict JSON.

---

### Chain-of-Thought Reasoning

Used to convert natural language math problems into expressions.

---

### Persona Prompt

General chat responses should follow the persona:

"Cynical but helpful research assistant that explains things using data engineering metaphors."

---

# Kafka Topics

The following topics must exist:

user_input_events  
router_decision_events  
llm_prompt_requests  
llm_response_events  
function_execution_requests  
bot_output_events  
guardrail_violation_events  
cot_math_expression_events

---

# Project Structure

Create a new folder:

exercise2/

Structure:

exercise2/

prompts/  
prompts.ts

services/  
guardrailService.ts  
llmRouterService.ts  
llmExtractionService.ts  
jsonParserService.ts  
cotMathService.ts

kafka/  
kafka_producer.ts  
kafka_consumer.ts

---

docker-compose.yml  
topics.sh  
index.ts

---

# Technical Requirements

Language:

TypeScript

Runtime:

Bun

Kafka client:

KafkaJS

---

# Important Constraints

Do NOT redesign the system.

Do NOT remove Exercise 1 services.

Exercise 2 must **extend the existing Kafka architecture**.

Only add the new LLM and prompt-engineering layers.

---

# Goal

Demonstrate how LLM prompt engineering techniques can enhance a distributed event-driven system built with Kafka.
