# Project Specification: SeriesSwarm (Event-Driven Architecture)

## 1. Executive Summary
**SeriesSwarm** is an event-driven, multi-agent backend system designed for the Series Hackathon. It acts as an "Invisible Operating System" inside iMessage.

Instead of a monolithic chatbot, it uses a **Kafka-based architecture** where a central Router consumes inbound messages, classifies intent, and dispatches tasks to specialized "Worker Agents." These agents generate native mobile assets (PDF Tickets, vCards, Calendar Invites, Audio) and produce them back to the outbound Kafka stream.

---

## 2. Technical Stack
* **Core Backbone:** Apache Kafka (via `confluent-kafka` Python client)
* **Language:** Python 3.9+
* **Intelligence:** OpenAI API (GPT-4o-mini for routing, TTS-1 for audio)
* **Asset Generation:** `fpdf`, `icalendar`, `vobject`, `io`

---

## 3. Architecture & Data Flow


[Image of event driven architecture diagram]


1.  **Ingestion:** The **Series API** produces a message to the `hackathon-inbound` topic.
2.  **Consumption:** Our **Main Router** consumes this message.
3.  **Processing:**
    * Router checks intent via LLM.
    * Router calls the specific Agent Function (e.g., `agents.bouncer`).
    * Agent returns a file object (Bytes).
4.  **Production:** The Router (acting as Producer) publishes the result to the `hackathon-outbound` topic (or calls the API directly if the hackathon rules specify API endpoints for replies).

---

## 4. Directory Structure
```text
series-swarm/
├── agents/
│   ├── __init__.py
│   ├── bouncer.py      # Agent A: PDF Ticket Generator
│   ├── secretary.py    # Agent B: Calendar Invite Generator
│   ├── connector.py    # Agent C: vCard Contact Generator
│   └── anchor.py       # Agent D: Audio Briefing Generator
├── core/
│   ├── kafka_consumer.py # Wrapper for confluent-kafka Consumer
│   ├── kafka_producer.py # Wrapper for confluent-kafka Producer
│   └── router.py         # LLM Classification Logic
├── main.py             # The Event Loop (Consumer -> Agent -> Producer)
├── requirements.txt    # Dependencies
└── .env                # KAFKA_BROKER, KAFKA_TOPIC_IN, KAFKA_TOPIC_OUT, OPENAI_KEY