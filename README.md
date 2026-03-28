# 🐋 Real-Time Crypto Whale Tracker (Streaming Pipeline)

## 📌 Project Overview

A real-time **Data Engineering pipeline** that ingests live cryptocurrency trade data from Binance WebSocket, filters for large "whale" transactions, and aggregates them using sliding window analysis — enabling instant visibility into high-value market activity.

**Business Goal:** Detect and monitor large crypto trades (> $10,000) across BTC/USDT and ETH/USDT pairs in real time, providing aggregated volume insights via a live Grafana dashboard.

## 🏗️ Architecture & Tech Stack

```
┌──────────────────────────────────────────────────────────────────┐
│              Binance WebSocket API (Live Trade Stream)           │
└──────────────────────────────┬───────────────────────────────────┘
                               │
          ┌────────────────────▼────────────────────┐
          │              Producer (Python)          │
          │   Filters whale trades > $10,000        │
          │   Publishes raw events → Kafka topic    │
          └────────────────────┬────────────────────┘
                               │ kafka:9092
          ┌────────────────────▼────────────────────┐
          │         Apache Kafka (KRaft mode)       │
          │         Topic: crypto_trades            │
          └────────────────────┬────────────────────┘
                               │
          ┌────────────────────▼────────────────────┐
          │        Consumer (Spark Structured       │
          │              Streaming)                 │
          │  Parse JSON · Compute sliding window    │
          │  Write raw_trade + sliding_wd_trade     │
          └────────────────────┬────────────────────┘
                               │ JDBC
          ┌────────────────────▼────────────────────┐
          │           PostgreSQL (Docker)           │
          └────────────────────┬────────────────────┘
                               │
          ┌────────────────────▼────────────────────┐
          │                 Grafana                 │
          │       Live dashboards & monitoring      │
          └─────────────────────────────────────────┘
```

- **Message Broker:** ![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?style=flat&logo=apachekafka&logoColor=white)
- **Stream Processing:** ![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=flat&logo=apachespark&logoColor=white)
- **Data Warehouse:** ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat&logo=postgresql&logoColor=white)
- **Visualization:** ![Grafana](https://img.shields.io/badge/Grafana-latest-F46800?style=flat&logo=grafana&logoColor=white)
- **Containerization:** ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat&logo=docker&logoColor=white)

## 🗂️ Data Source

**Binance WebSocket Trade Stream:** [Binance API Docs](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Trade-Streams)

Live trade feed for `BTCUSDT` and `ETHUSDT` pairs via `wss://stream.binance.com:9443/stream`.

## 📁 Project Structure

```
crypto-whale-tracker/
│
├── scripts/
│   ├── producer/
│   │   └── api_listener.py        # WebSocket listener → Kafka producer
│   └── consumer/
│       └── spark_consumer.py      # Spark Structured Streaming consumer
│
├── database/
│   └── init.sql                   # PostgreSQL table definitions
│
├── dashboard/
│   └── datasource.yml             # Grafana datasource provisioning
│
├── dockerfile                     # Python + Java image for producer/consumer
├── docker-compose.yml             # Full stack orchestration
├── requirements.txt
├── .env.example                   # Environment variable template
└── README.md
```

## ⚙️ Pipeline Workflow

### 1. Producer — WebSocket Ingestion

- Connects to Binance WebSocket stream for `BTCUSDT` and `ETHUSDT`
- Filters trades with `total_value > $10,000` (whale threshold)
- Publishes raw trade events to Kafka topic `crypto_trades`
- Auto-reconnects to Kafka on startup until broker is ready

### 2. Kafka — Message Broker

- Runs in **KRaft mode** (no ZooKeeper dependency)
- Single-broker setup, auto-creates topic on first publish
- Decouples producer and consumer for independent scaling

### 3. Consumer — Spark Structured Streaming

- Reads continuously from Kafka topic in real time
- Parses binary Kafka messages and casts to typed schema
- Writes two output streams to PostgreSQL:
  - `raw_trade` — every individual whale trade event
  - `sliding_wd_trade` — 1-minute volume aggregated per symbol/trade type, sliding every 10 seconds

### 4. PostgreSQL — Storage

- Tables auto-created on container startup via `init.sql`
- Receives micro-batch writes from Spark via JDBC

### 5. Grafana — Visualization

- Datasource provisioned automatically via `datasource.yml`
- Connects to PostgreSQL to display live trade volume charts

## 🚀 Key Engineering Highlights

| Feature               | Details                                                                                |
| --------------------- | -------------------------------------------------------------------------------------- |
| **KRaft Kafka**       | No ZooKeeper — leaner setup, modern Kafka deployment                                   |
| **Sliding Window**    | 1-minute aggregation sliding every 10s for smooth real-time volume curves              |
| **Auto-provisioning** | Grafana datasource and PostgreSQL schema bootstrapped automatically via Docker volumes |
| **Fault Tolerance**   | Spark checkpoint locations preserve stream state across restarts                       |
| **Whale Filtering**   | Trades < $10,000 dropped at ingest — reduces downstream load significantly             |
| **Containerized**     | All services (Kafka, Spark, Postgres, Grafana) run via a single `docker-compose up`    |

## 🛠️ How to Run

### Prerequisites

- Docker & Docker Compose

No local Python or Java install required — everything runs inside Docker.

### 1. Clone the Repository

```bash
git clone https://github.com/hovngnvm/crypto-streaming-pipeline.git
cd crypto-streaming-pipeline
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Then edit .env with your credentials
```

`.env.example`:

```env
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_DB=your_db_name
POSTGRES_PORT=5433

GRAFANA_USER=your_username
GRAFANA_PASSWORD=your_password
```

### 3. Start the Full Stack

```bash
docker-compose up -d
```

This will start all services in order:

- **Kafka** broker (KRaft mode)
- **PostgreSQL** with auto-initialized schema
- **Grafana** with pre-configured datasource
- **Producer** connecting to Binance WebSocket
- **Consumer** running Spark Structured Streaming job

### 4. View Live Dashboard

Open Grafana at `http://localhost:3000` and log in with your credentials in `.env`.

The PostgreSQL datasource is pre-connected. Create panels querying `raw_trade` and `sliding_wd_trade` to visualize live whale activity.

### 5. Monitor Logs

```bash
# Watch the producer filtering and publishing trades
docker logs -f producer

# Watch Spark consuming and writing to Postgres
docker logs -f consumer
```

## 📊 Database Schema

### `raw_trade`

Stores every individual whale trade event as received from Binance.

| Column        | Type             | Description                 |
| ------------- | ---------------- | --------------------------- |
| `symbol`      | VARCHAR(20)      | Trading pair (e.g. BTCUSDT) |
| `trade_type`  | VARCHAR(10)      | BUY or SELL                 |
| `price`       | DOUBLE PRECISION | Trade price in USD          |
| `quantity`    | DOUBLE PRECISION | Asset quantity              |
| `total_value` | DOUBLE PRECISION | price × quantity            |
| `timestamp`   | TIMESTAMP        | Event time from Binance     |
| `inserted_at` | TIMESTAMP        | Row insertion time          |

### `sliding_wd_trade`

Stores 1-minute sliding window aggregations, updated every 10 seconds.

| Column         | Type             | Description                      |
| -------------- | ---------------- | -------------------------------- |
| `window_start` | TIMESTAMP        | Window start time                |
| `window_end`   | TIMESTAMP        | Window end time                  |
| `symbol`       | VARCHAR(20)      | Trading pair                     |
| `trade_type`   | VARCHAR(10)      | BUY or SELL                      |
| `total_volume` | DOUBLE PRECISION | Aggregated trade value in window |
| `inserted_at`  | TIMESTAMP        | Row insertion time               |

## 📄 License

This project is licensed under the MIT License.
