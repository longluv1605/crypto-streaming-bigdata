# Crypto Pipeline Project

This project is a Dockerized pipeline for cryptocurrency price prediction. It consists of:

- **Ingestion Layer**: Kafka-based ingestion of raw data.
- **Batch Processing Layer**: Spark jobs for cleaning and aggregating data.
- **Streaming Layer**: Real-time streaming analysis.

## Setup

1. Install Docker and Docker Compose.
2. Build and start the containers:

   ```bash
   docker-compose up --build
