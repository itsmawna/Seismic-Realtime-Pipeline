# Seismic Real-Time Pipeline

This project implements a real-time seismic pipeline, from fetching data via a WebSocket stream, ingesting it into Kafka, processing it with Spark Structured Streaming, and visualizing it with an interactive Dash/Plotly dashboard.

---

## Features

### Seismic Producer
- Connects to the seismic WebSocket stream: `wss://www.seismicportal.eu/standing_order/websocket`
- Filters and transforms data
- Publishes to Kafka topic `RawSeismicData`

### Spark Streaming Processor
- Reads continuously from the Kafka topic
- Filters events with magnitude ≥ 2.0
- Prints unique events to console
- Optionally counts the total number of processed events

### Seismic Real-Time Dashboard
- Interactive dashboard built with Dash & Plotly
- World map of earthquakes
- Histogram of magnitudes
- Depth vs Magnitude graph
- Top 10 affected regions
- Automatic alerts for:
  - Magnitude ≥ 5.0
  - Shallow earthquakes (< 20 km)

---

## Requirements

```bash
pip install requirements.txt
```
Ports:
- Kafka: 9092
- Dash: 8050

---
## Installation and Execution

Before starting, make sure Kafka is installed and configured in `C:/Kafka`. Open **new terminals** for each process to run Kafka, Producer, Processor, and Dashboard simultaneously.
### Navigate to Kafka Directory
```bash
cd C:/Kafka
```

### 1. Prepare Kafka
```bash
# Format Kafka storage (KRaft)
.\bin\windows\kafka-storage.bat format -t <CLUSTER_ID> -c .\config\kraft\server.properties
```
(Replace <CLUSTER_ID> with a generated UUID )
```bash
# Start Kafka server
C:\Kafka> .\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
```
Keep this terminal open, Kafka must be running for the Producer and Processor !
### 2. Activate Python Environment
Open a new terminal in your project folder
```bash
venv\Scripts\activate
```

### 3. Run the Producer
```bash
python seismic_producer.py
```
- Connects to the WebSocket stream
- Publishes events to Kafka (RawSeismicData)
- Example log:

```bash
INFO:root:New event: Mag 4.1 - Region: NEGROS- CEBU REG, PHILIPPINES
```

### 4. Run the Spark Processor
```bash
python spark_streaming_processor.py
```
- Reads Kafka messages
- Displays filtered events (magnitude ≥ 2.0)
- Counts unique events
- Example output:
```bash
🆕 EVENT #1
======================================================================
  Magnitude    : 3.0
  Region       : OFFSHORE COQUIMBO, CHILE
  Time         : 2025-11-09T18:32:59.0Z
  Latitude     : -30.12
  Longitude    : -72.03
  Depth        : 48.7 km
  Mag Type     : ml
  Action       : update

```

### 5. Run the Dashboard
```bash
python seismic_realtime_dashboard.py
```
- Opens interactive dashboard at http://localhost:8050
- Updates every 5 seconds
- Shows real-time statistics and alerts

#### Notes : 
- Events are filtered to keep only magnitude ≥ 2.0
- Dashboard stores in memory only the last 100 events
- Kafka must be running before the Producer and Processor
- Spark Streaming reads from startingOffsets = "earliest" to capture historical data
---

## References
- [Seismic Portal WebSocket API](https://www.seismicportal.eu/realtime.html)
- [Kafka Python Documentation](https://kafka-python.readthedocs.io/en/master/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
