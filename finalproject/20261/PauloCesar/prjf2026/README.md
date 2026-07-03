# prjf2026

## Overview
This folder follows the required delivery layout:

```text
prjf2026/
├── bin/
├── src/
├── misc/
├── datasample/
├── presentation/
└── README.md
```

This project implements a local streaming pipeline for synthetic ESP32 sensor data focused on wildfire-risk monitoring.
The pipeline generates sensor readings, publishes them to Kafka, processes them with Spark Structured Streaming, stores them in PostgreSQL, and visualizes the most recent sensor positions in a Streamlit web app.

## Pipeline Summary
1. `src/simulador_stress2.py` generates synthetic sensor messages with latitude, longitude, temperature, humidity, CO2, and edge-AI status.
2. Kafka receives the messages on topic `dados-sensores`.
3. `src/spark/spark_consumer.py` consumes the stream, computes `indice_risco`, and writes the processed records to PostgreSQL.
4. `src/app_streamlit_monitoramento.py` reads the latest stored data and displays the sensors on a map with risk colors.

## Architecture
```text
src/simulador_stress2.py
        |
        v
Kafka (topic: dados-sensores)
        |
        v
src/spark/spark_consumer.py
        |
        v
PostgreSQL (table: historico_sensores)
        |
        v
src/app_streamlit_monitoramento.py
```

## Folder Guide
- `bin/`: executable scripts used to bootstrap the local Python environment, start the platform, stop it, and observe the pipeline.
- `src/`: all source code used by the project.
- `misc/`: auxiliary files such as Docker Compose, dashboards, and Python requirements.
- `datasample/`: small sample payload for quick inspection.
- `presentation/`: presentation material in PDF format.

## Main Files
### `bin/`
- `bootstrap_python_env.sh`: creates `prjf2026/venv_incendios` locally and installs the Python dependencies from `misc/requirements.txt`.
- `iniciar_tudo.sh`: starts Docker services, Spark consumer, simulator, and Streamlit.
- `parar_tudo.sh`: stops local processes and shuts down the Docker services.
- `observar_pipeline.sh`: opens Terminal windows to observe Kafka, Spark, PostgreSQL, and simulator activity in real time.
- `startspark.sh`: starts only the Spark consumer.
- `startspark_v`: alternate copy of the Spark start script.

### `src/`
- `simulador_stress2.py`: generates synthetic sensor traffic.
- `app_streamlit_monitoramento.py`: displays the current sensor map and risk levels.
- `monitor_postgres_tempo_real.py`: monitors recent PostgreSQL inserts in real time.
- `spark/spark_consumer.py`: processes the Kafka stream and persists the results.

### `misc/`
- `docker/docker-compose.yml`: starts Kafka, Zookeeper, PostgreSQL, and Grafana.
- `requirements.txt`: Python dependencies used by the scripts.
- `dashboard.json` and `Dashcompleto.json`: legacy dashboard files.
- `comando.txt`: auxiliary command notes.

### `datasample/`
- `sample_sensor.json`: compact synthetic sample used for quick inspection.

### `presentation/`
- `Apresentação Trabalho Final.pdf`: current presentation file.

## Data
The project uses synthetic data only.
No external dataset download is required.
A small sample payload is provided in `datasample/sample_sensor.json`.

## Environment And Dependencies
The project is self-contained inside `prjf2026/` for scripts, source code, logs, Docker Compose files, and the local Python virtual environment.
On the first execution, the scripts automatically create `prjf2026/venv_incendios` and install the dependencies listed in `misc/requirements.txt`.

System requirements:
- Docker
- Docker Compose
- Python 3
- Java compatible with Spark

Main network ports:
- Kafka: `9092`
- PostgreSQL: `5432`
- Streamlit: `8501`
- Grafana: `3000`

## How To Run
### 1. Start the full platform
```bash
cd prjf2026
./bin/iniciar_tudo.sh
```

This command:
- starts the Docker services
- creates the local virtual environment if needed
- installs Python dependencies on first run
- starts the Spark consumer
- starts the simulator
- starts the Streamlit web application

### 2. Open the web application
After startup, access:

- [http://localhost:8501](http://localhost:8501)

### 3. Observe the pipeline in real time
```bash
cd prjf2026
./bin/observar_pipeline.sh
```

This script opens Terminal windows to observe:
- Kafka topic consumption
- Spark processing logs
- PostgreSQL inserts
- simulator activity

### 4. Stop everything
```bash
cd prjf2026
./bin/parar_tudo.sh
```

## PostgreSQL Schema
The project expects the following table:

```sql
CREATE TABLE IF NOT EXISTS historico_sensores (
    sensor_id TEXT,
    timestamp TIMESTAMPTZ,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    temperatura DOUBLE PRECISION,
    umidade DOUBLE PRECISION,
    co2 DOUBLE PRECISION,
    status_ia_borda TEXT,
    indice_risco DOUBLE PRECISION
);
```

The startup script already guarantees that this table exists before the pipeline begins to process data.

## Validation Status
The following execution flow was validated from inside `prjf2026/`:
- `./bin/iniciar_tudo.sh`
- Spark consuming Kafka and writing to PostgreSQL
- Streamlit running on port `8501`
- PostgreSQL receiving new records
- `./bin/parar_tudo.sh`

## Notes
- The official `README-report-template.md` was not available in the repository, so this README is a refined project-organization version.
- Grafana remains present in Docker Compose, but the main validated flow uses Kafka, Spark, PostgreSQL, and Streamlit.
- Full Docker-only reproducibility would still require packaging the Python runtime dependencies inside containers instead of creating a local virtual environment.
