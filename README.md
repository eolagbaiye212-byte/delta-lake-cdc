# CDC Data Pipeline — Medallion Pattern (Portfolio Project)

 A professional, end-to-end demonstration of a CDC-enabled data pipeline implementing the Medallion architecture (Bronze → Silver → Gold). Uses Debezium and Kafka for change-data-capture, PostgreSQL as the source, PySpark for transformations, and Delta-style storage for the data lake. Designed as a portfolio project to illustrate design, implementation, and operational considerations.

---

**Architecture Diagram (placeholder)**

![Architecture diagram placeholder](docs/architecture-diagram.png)

Replace the image above with a diagram showing the following components and connections:
- PostgreSQL (source)
- Debezium connector
- Kafka (topics)
- Consumers (Spark streaming / Kafka Connect sinks)
- Delta Lake storage (Bronze/Silver/Gold folders)
- Downstream analytics / BI tools

---

## Tech Stack

- PostgreSQL — transactional OLTP source
- Debezium — CDC connector for capturing row-level changes
- Apache Kafka — durable change stream
- PySpark (Spark Structured Streaming / Batch) — transformations
- Delta Lake (folder-based Delta / _delta_log) — data lake storage format
- Docker & Docker Compose — local environment and connector orchestration

---

## Features

- End-to-end CDC demonstration from PostgreSQL into a Delta-style data lake
- Medallion architecture with Bronze (raw), Silver (cleaned/enriched), and Gold (business-ready) layers
- Synthetic data generation utilities for repeatable demos
- Example Debezium Docker Compose configuration for local CDC simulation
- Clear project structure and runnable scripts for quick portfolio demos

---

## Prerequisites

- Windows, macOS, or Linux with sufficient CPU/RAM to run Docker and Spark
- Python 3.10+ (project includes a `myenv` venv for convenience)
- Docker & Docker Compose
- Java runtime for Spark (if running locally)
- Optionally Apache Spark installed locally for heavier workloads

---

## Installation

1. Clone the repository:

```powershell
git clone <repo-url>
cd "Data Lake with CDC Project"
```

2. Create and activate a Python virtual environment (recommended):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt  # optional: add this file to pin deps
```

3. (Optional) Use the included helper virtualenv: activate `myenv\Activate.ps1` in PowerShell.

4. Start local infrastructure (Debezium + Kafka) for CDC testing (optional):

```powershell
cd docker_debezium_config
docker compose up -d
```

Wait for Kafka and Debezium to be healthy before continuing (check logs with `docker compose logs -f`).

---

## Configuration

Most configuration values are stored in the `docker_debezium_config/debezium_config.json` and environment settings in `docker-compose.yaml` for local testing. For the pipeline and Spark jobs, configuration values are read from simple constants or environment variables inside the `spark_jobs/` modules and `medallion_main.py`.

Key configuration items to check before running:
- PostgreSQL connection string (host, port, user, password, database)
- Kafka bootstrap servers
- Topics and Debezium connector settings
- Local path to `delta_lake/` where Bronze/Silver/Gold folders will be written

If you prefer, add a `.env` file and update the scripts to load these values using `python-dotenv`.

---

## Project Structure

- `medallion_main.py` — pipeline orchestrator and example job runner
- `data_generation/` — synthetic data generator and driver (`generate_data_main.py`, `generate_data.py`)
- `spark_jobs/` — modules for `bronze_layer.py`, `silver_layer.py`, `gold_layer.py`
- `delta_lake/` — example Delta-style folders (`bronze/`, `silver/`, `gold/`)
- `docker_debezium_config/` — Debezium connector config and `docker-compose.yaml`
- `create_schema.sql` — example DDL for PostgreSQL source tables
- `log_progress.py` — small helper for logging progress

---

## Data Flow Explanation

1. Source writes (PostgreSQL) are captured by Debezium and published to Kafka topics.
2. A consumer (Spark Structured Streaming or a Kafka Connect sink) ingests the change events into the Bronze layer as raw JSON/Parquet files, retaining the original CDC metadata (operation type, timestamp, offsets).
3. Silver layer jobs read Bronze raw data, perform transformations such as schema normalization, deduplication, and type casting, and write cleaned Parquet/Delta files.
4. Gold layer jobs produce business-ready tables (facts and dimensions) optimized for analytics and BI, often shaped into star schemas.

The project includes sample scripts in `spark_jobs/` to demonstrate batch-style transformations; you can extend them to streaming for near-real-time processing.

---

## Medallion Architecture (Bronze / Silver / Gold)

- Bronze: Raw, append-only data landing zone. Keep original CDC payloads and metadata. Use partitioning appropriate for ingestion pattern (e.g., by date).
- Silver: Cleansed and deduplicated records with a curated schema. Apply business logic and early enrichment here.
- Gold: Business-level consumption layer. Star schemas, fact tables, and dimensional models reside here for reporting and analytics.

This separation improves reliability, auditability, and enables multiple downstream consumers to use the same raw input for different purposes.

---

## Star Schema Explanation

The Gold layer is typically modeled as a star schema to support fast, slice-and-dice analytics:

- Dimensions: `dim_customer`, `dim_product`, `dim_date`, `dim_location` — contain descriptive attributes and surrogate keys.
- Facts: `fact_sales`, `fact_inventory_snapshot`, `fact_order_status` — record measurable events and foreign keys pointing to dimensions.

Benefits:
- Simpler queries for business users
- Efficient joins against smaller, denormalized dimension tables
- Better OLAP performance when materialized or cached in BI tools

---

## Usage — Running the Pipeline

1. (Optional) Start Debezium and Kafka for CDC:

```powershell
cd docker_debezium_config
docker compose up -d
```

2. Start the synthetic data generator (local demo):

```powershell
python data_generation\generate_data_main.py
```

3. Run the medallion pipeline (example):

```powershell
python medallion_main.py
```

Notes:
- `medallion_main.py` contains orchestration steps that exercise `spark_jobs/`. Adjust file paths and Spark options to match your environment.
- To run Spark locally, ensure Java and Spark are available and configured in the activated Python environment.

---

## Troubleshooting

- Debezium/Kafka not starting: check Docker logs with `docker compose logs -f` inside `docker_debezium_config` and ensure ports are not already in use.
- Spark job fails with classpath or Java errors: verify Java (JDK/JRE) is installed and `SPARK_HOME` is set if using a local Spark install.
- Permission errors writing to `delta_lake/`: ensure the running user has write permissions to the target folders.
- Wrong schema or missing CDC fields: confirm Debezium connector configuration and PostgreSQL replication slot permissions.

If you need help debugging, run the failing command with verbose logs and share the last 50–100 lines of output.

---

## Future Improvements (ideas for portfolio)

- Add `requirements.txt` and automated environment setup scripts
- Convert batch Spark jobs to Structured Streaming for near-real-time processing
- Add automated tests for data generation and transformation logic
- Add CI workflow to run linting and unit tests
- Add Terraform or Docker Compose orchestration to deploy the entire stack reproducibly
- Add example notebooks or Looker/Power BI dashboards showing sample analyses

---

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/awesome-feature`)
3. Commit changes with descriptive messages
4. Open a pull request describing the change

Be sure to include tests for any new transformation logic and update the README when adding features.

---

## License

This project is released under the MIT License. See `LICENSE` for details.

---

## Contact

Project author: Your Name — set your professional contact info here.

- Email: eolagbaiye212@gmail.com
- LinkedIn: https://www.linkedin.com/in/emmanuel-olagbaiye
- GitHub: https://github.com/eolagbaiye212-byte

---