# GCP E-Commerce Data Intelligence Pipeline

> End-to-end data engineering project built on Google Cloud Platform for a dropshipping operation — automating product ingestion, AI-powered categorization, and real-time price monitoring.

---

## Business Problem

Dropshipping operations deal with three recurring data challenges: **unreliable supplier files** that break downstream processes, **thousands of uncategorized products** that require manual tagging, and **price fluctuations** that need to be caught before they erode margins. This pipeline automates all three.

---

## Architecture

```mermaid
graph TD
    subgraph 01_Batch_Ingestion
        A[CSV Proveedor] --> B[Cloud Storage\n raw-products]
        B --> C{Cloud Function\nValidation}
        C -- Invalid --> D[GCS /failed]
        C -- Valid --> E[BigQuery\nstaging_products]
        E --> F[BigQuery\nfinal_products]
    end

    subgraph 02_AI_Enrichment
        F --> G[Python Script\n+ Gemini 1.5 Flash]
        G --> H[BigQuery\nenriched_products]
    end

    subgraph 03_Price_Monitoring
        I[Price Simulator] --> J[Pub/Sub Topic\nprice-updates]
        J --> K[Cloud Function\nIngest]
        K --> L[BigQuery\nTimeseries]
        L --> M[Looker Studio\nDashboard]
    end
```

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Storage | Google Cloud Storage (GCS) |
| Data Warehouse | BigQuery |
| Compute | Cloud Functions Gen2 |
| Messaging | Pub/Sub |
| AI / LLM | Gemini 1.5 Flash via `google-generativeai` |
| Orchestration | Eventarc (GCS trigger) |
| Visualization | Looker Studio |
| Language | Python 3.10+ |
| IaC | gcloud CLI (`infra/setup.sh`) |

---

## Project Structure

```
gcp-ecommerce-data-intelligence/
├── infra/
│   └── setup.sh                  # Infrastructure provisioning script
├── 01_batch_ingestion/
│   ├── main.py                   # Cloud Function — CSV validation & BQ load
│   └── requirements.txt
├── 02_ai_enrichment/
│   ├── enrich.py                 # Gemini enrichment script (async)
│   └── requirements.txt
├── 03_price_monitoring/
│   ├── simulator.py              # Pub/Sub price event publisher
│   ├── main.py                   # Cloud Function — event consumer
│   └── requirements.txt
├── data/
│   └── products_load.csv         # Sample supplier data (Resparked brand)
├── tests/
│   └── ingestion.py              # Unit tests for validation logic
└── README.md
```

---

## Module 01 — Batch Ingestion

Triggers automatically when a `.csv` file lands in the `raw-products` bucket.

**Validation logic (row-level):**
- `product_id` cannot be null or empty
- `price` must be numeric and greater than 0
- `supplier` cannot be empty or `"Unknown"`

Valid rows are loaded to `staging_products` with an `ingested_at` timestamp. Invalid rows are logged individually with rejection reasons to Cloud Logging. Unparseable files (wrong encoding, missing columns) are moved to the `failed-products` bucket.

**Test results against sample data (50 rows):**
- ✅ 40 rows loaded to `final_products`
- ❌ 10 rows rejected: 7 invalid price, 3 missing product_id, 2 unknown supplier


```bash
# Trigger manually by uploading a file
gsutil cp data/products_load.csv gs://${PROJECT_ID}-raw-products/
```

- **Observability:** Integrated with Cloud Logging. Every rejected row generates a structured log entry with the specific validation failure, enabling a Data Quality Dashboard in the future
- **Idempotency:** The promotion script from staging to final uses a MERGE statement, ensuring that re-running the pipeline doesn't create duplicate records.

---

## Module 02 — AI Enrichment

Reads product `name` and `description` from `final_products`, sends 
them to Gemini 2.5 Flash via Vertex AI, and writes structured 
enrichment data to `enriched_products`.

**Processing architecture:**
- Synchronous loop with `time.sleep(4)` between requests to respect 
  Vertex AI rate limits
- Pydantic schema validation on every Gemini response before writing 
  to BigQuery — malformed responses fail fast instead of corrupting 
  the table
- Incremental by design: LEFT JOIN against `enriched_products` ensures 
  already-processed products are never sent to Gemini again
- Batch insert: results accumulate in a buffer and flush to BigQuery 
  every 10 products, not one-by-one

**Results on Resparked dataset (39 products):**
- ✅ 39/39 products enriched successfully
- ⏱ ~200 seconds total processing time
- 📊 Average confidence score: 0.97
- Categories identified: Herramientas, Insumos, Accesorios, 
  Materiales, Kits

**Enriched schema written to BigQuery:**
```json
{
  "product_id": "RSP-002",
  "category": "Herramientas",
  "subcategory": "Herramienta de grabado",
  "tags": ["grabador", "bolígrafo-eléctrico", "oro-rosa", 
           "grip-silicona", "precisión", "customizer-pen"],
  "confidence_score": 0.98,
  "enriched_at": "2026-04-24T00:30:00Z"
}
```

![Enriched products in BigQuery](docs/enriched_products_preview.png)

```bash
# Run enrichment (incremental — skips already-processed products)
export GCP_PROJECT=your-project-id
export DATASET_NAME=dropshipping
python 02_ai_enrichment/enrich.py
```

---

## Module 03 — Price Monitoring

*(In progress)*

Simulates real-time supplier price updates via Pub/Sub, persists events to BigQuery, and visualizes price trends in Looker Studio.

---

## Key Engineering Patterns

**Dead-letter pattern** — corrupt or unparseable files are isolated in a separate bucket instead of failing silently, enabling manual review without data loss.

**Staging-to-core loading** — data lands in `staging_products` first for inspection before being promoted to `final_products`. This mirrors production ELT patterns used in enterprise data warehouses.

**Row-level validation with structured logging** — instead of rejecting entire files on a single bad row, each row is evaluated independently and rejection reasons are logged with row index and motif. This gives full observability into data quality over time.

**Structured LLM output** — Gemini responses are constrained to a JSON schema with Pydantic validation, making AI output safe to write directly to BigQuery without manual parsing.

---

## Design Decisions

- **Gemini 1.5 Flash over Pro** — lower latency and cost for batch categorization at scale. Flash handles product description analysis with equivalent accuracy at a fraction of the price.
- **Cloud Functions Gen2 over Dataflow** — for this data volume (hundreds to thousands of rows per file), a serverless function is faster to deploy, cheaper to run, and easier to maintain than a full Dataflow pipeline.
- **Row-level vs file-level rejection** — rejecting entire files on partial errors would discard valid data. Row-level validation maximizes throughput while maintaining auditability.
- **gcloud CLI over Terraform** — for a portfolio project with a single environment, a documented Bash script is more transparent and auditable than Terraform state management.

---

## Setup

**Prerequisites:** GCP project with billing enabled, `gcloud` CLI authenticated.

```bash
# 1. Clone the repo
git clone https://github.com/Pulpoide/gcp-ecommerce-data-intelligence.git
cd gcp-ecommerce-data-intelligence

# 2. Set your project
export PROJECT_ID=your-project-id

# 3. Provision all infrastructure
bash infra/setup.sh

# 4. Deploy ingestion function
gcloud functions deploy ingest-products \
  --gen2 --runtime=python312 --region=us-central1 \
  --source=./01_batch_ingestion --entry-point=process_csv \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=${PROJECT_ID}-raw-products" \
  --set-env-vars GCP_PROJECT=${PROJECT_ID},DATASET_NAME=dropshipping \
  --memory=256Mi --timeout=120s

# 5. Test the pipeline
gsutil cp data/products_load.csv gs://${PROJECT_ID}-raw-products/
```

---

## Author

**Joaquín Olivero** — Junior Developer | Backend & AI Integration  
[joacolivero.com](https://joacolivero.com) · [GitHub](https://github.com/Pulpoide) · [LinkedIn](https://linkedin.com/in/joacolivero)