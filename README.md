# ◫ Cartographer

**Map primary keys, foreign keys, and table relationships across any data source.**

Cartographer is an internal data tool built by Common Gene Labs. It takes tabular data — from files or live database connections — and surfaces the hidden structure within it: which columns are primary keys, which are foreign keys, and how tables relate to each other. It produces an interactive entity-relationship diagram alongside column-level metadata and an exportable relationships report.

Originally conceived and designed by **Dr. Amelia Miramonti, PhD**.

---

## What it does

Upload CSV, Excel, Parquet, or JSON files, or connect directly to a database. Cartographer runs a multi-signal inference engine across your tables — examining column names, value overlap, cardinality, format patterns, and statistical distributions — to identify likely relationships. Results are shown as an interactive ERD you can drag, zoom, and explore.

When a database connection is used, declared foreign key constraints are imported directly, giving you an accurate diagram without inference.

---

## Supported data sources

**File formats**

| Format | Extensions |
|---|---|
| CSV | `.csv` |
| TSV | `.tsv` |
| Excel | `.xlsx`, `.xlsm`, `.xls` |
| OpenDocument Spreadsheet | `.ods` |
| Parquet | `.parquet` |
| JSON array | `.json` |
| Newline-delimited JSON | `.ndjson` |

**Database connections**

| Database | Notes |
|---|---|
| PostgreSQL | Standard credentials |
| MySQL / MariaDB | Standard credentials |
| SQL Server | Requires Microsoft ODBC Driver 13, 17, or 18 |
| Snowflake | Account identifier + optional warehouse and role |
| Google BigQuery | Project + dataset; service account JSON or Application Default Credentials |
| Amazon Redshift | Standard Redshift cluster endpoint |

---

## How the inference works

Cartographer runs up to 7 signals in parallel on each candidate column pair and combines their scores into a composite confidence rating.

| Signal | What it measures |
|---|---|
| **Naming** | Exact name patterns: `id`, `{table}_id`, `{table}_key` |
| **Name similarity** | Fuzzy match between column names across tables |
| **Value overlap** | Fraction of values in column A that exist in column B |
| **Cardinality** | Whether column A has FK-like cardinality (many values → few unique) |
| **Format fingerprint** | Whether both columns share the same value format (UUID, date, integer, etc.) |
| **Distribution similarity** | KS-test on numeric distributions |
| **Null-pattern correlation** | Pearson correlation of null positions |

Each relationship gets a **low / medium / high** confidence label. A minimum confidence threshold can be set in the sidebar to filter noise.

Schema-defined relationships and database FK constraints always take priority over inferred ones.

---

## Running locally

```bash
# 1. Clone and enter the directory
git clone <repo-url>
cd cartographer

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run
streamlit run app.py
```

Opens at `http://localhost:8501`.

---

## Dependencies

```
streamlit>=1.32.0
pandas>=2.0.0
networkx>=3.0
pyvis>=0.3.2
pyyaml>=6.0
scipy>=1.10.0
openpyxl>=3.0.0
xlrd>=2.0.0
odfpy>=1.4.0
pyarrow>=12.0.0
pyodbc>=4.0.0
snowflake-connector-python>=3.0.0
psycopg2-binary>=2.9.0
pymysql>=1.1.0
google-cloud-bigquery>=3.0.0
google-cloud-bigquery-storage>=2.0.0
db-dtypes>=1.0.0
redshift-connector>=2.0.0
```

SQL Server also requires the [Microsoft ODBC Driver](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) installed on the host machine.

---

## Deployment

### Streamlit Community Cloud

1. Push to GitHub
2. Connect at [share.streamlit.io](https://share.streamlit.io)
3. Set main file to `app.py`
4. Deploy — no additional configuration needed

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY app.py .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

```bash
docker build -t cartographer .
docker run -p 8501:8501 cartographer
```

### Other platforms

No external service dependencies. Start command:

```
streamlit run app.py --server.port=$PORT --server.address=0.0.0.0
```

---

## Schema file format

Relationships can also be declared explicitly via a JSON or YAML schema file, without needing row data. Schema-defined relationships take priority over all inference.

```json
{
  "tables": [
    {
      "name": "orders",
      "columns": [
        { "name": "order_id",    "type": "integer", "primary_key": true },
        { "name": "customer_id", "type": "integer",
          "foreign_key": { "table": "customers", "column": "customer_id" } }
      ]
    },
    {
      "name": "customers",
      "columns": [
        { "name": "customer_id", "type": "integer", "primary_key": true },
        { "name": "name",        "type": "text" }
      ]
    }
  ]
}
```

An `example_schema.json` is included — a 5-table e-commerce model ready to load.

---

## Project structure

```
.
├── app.py                  # Main Streamlit application
├── requirements.txt        # Python dependencies
├── example_schema.json     # Sample schema for testing
└── README.md
```

---

## Credit

Cartographer was originally conceived and designed by **Dr. Amelia Miramonti, PhD**.

Built by [Common Gene Labs](https://elaiken3.github.io/common-gene-labs/) — an independent lab building human-centered tools for starting, focus, and emotional friction.
