# ◫ Cartographer

**Map primary keys, foreign keys, and table relationships across any data source.**

Cartographer is an internal data tool built by Common Gene Labs. It takes tabular data — from files or live database connections — and surfaces the hidden structure within it: which columns are primary keys, which are foreign keys, and how tables relate to each other. It produces an interactive entity-relationship diagram alongside column-level metadata, an exportable relationships report, and ready-to-use dbt and Mermaid output.

Originally conceived and designed by **Dr. Amelia Miramonti, PhD**.

---

## What it does

Upload CSV, Excel, Parquet, JSON, or a range of other tabular formats, or connect directly to a database. Cartographer runs a multi-signal inference engine across your tables — examining column names, value overlap, cardinality, format patterns, and statistical distributions — to identify likely relationships. Results are shown as an interactive ERD you can drag, zoom, focus, search, and export.

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
| SPSS | `.sav`, `.por` |
| SAS | `.sas7bdat`, `.xpt` |
| R data | `.rds`, `.rdata`, `.rda` |
| Stata | `.dta` |
| MATLAB | `.mat` |
| HDF5 | `.h5`, `.hdf5` |

Multi-sheet Excel files, multi-object R files, and multi-dataset HDF5 files are loaded as separate tables automatically.

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

Each relationship gets a **low / medium / high** confidence label. A minimum confidence threshold can be set in the sidebar to filter noise. Individual relationships can be promoted, demoted, or suppressed directly from the Relationships tab — edits persist across reruns and are included in session saves.

Schema-defined relationships and database FK constraints always take priority over inferred ones.

> **Large tables:** tables over 100,000 rows are automatically sampled to 10,000 rows for analysis. The original row count is preserved and displayed wherever the table appears.

---

## ERD diagram

The ERD tab renders an interactive vis.js network with the following capabilities:

**Layout modes**

| Mode | Behaviour |
|---|---|
| Force | Physics-based spring layout (default) |
| Hierarchical | Top-down directed tree, physics disabled |
| Circular | Nodes arranged in a ring post-stabilization |
| Compact | Tighter spring constants for dense schemas |

**Interaction**

- **Drag** nodes to reposition them — positions are pinned and restored on every subsequent render, including after page refresh
- **Double-click** a node to unpin it and let physics take over
- **Scroll** to zoom
- **Hover** a node for a full column list with PK/FK annotations, row count, and source
- **Hover** an edge for the full relationship path, detection method, confidence score, and contributing signals
- **Search** — type in the search box above the diagram to highlight matching tables and zoom to them
- **Focus mode** — click the Focus button, then click any table to isolate its immediate neighbourhood; all other nodes and edges are dimmed

**Toolbar buttons**

| Button | Action |
|---|---|
| Focus | Toggle focus-on-click mode |
| Map | Toggle minimap overlay (bottom-right corner) |
| PNG ↓ | Export the current canvas as `cartographer_erd.png` |

**Visual encoding**

- Node borders glow **yellow** for tables matching the current search query
- **Solid edges** indicate naming/schema/cardinality signals; **dashed edges** indicate statistical signals (value overlap, distribution, null pattern, format)
- Edge **thickness** and **opacity** both encode confidence: high → thick/opaque, low → thin/faint
- Source-group **background ellipses** appear when tables come from more than one source, labelled with the source name

---

## Table Details tab

Each table is shown as a card with pills indicating row count, column count, source format, detected PKs, and inferred FK references.

Below each card:
- A **column summary** dataframe lists every column with its type, non-null count, unique count, and PK/FK flags
- A **Show preview** checkbox reveals the first 5 rows of the actual data
- A **⚠ Possible unlinked columns** banner appears when a column name is shared with another table but no relationship has been detected, flagging it as a potential missed FK

Tables that were auto-sampled show a `⚠ SAMPLED N of M rows` banner above the column summary.

---

## Relationships tab

Relationships are grouped by detection method. Each row shows:

- Source and target table + column
- Detection method badge (colour-coded by signal type)
- Confidence dot and score for auto-detected relationships
- Signal chips listing each contributing signal

**Inline confidence editing** — each auto-detected relationship has ▲ Promote / ▼ Demote / ✕ Suppress controls. Suppressed relationships are hidden from the ERD and all exports. A **Clear overrides** button resets all edits.

---

## Export tab

| Export | Format | Contents |
|---|---|---|
| Relationships CSV | `.csv` | All relationships with confidence, score, signals, and reasons |
| dbt schema.yml | `.yml` | dbt models with `unique`, `not_null`, and `relationships` tests |
| Mermaid ERD | `.mmd` | `erDiagram` block ready to paste into GitHub, Notion, or Confluence |
| Save session | `.json` | Full workspace snapshot: tables, relationships, node positions, confidence overrides |

A **Restore session** uploader lets you reload a previously saved workspace, including all node positions and manual edits.

---

## Running locally

Running locally is the most privacy-preserving option — your data never leaves your machine. See [DATA_PRIVACY.md](DATA_PRIVACY.md) for a full audit of exactly where uploaded files and database data are stored and processed.

**Prerequisites:** Python 3.10 or later. SQL Server additionally requires the [Microsoft ODBC Driver 17 or 18](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

**macOS / Linux:**

```bash
# 1. Clone and enter the directory
git clone <repo-url>
cd cartographer

# 2. Create a virtual environment
python -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run
streamlit run app.py
```

**Windows (cmd):**

```cmd
REM 1. Clone and enter the directory
git clone <repo-url>
cd cartographer

REM 2. Create a virtual environment
python -m venv venv
venv\Scripts\activate

REM 3. Install dependencies
pip install -r requirements.txt

REM 4. Run
streamlit run app.py
```

**Windows (PowerShell):**

```powershell
# 1. Clone and enter the directory
git clone <repo-url>
cd cartographer

# 2. Create a virtual environment
python -m venv venv
venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run
streamlit run app.py
```

Opens at `http://localhost:8501`.

### Running in Positron

[Positron](https://github.com/posit-dev/positron) is a data science IDE from Posit. The workflow is the same as any terminal-based Python project:

1. Open the `cartographer` folder (File > Open Folder).
2. Open the Positron terminal (View > Terminal) and create the virtual environment:
   ```bash
   python -m venv venv
   ```
3. Select the interpreter — use the picker in the status bar and choose `venv/bin/python` (macOS/Linux) or `venv\Scripts\python.exe` (Windows).
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Launch the app:
   ```bash
   streamlit run app.py
   ```
6. Open `http://localhost:8501` in your browser. Press `Ctrl+C` to stop.

### Running in VS Code

The steps are identical to Positron. Use the integrated terminal (`` Ctrl+` ``) and the Python interpreter picker in the status bar to select your `venv`.

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
pyreadstat>=1.2.0
pyreadr>=0.4.0
scipy>=1.10.0
h5py>=3.0.0
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

Relationships can be declared explicitly via a JSON or YAML schema file, without needing row data. Schema-defined relationships take priority over all inference.

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
├── inference.py            # Multi-signal FK inference engine
├── schema_parser.py        # JSON/YAML schema parser
├── db_connectors.py        # Database connector classes
├── requirements.txt        # Python dependencies
├── example_schema.json     # Sample schema for testing
└── README.md
```

---

## Credit

Cartographer was originally conceived and designed by **Dr. Amelia Miramonti, PhD**.

Built by [Common Gene Labs](https://elaiken3.github.io/common-gene-labs/) — an independent lab building human-centered tools for starting, focus, and emotional friction.
