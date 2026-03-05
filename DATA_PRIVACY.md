# Data Privacy & Local Usage Guide

This document describes exactly where uploaded files and database data go when you use Cartographer, and provides instructions for running the app locally so that your data never leaves your machine.

---

## Summary

**Cartographer processes all data locally, in memory, on the machine running the app.** No data is transmitted to any external server, API, or cloud service. Nothing is written to disk beyond a short-lived temporary file that is deleted immediately after reading (see below). When the browser tab is closed or the Streamlit session ends, all data is gone.

---

## Data flow, step by step

### 1. File upload

Files are loaded through Streamlit's `st.file_uploader` widget (`app.py:1038`). Streamlit streams the file bytes directly from your browser to the running Python process — the bytes are never written to the server's filesystem for most formats.

| Format | How it is read | Disk write? |
|---|---|---|
| CSV, TSV | `pandas.read_csv()` from the in-memory buffer | No |
| Excel (xlsx, xlsm, xls) | `pandas.ExcelFile` / `pandas.read_excel()` from the in-memory buffer | No |
| ODS | `pandas.read_excel()` from the in-memory buffer | No |
| Parquet | `pandas.read_parquet()` from the in-memory buffer | No |
| JSON, NDJSON | `pandas.read_json()` from the in-memory buffer | No |
| Stata (.dta) | `pandas.read_stata()` from the in-memory buffer | No |
| MATLAB (.mat) | `scipy.io.loadmat()` from the in-memory buffer | No |
| HDF5 (.h5, .hdf5) | `h5py.File(io.BytesIO(...))` — bytes held in memory | No |
| SPSS (.sav, .por) | Written to OS temp dir, read by `pyreadstat`, **deleted immediately** | Temp only |
| SAS (.sas7bdat, .xpt) | Written to OS temp dir, read by `pyreadstat`, **deleted immediately** | Temp only |
| R (.rds, .rdata, .rda) | Written to OS temp dir, read by `pyreadr`, **deleted immediately** | Temp only |

For SPSS, SAS, and R formats, `tempfile.NamedTemporaryFile` creates a file in your OS's temporary directory (e.g., `C:\Users\...\AppData\Local\Temp` on Windows or `/tmp` on Unix). The file is deleted with `os.unlink()` in a `finally` block immediately after parsing (`app.py:1087–1094`, `1100–1108`, `1114–1121`). If the process is killed mid-read, the OS will clean the temp directory on reboot.

### 2. In-memory storage

Once parsed, every table is stored as a pandas DataFrame in `st.session_state.tables` — a Python dictionary that lives entirely in the Streamlit server process's RAM (`app.py:975–976`). No database, no file cache, no disk. The following session-state keys hold your data:

| Key | Contents |
|---|---|
| `st.session_state.tables` | `{table_name: pd.DataFrame}` — all uploaded/connected tables |
| `st.session_state.schema_rels` | Relationships declared in a schema file |
| `st.session_state.manual_rels` | Relationships added manually in the sidebar |
| `st.session_state.fk_cache` | Inference results keyed by a table digest (no row data, only relationship metadata) |
| `st.session_state.db_conn` | Open database connection object (for DB sources only) |
| `st.session_state.db_meta` | Table list, PK set, and FK map returned from DB introspection |
| `st.session_state.node_positions` | ERD node positions (table names and x/y coordinates only) |
| `st.session_state.false_positives` | Suppressed relationship keys |
| `st.session_state.conf_overrides` | Confidence overrides |

All of this is destroyed when the Streamlit session ends (tab closed, server restarted, or the "Clear all" button is clicked).

### 3. Large-table sampling

If any table has more than 100,000 rows, Cartographer creates a sampled copy of 10,000 rows (`app.py:1530–1537`) for inference only. The original full DataFrame remains in `st.session_state.tables`. Sampling uses a fixed random seed (`random_state=42`) for reproducibility. The sampled copy exists only in memory alongside the original.

### 4. Inference and analysis

The inference engine (`inference.py`) receives the in-memory DataFrames and returns a list of relationship metadata dicts — table names, column names, scores, and signal labels. **No row values are ever stored in inference results.** The engine uses row values only transiently during scoring (value overlap, distribution, null-pattern signals) and discards them immediately. All computation runs in the local Python process.

### 5. Database connections

When a database is used instead of files, credentials (host, port, username, password) are entered in the sidebar and passed directly to the connector class (`db_connectors.py`). They are held in `st.session_state.db_conn` for the duration of the session and used to query the database. Data is sampled at `SAMPLE_LIMIT = 10,000` rows per table (`db_connectors.py:43`). Credentials are never logged, written to disk, or transmitted anywhere other than the target database server.

For Google BigQuery, a service account JSON key can be pasted as text. This is parsed in memory (`db_connectors.py:337`) and is not written to disk or stored beyond the session.

### 6. Schema files

Schema definition files (`.json`, `.yaml`, `.yml`) are read as plain text, parsed into empty DataFrames and relationship lists, and stored in session state. They contain column names and relationship declarations only — no row data.

### 7. Exports

All exports are generated in-memory and offered as browser downloads:

| Export | What it contains | Where it goes |
|---|---|---|
| Relationships CSV | Table/column names, confidence scores, signal names | Downloaded to your local machine |
| dbt schema.yml | Table/column names and test declarations | Downloaded to your local machine |
| Mermaid ERD | Table/column names and relationship arrows | Downloaded to your local machine |
| Save session (.json) | Tables serialized as JSON records, relationships, node positions, overrides | Downloaded to your local machine |

The "Save session" export (`app.py:1915–1929`) includes the full table data as JSON. The resulting file stays on your local machine — it is not sent anywhere by the app. Treat it with the same care as the original data files.

Restoring a saved session re-loads that JSON file through the same `st.file_uploader` path described in step 1.

### 8. What the app never does

- Does not call any external API
- Does not send data to Anthropic, Streamlit Cloud, or any third party
- Does not log, persist, or cache data between sessions
- Does not write files to the server's filesystem (beyond the transient temp files noted above)
- Does not read environment variables containing credentials (credentials are entered interactively)

---

## Running Cartographer locally

Running locally is the most privacy-preserving option. Your data never leaves your own machine.

### Prerequisites

- Python 3.10 or later
- For SQL Server: [Microsoft ODBC Driver 17 or 18](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server) installed on the host

### Terminal / command line

```bash
# 1. Clone the repository
git clone <repo-url>
cd cartographer

# 2. Create and activate a virtual environment
python -m venv venv
source venv/bin/activate          # macOS / Linux
venv\Scripts\activate             # Windows (cmd)
# or: venv\Scripts\Activate.ps1  # Windows (PowerShell)

# 3. Install dependencies
pip install -r requirements.txt

# 4. Launch the app
streamlit run app.py
```

The app opens in your browser at `http://localhost:8501`. All data stays on your machine.

### Positron

[Positron](https://github.com/posit-dev/positron) is a next-generation data science IDE from Posit. Running Cartographer in Positron works the same way as any other terminal-based Python workflow.

1. **Open the project** — use File > Open Folder and select the `cartographer` directory.

2. **Create a virtual environment** — open the Positron terminal (View > Terminal) and run:
   ```bash
   python -m venv venv
   ```

3. **Select the interpreter** — Positron may prompt you to select the Python interpreter. Choose the one inside `venv/` (e.g., `venv/bin/python` on macOS/Linux or `venv\Scripts\python.exe` on Windows). You can also set this via the interpreter picker in the bottom status bar.

4. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

5. **Run the app** from the terminal:
   ```bash
   streamlit run app.py
   ```

   The Streamlit dev server will print a local URL (default `http://localhost:8501`). Open it in your browser.

6. **Stop the app** — press `Ctrl+C` in the terminal. The Streamlit process and all in-memory data are immediately destroyed.

### VS Code

The steps are identical to Positron. Use the integrated terminal (`` Ctrl+` ``) and the Python interpreter picker in the status bar to select your `venv`.

### Confirming no data leaves your machine

When running locally, `streamlit run app.py` starts a web server bound to `localhost` only. No port is exposed to the network unless you explicitly pass `--server.address=0.0.0.0`. You can verify this with `netstat -an | grep 8501` — the listening address should show `127.0.0.1`, not `0.0.0.0`.

---

## Notes for shared or institutional environments

If you run Cartographer on a shared server (e.g., a lab compute node or a cloud VM accessed by multiple users), keep these points in mind:

- **Other users on the same machine** cannot access your Streamlit session through the browser, but any user with OS-level access to the Python process could inspect its memory.
- **Streamlit's session isolation** means different browser tabs/users get separate `st.session_state` dictionaries — one user cannot see another's uploaded data through the app.
- **Temporary files** (SPSS, SAS, R formats) are written to the OS temp directory, which may be shared. They are deleted immediately after reading, but briefly exist on disk.
- **Exported files** (including "Save session") contain your data. Store and transmit them according to your institution's data handling policies.
- If your data is subject to IRB, HIPAA, GDPR, or similar requirements, running locally on an approved machine is the safest configuration.
