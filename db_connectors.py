"""
db_connectors.py — Cartographer
Database connection and schema introspection classes.

Each connector wraps a specific database engine and exposes a
consistent interface:
  .connect()                     → opens and stores the connection
  .introspect()                  → returns (tables, pk_set, fk_map)
  .load_table(schema, table)     → returns a pd.DataFrame sample
  .close()                       → closes the connection

Copyright 2026 Common Gene Labs. All rights reserved.
Original concept by Dr. Amelia Miramonti, PhD.
"""

from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any
import re
import pandas as pd


# ─── Type aliases ────────────────────────────────────────────────────────────
# tables   : list of (schema, table_name)
# pk_set   : set of (schema, table_name, column_name)
# fk_map   : {(schema, table, col): (ref_schema, ref_table, ref_col)}
TableList = list[tuple[str, str]]
PKSet     = set[tuple[str, str, str]]
FKMap     = dict[tuple[str, str, str], tuple[str, str, str]]
Meta      = tuple[TableList, PKSet, FKMap]


# ─── Base class ──────────────────────────────────────────────────────────────

class DatabaseConnector(ABC):
    """
    Abstract base for all database connectors.
    Subclasses implement _open(), _introspect(), and _sample_query().
    """

    DB_TYPE: str = ""          # e.g. "postgres" — set per subclass
    SAMPLE_LIMIT: int = 10_000

    def __init__(self) -> None:
        self._conn: Any = None

    # ── Public interface ──────────────────────────────────────────────────

    def connect(self, **kwargs) -> None:
        """Open the connection. Raises on failure."""
        self._conn = self._open(**kwargs)

    def introspect(self, **kwargs) -> Meta:
        """Return (tables, pk_set, fk_map) for the connected database."""
        self._require_connection()
        return self._introspect(**kwargs)

    def load_table(self, schema: str, table: str) -> pd.DataFrame:
        """Load up to SAMPLE_LIMIT rows from schema.table into a DataFrame."""
        self._require_connection()
        return self._load(schema, table)

    def close(self) -> None:
        """Close the connection if open."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    @property
    def is_connected(self) -> bool:
        return self._conn is not None

    # ── Abstract internals ────────────────────────────────────────────────

    @abstractmethod
    def _open(self, **kwargs) -> Any:
        """Return an open connection/client object."""

    @abstractmethod
    def _introspect(self, **kwargs) -> Meta:
        """Return (tables, pk_set, fk_map)."""

    @abstractmethod
    def _load(self, schema: str, table: str) -> pd.DataFrame:
        """Return a sampled DataFrame for the given table."""

    # ── Shared utility ────────────────────────────────────────────────────

    def _require_connection(self) -> None:
        if self._conn is None:
            raise RuntimeError(f"{self.__class__.__name__}: not connected.")

    @staticmethod
    def sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize column names to safe identifiers."""
        df.columns = [re.sub(r"[^a-zA-Z0-9_]", "_", str(c)) for c in df.columns]
        return df


# ─── PostgreSQL ──────────────────────────────────────────────────────────────

class PostgresConnector(DatabaseConnector):
    DB_TYPE = "postgres"

    def _open(self, host, port, database, user, password, **_) -> Any:
        import psycopg2
        return psycopg2.connect(
            host=host, port=int(port), dbname=database,
            user=user, password=password, connect_timeout=10,
        )

    def _introspect(self, schema="public", **_) -> Meta:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            (schema,),
        )
        tables: TableList = [(schema, r[0]) for r in cur.fetchall()]

        cur.execute(
            "SELECT kcu.table_name, kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema    = kcu.table_schema "
            "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s",
            (schema,),
        )
        pk_set: PKSet = {(schema, r[0], r[1]) for r in cur.fetchall()}

        cur.execute(
            "SELECT kcu.table_name, kcu.column_name, "
            "       ccu.table_name, ccu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema    = kcu.table_schema "
            "JOIN information_schema.constraint_column_usage ccu "
            "  ON ccu.constraint_name = tc.constraint_name "
            " AND ccu.table_schema    = tc.table_schema "
            "WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s",
            (schema,),
        )
        fk_map: FKMap = {
            (schema, r[0], r[1]): (schema, r[2], r[3])
            for r in cur.fetchall()
        }
        return tables, pk_set, fk_map

    def _load(self, schema: str, table: str) -> pd.DataFrame:
        q = f'SELECT * FROM "{schema}"."{table}" LIMIT {self.SAMPLE_LIMIT}'
        return self.sanitize_columns(pd.read_sql(q, self._conn))


# ─── MySQL / MariaDB ──────────────────────────────────────────────────────────

class MySQLConnector(DatabaseConnector):
    DB_TYPE = "mysql"

    def _open(self, host, port, database, user, password, **_) -> Any:
        import pymysql
        return pymysql.connect(
            host=host, port=int(port), db=database,
            user=user, password=password, connect_timeout=10,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _introspect(self, schema=None, **kwargs) -> Meta:
        # For MySQL, schema == database name
        database = schema or kwargs.get("database", "")
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "ORDER BY table_name",
                (database,),
            )
            tables: TableList = [(database, r["table_name"]) for r in cur.fetchall()]

            cur.execute(
                "SELECT table_name, column_name "
                "FROM information_schema.key_column_usage "
                "WHERE constraint_name = 'PRIMARY' AND table_schema = %s",
                (database,),
            )
            pk_set: PKSet = {
                (database, r["table_name"], r["column_name"])
                for r in cur.fetchall()
            }

            cur.execute(
                "SELECT table_name, column_name, "
                "       referenced_table_name, referenced_column_name "
                "FROM information_schema.key_column_usage "
                "WHERE table_schema = %s AND referenced_table_name IS NOT NULL",
                (database,),
            )
            fk_map: FKMap = {
                (database, r["table_name"], r["column_name"]):
                (database, r["referenced_table_name"], r["referenced_column_name"])
                for r in cur.fetchall()
            }
        return tables, pk_set, fk_map

    def _load(self, schema: str, table: str) -> pd.DataFrame:
        q = f"SELECT * FROM `{schema}`.`{table}` LIMIT {self.SAMPLE_LIMIT}"
        return self.sanitize_columns(pd.read_sql(q, self._conn))


# ─── SQL Server ───────────────────────────────────────────────────────────────

class SQLServerConnector(DatabaseConnector):
    DB_TYPE = "sqlserver"

    def _open(self, host, port, database, user, password, driver, **_) -> Any:
        import pyodbc
        conn_str = (
            f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
            f"UID={user};PWD={password};TrustServerCertificate=yes;Encrypt=yes;"
        )
        return pyodbc.connect(conn_str, timeout=10)

    def _introspect(self, schema=None, **kwargs) -> Meta:
        database = schema or kwargs.get("database", "")
        cur = self._conn.cursor()

        cur.execute(
            f"SELECT TABLE_SCHEMA, TABLE_NAME "
            f"FROM [{database}].INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_TYPE = 'BASE TABLE' "
            f"ORDER BY TABLE_SCHEMA, TABLE_NAME"
        )
        tables: TableList = [(r[0], r[1]) for r in cur.fetchall()]

        cur.execute(
            f"SELECT KU.TABLE_SCHEMA, KU.TABLE_NAME, KU.COLUMN_NAME "
            f"FROM [{database}].INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC "
            f"JOIN [{database}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE KU "
            f"  ON TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME "
            f" AND TC.TABLE_SCHEMA    = KU.TABLE_SCHEMA "
            f"WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'"
        )
        pk_set: PKSet = {(r[0], r[1], r[2]) for r in cur.fetchall()}

        cur.execute(
            f"SELECT FK.TABLE_SCHEMA, FK.TABLE_NAME, FK.COLUMN_NAME, "
            f"       PK.TABLE_SCHEMA, PK.TABLE_NAME, PK.COLUMN_NAME "
            f"FROM [{database}].INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS RC "
            f"JOIN [{database}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE FK "
            f"  ON RC.CONSTRAINT_NAME = FK.CONSTRAINT_NAME "
            f"JOIN [{database}].INFORMATION_SCHEMA.KEY_COLUMN_USAGE PK "
            f"  ON RC.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME"
        )
        fk_map: FKMap = {
            (r[0], r[1], r[2]): (r[3], r[4], r[5])
            for r in cur.fetchall()
        }
        return tables, pk_set, fk_map

    def _load(self, schema: str, table: str) -> pd.DataFrame:
        q = f"SELECT TOP {self.SAMPLE_LIMIT} * FROM [{schema}].[{table}]"
        return self.sanitize_columns(pd.read_sql(q, self._conn))


# ─── Snowflake ────────────────────────────────────────────────────────────────

class SnowflakeConnector(DatabaseConnector):
    DB_TYPE = "snowflake"

    def __init__(self) -> None:
        super().__init__()
        self._database: str = ""
        self._schema: str = ""

    def _open(self, account, user, password, database, schema,
              warehouse="", role="", **_) -> Any:
        import snowflake.connector
        self._database = database
        self._schema   = schema
        kw = dict(account=account, user=user, password=password,
                  database=database, schema=schema)
        if warehouse: kw["warehouse"] = warehouse
        if role:      kw["role"]      = role
        return snowflake.connector.connect(**kw)

    def _introspect(self, schema=None, **kwargs) -> Meta:
        db  = self._database
        sch = schema or self._schema
        cur = self._conn.cursor()

        cur.execute(f"SHOW TABLES IN SCHEMA {db}.{sch}")
        tables: TableList = [(sch, r[1]) for r in cur.fetchall()]

        cur.execute(f"SHOW PRIMARY KEYS IN SCHEMA {db}.{sch}")
        pk_set: PKSet = {(sch, r[3], r[4]) for r in cur.fetchall()}

        cur.execute(f"SHOW IMPORTED KEYS IN SCHEMA {db}.{sch}")
        fk_map: FKMap = {
            (sch, r[7], r[8]): (sch, r[3], r[4])
            for r in cur.fetchall()
        }
        return tables, pk_set, fk_map

    def _load(self, schema: str, table: str) -> pd.DataFrame:
        q = f'SELECT * FROM "{schema}"."{table}" LIMIT {self.SAMPLE_LIMIT}'
        return self.sanitize_columns(pd.read_sql(q, self._conn))


# ─── Google BigQuery ──────────────────────────────────────────────────────────

class BigQueryConnector(DatabaseConnector):
    """
    conn is a google.cloud.bigquery.Client (not a DBAPI connection).
    BigQuery has no declared PK/FK constraints — inference runs on loaded data.
    """
    DB_TYPE = "bigquery"

    def __init__(self) -> None:
        super().__init__()
        self._project: str = ""
        self._dataset: str = ""

    def _open(self, project, dataset, credentials_json=None, **_) -> Any:
        from google.cloud import bigquery
        from google.oauth2 import service_account

        self._project = project
        self._dataset = dataset

        if credentials_json:
            info  = json.loads(credentials_json)
            creds = service_account.Credentials.from_service_account_info(
                info, scopes=["https://www.googleapis.com/auth/bigquery"]
            )
            return bigquery.Client(project=project, credentials=creds)
        return bigquery.Client(project=project)   # Application Default Credentials

    def _introspect(self, schema=None, **kwargs) -> Meta:
        dataset = schema or self._dataset
        tables: TableList = [
            (dataset, t.table_id)
            for t in self._conn.list_tables(f"{self._project}.{dataset}")
        ]
        pk_set: PKSet = set()
        fk_map: FKMap = {}
        return tables, pk_set, fk_map

    def _load(self, schema: str, table: str) -> pd.DataFrame:
        q = f"SELECT * FROM `{schema}`.`{table}` LIMIT {self.SAMPLE_LIMIT}"
        return self.sanitize_columns(self._conn.query(q).to_dataframe())


# ─── Amazon Redshift ──────────────────────────────────────────────────────────

class RedshiftConnector(DatabaseConnector):
    """
    Redshift stores declared FK constraints but does not enforce them.
    Constraints are read where present; inference fills the gaps.
    """
    DB_TYPE = "redshift"

    def _open(self, host, port, database, user, password, **_) -> Any:
        import redshift_connector
        return redshift_connector.connect(
            host=host, port=int(port), database=database,
            user=user, password=password,
        )

    def _introspect(self, schema="public", **_) -> Meta:
        cur = self._conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
            "ORDER BY table_name",
            (schema,),
        )
        tables: TableList = [(schema, r[0]) for r in cur.fetchall()]

        cur.execute(
            "SELECT tc.table_name, kcu.column_name "
            "FROM information_schema.table_constraints tc "
            "JOIN information_schema.key_column_usage kcu "
            "  ON tc.constraint_name = kcu.constraint_name "
            " AND tc.table_schema    = kcu.table_schema "
            "WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s",
            (schema,),
        )
        pk_set: PKSet = {(schema, r[0], r[1]) for r in cur.fetchall()}
        fk_map: FKMap = {}   # Redshift does not enforce FK constraints
        return tables, pk_set, fk_map

    def _load(self, schema: str, table: str) -> pd.DataFrame:
        q = f'SELECT * FROM "{schema}"."{table}" LIMIT {self.SAMPLE_LIMIT}'
        return self.sanitize_columns(pd.read_sql(q, self._conn))


# ─── Registry ────────────────────────────────────────────────────────────────

CONNECTORS: dict[str, type[DatabaseConnector]] = {
    "postgres":   PostgresConnector,
    "mysql":      MySQLConnector,
    "sqlserver":  SQLServerConnector,
    "snowflake":  SnowflakeConnector,
    "bigquery":   BigQueryConnector,
    "redshift":   RedshiftConnector,
}


def get_connector(db_type: str) -> DatabaseConnector:
    """
    Factory function. Returns a fresh connector instance for the given db_type.
    Raises KeyError for unknown types.
    """
    cls = CONNECTORS.get(db_type)
    if cls is None:
        raise KeyError(f"Unknown database type: {db_type!r}. "
                       f"Valid options: {list(CONNECTORS)}")
    return cls()