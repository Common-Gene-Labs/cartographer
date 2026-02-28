"""
schema_parser.py — Cartographer
Parses JSON and YAML schema definition files into DataFrames and
explicit relationship lists.

Expected JSON format
--------------------
{
  "tables": [
    {
      "name": "orders",
      "columns": [
        {"name": "order_id",    "type": "integer", "primary_key": true},
        {"name": "customer_id", "type": "integer",
         "foreign_key": {"table": "customers", "column": "customer_id"}}
      ]
    }
  ]
}

YAML input is converted to the same structure before parsing.

Copyright 2026 Common Gene Labs. All rights reserved.
Original concept by Dr. Amelia Miramonti, PhD.
"""

from __future__ import annotations

import json
import yaml
import pandas as pd


class SchemaParser:
    """
    Parses structured schema definitions (JSON or YAML) into the
    internal table/relationship representation used by Cartographer.

    Returns
    -------
    tables : dict[str, pd.DataFrame]
        Empty DataFrames with the correct columns, plus attrs metadata.
    rels   : list[dict]
        Explicit FK relationships declared in the schema.
    """

    def parse(self, raw: str, fmt: str = "json") -> tuple[dict[str, pd.DataFrame], list[dict]]:
        """
        Parse a schema string.

        Parameters
        ----------
        raw : str   — raw file content
        fmt : str   — "json" or "yaml"
        """
        if fmt == "yaml":
            data = yaml.safe_load(raw)
            raw  = json.dumps(data)
        return self._parse_json(raw)

    def parse_json(self, raw: str) -> tuple[dict[str, pd.DataFrame], list[dict]]:
        return self._parse_json(raw)

    def parse_yaml(self, raw: str) -> tuple[dict[str, pd.DataFrame], list[dict]]:
        data = yaml.safe_load(raw)
        return self._parse_json(json.dumps(data))

    # ── Internal ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_json(raw: str) -> tuple[dict[str, pd.DataFrame], list[dict]]:
        data   = json.loads(raw)
        tables: dict[str, pd.DataFrame] = {}
        rels:   list[dict]              = []

        for tbl in data.get("tables", []):
            tname = tbl["name"]
            cols  = [c["name"] for c in tbl.get("columns", [])]

            df = pd.DataFrame(columns=cols)
            df.attrs["source"]       = "schema"
            df.attrs["columns_meta"] = tbl.get("columns", [])
            tables[tname] = df

            for col in tbl.get("columns", []):
                fk = col.get("foreign_key")
                if fk:
                    rels.append(dict(
                        from_table  = tname,
                        from_col    = col["name"],
                        to_table    = fk["table"],
                        to_col      = fk.get("column"),
                        detected_by = "schema",
                        confidence  = "high",
                        score       = 1.0,
                        reasons     = ["declared in schema"],
                    ))

        return tables, rels