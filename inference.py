"""
inference.py — Cartographer
Multi-signal foreign key and primary key inference engine.

The SchemaInferenceEngine runs up to 7 independent signals across
every candidate column pair and combines them into a composite
confidence score using a noisy-OR aggregation.

Signals
-------
1. naming        — exact FK naming conventions ({table}_id, etc.)
2. name_sim      — Jaro-Winkler fuzzy name similarity
3. value_overlap — fraction of FK values found in PK column
4. cardinality   — identical value-set match
5. format        — shared value format fingerprint (UUID, date, etc.)
6. distribution  — cosine similarity of value-frequency histograms
7. null_pattern  — Pearson correlation of null positions

Copyright 2026 Common Gene Labs. All rights reserved.
Original concept by Dr. Amelia Miramonti, PhD.
"""

from __future__ import annotations

import hashlib
import math
import re
from typing import Any

import pandas as pd


# ─── Constants ───────────────────────────────────────────────────────────────

OVERLAP_HIGH    = 0.98
OVERLAP_MEDIUM  = 0.80
NAME_SIM_HIGH   = 0.85
NAME_SIM_MED    = 0.72
DIST_SIM_HIGH   = 0.90
DIST_SIM_MED    = 0.75

FORMAT_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("uuid",      re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)),
    ("email",     re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")),
    ("zip_us",    re.compile(r"^\d{5}(-\d{4})?$")),
    ("phone",     re.compile(r"^\+?[\d\s\-().]{7,15}$")),
    ("iso_date",  re.compile(r"^\d{4}-\d{2}-\d{2}$")),
    ("iso_ts",    re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}")),
    ("hex_color", re.compile(r"^#[0-9a-f]{3,6}$", re.I)),
    ("int_code",  re.compile(r"^\d{1,6}$")),
    ("alpha_code",re.compile(r"^[A-Z]{2,4}$")),
]

WEIGHT_MAP: dict[str, float] = {
    "naming_exact":      1.00,
    "cardinality_match": 0.95,
    "overlap_high":      0.90,
    "name_sim":          0.60,
    "overlap_medium":    0.55,
    "dist_high":         0.50,
    "format_match":      0.40,
    "dist_med":          0.30,
    "name_sim_weak":     0.25,
    "null_corr":         0.20,
}

LABEL_MAP: dict[str, str] = {
    "naming_exact":      "naming",
    "name_sim":          "name_similarity",
    "name_sim_weak":     "name_similarity",
    "overlap_high":      "value_overlap",
    "overlap_medium":    "value_overlap",
    "cardinality_match": "cardinality",
    "format_match":      "format",
    "dist_high":         "distribution",
    "dist_med":          "distribution",
    "null_corr":         "null_pattern",
}

CONF_RANK: dict[str, int] = {"low": 0, "medium": 1, "high": 2}


# ─── Name helpers (module-level, used by engine and by app.py) ────────────────

def clean_name(name: str) -> str:
    """Normalize a column/table name: lowercase, non-alnum → underscore, collapse."""
    n = re.sub(r"[^a-z0-9]", "_", name.lower())
    n = re.sub(r"_+", "_", n).strip("_")
    return n


def id_stem(col_clean: str) -> str:
    """'document_id' → 'document'"""
    return re.sub(r"_id$", "", col_clean)


def is_pk_name(col_clean: str, tname_clean: str) -> bool:
    if col_clean == "id":
        return True
    if col_clean == f"{tname_clean}_id":
        return True
    if col_clean.endswith("_id") and tname_clean.startswith(id_stem(col_clean)):
        return True
    return False


def is_fk_for(col_clean: str, t2_clean: str) -> bool:
    if col_clean == f"{t2_clean}_id":
        return True
    if col_clean.endswith("_id") and t2_clean.startswith(id_stem(col_clean)):
        return True
    return False


def table_digest(tables: dict[str, pd.DataFrame], method: str) -> str:
    parts = "|".join(f"{k}:{len(v)}:{len(v.columns)}" for k, v in tables.items())
    return hashlib.md5(f"{parts}//{method}".encode()).hexdigest()


# ─── Low-level signal functions ───────────────────────────────────────────────

def _jaro_winkler(s1: str, s2: str) -> float:
    """Pure-Python Jaro-Winkler similarity (no external dependency)."""
    if s1 == s2:
        return 1.0
    l1, l2 = len(s1), len(s2)
    if l1 == 0 or l2 == 0:
        return 0.0
    match_dist   = max(l1, l2) // 2 - 1
    match_dist   = max(match_dist, 0)
    s1_matches   = [False] * l1
    s2_matches   = [False] * l2
    matches      = 0
    transpositions = 0

    for i in range(l1):
        lo = max(0, i - match_dist)
        hi = min(i + match_dist + 1, l2)
        for j in range(lo, hi):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(l1):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches/l1 + matches/l2 + (matches - transpositions/2)/matches) / 3
    prefix = sum(1 for i in range(min(4, l1, l2)) if s1[i] == s2[i])
    return jaro + prefix * 0.1 * (1 - jaro)


def _col_dtype_class(series: pd.Series) -> str:
    if pd.api.types.is_numeric_dtype(series):
        return "numeric"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "datetime"
    return "string"


def _format_fingerprint(series: pd.Series, sample: int = 200) -> str | None:
    vals = series.dropna().astype(str)
    if len(vals) == 0:
        return None
    probe = vals.sample(min(sample, len(vals)), random_state=42)
    for name, pat in FORMAT_PATTERNS:
        if probe.str.match(pat).sum() / len(probe) >= 0.80:
            return name
    return None


def _value_overlap(vals1: pd.Series, vals2: pd.Series) -> float:
    s1 = set(vals1.dropna().astype(str).unique()[:50_000])
    s2 = set(vals2.dropna().astype(str).unique()[:50_000])
    if not s1:
        return 0.0
    return len(s1 & s2) / len(s1)


def _distribution_similarity(vals1: pd.Series, vals2: pd.Series) -> float:
    import numpy as np
    s1 = vals1.dropna().astype(str).value_counts()
    s2 = vals2.dropna().astype(str).value_counts()
    if s1.empty or s2.empty:
        return 0.0
    vocab = list(set(s1.index) | set(s2.index))
    v1    = np.array([s1.get(w, 0) for w in vocab], dtype=float)
    v2    = np.array([s2.get(w, 0) for w in vocab], dtype=float)
    n1, n2 = np.linalg.norm(v1), np.linalg.norm(v2)
    if n1 == 0 or n2 == 0:
        return 0.0
    return float(np.dot(v1, v2) / (n1 * n2))


def _null_pattern_correlation(
    df1: pd.DataFrame, col1: str,
    df2: pd.DataFrame, col2: str,
) -> float:
    if len(df1) != len(df2):
        return 0.0
    try:
        from scipy.stats import pearsonr
        mask1 = df1[col1].isna().astype(int)
        mask2 = df2[col2].isna().astype(int)
        if mask1.std() == 0 or mask2.std() == 0:
            return 0.0
        r, _ = pearsonr(mask1, mask2)
        return float(r)
    except Exception:
        return 0.0


# ─── Main engine class ────────────────────────────────────────────────────────

class SchemaInferenceEngine:
    """
    Runs multi-signal FK detection across a dict of DataFrames.

    Usage
    -----
        engine = SchemaInferenceEngine()
        rels = engine.detect_fks(tables, method="both", min_confidence="medium")

    The engine is stateless — safe to reuse across calls.
    Results are deterministic for the same input tables and settings.
    """

    def detect_pks(
        self,
        df: pd.DataFrame,
        table_name: str,
        method: str = "both",
    ) -> list[str]:
        """
        Return at most one primary key column, chosen by priority:
          1. Naming convention match (id / {table}_id)
          2. First all-unique, non-null column whose name ends in _id/_key
          3. First all-unique, non-null column (any name)
        """
        cols = df.columns.tolist()
        n    = len(df)

        if method in ("naming", "both"):
            tname = clean_name(table_name)
            for col in cols:
                if is_pk_name(clean_name(col), tname):
                    return [col]

        if method in ("uniqueness", "both", "content", "all") and n > 0:
            unique_cols = [
                c for c in cols
                if df[c].notna().all() and df[c].nunique() == n
            ]
            id_like = [c for c in unique_cols
                       if re.search(r"(_id|_key|_no|_num|_code)$", clean_name(c))]
            if id_like:
                return [id_like[0]]
            if unique_cols:
                return [unique_cols[0]]

        return []

    def detect_fks(
        self,
        tables: dict[str, pd.DataFrame],
        method: str = "both",
        min_confidence: str = "medium",
        enable_flags: dict[str, bool] | None = None,
    ) -> list[dict]:
        """
        Run FK detection across all table pairs.

        Parameters
        ----------
        tables          : {table_name: DataFrame}
        method          : "naming" | "content" | "both" | "manual"
        min_confidence  : "low" | "medium" | "high"
        enable_flags    : fine-grained per-signal overrides (optional)

        Returns
        -------
        List of relationship dicts, sorted by confidence desc, score desc.
        """
        if method == "manual" or len(tables) < 2:
            return []

        min_rank = CONF_RANK.get(min_confidence, 1)
        flags    = self._build_flags(method, enable_flags)
        tnames   = list(tables.keys())

        # Pre-compute PK columns per table (all-unique + no-nulls)
        pk_map: dict[str, list[str]] = {}
        for t in tnames:
            df = tables[t]
            n  = len(df)
            pk_map[t] = [
                c for c in df.columns
                if n > 0 and df[c].notna().all() and df[c].nunique() == n
            ]

        results: list[dict]    = []
        seen:    set[str]      = set()
        best:    dict[str, float] = {}

        for t1 in tnames:
            df1 = tables[t1]
            if len(df1) == 0 and not flags.get("naming"):
                continue

            for col1 in df1.columns:
                if col1 in pk_map[t1]:
                    continue

                too_large = len(df1) > 150_000 or len(df1.columns) > 300

                for t2 in tnames:
                    if t2 == t1:
                        continue

                    rel_key = f"{t1}|{col1}|{t2}"
                    if rel_key in seen:
                        continue

                    df2 = tables[t2]
                    target_cols = pk_map[t2] if pk_map[t2] else list(df2.columns)

                    if too_large:
                        target_cols = [
                            c for c in target_cols
                            if re.search(r"(_id|_key|id$|key$)", clean_name(c))
                        ]

                    best_result  = None
                    best_to_col  = None

                    for col2 in target_cols:
                        result = self._score_candidate(
                            t1, col1, df1, t2, col2, df2, flags
                        )
                        if result is None:
                            continue
                        if best_result is None or result["score"] > best_result["score"]:
                            best_result = result
                            best_to_col = col2

                    if best_result is None:
                        continue
                    if CONF_RANK.get(best_result["confidence"], 0) < min_rank:
                        continue

                    col_key = f"{t1}|{col1}"
                    if col_key in best and best[col_key] > best_result["score"] + 0.05:
                        continue
                    best[col_key] = best_result["score"]

                    seen.add(rel_key)
                    results.append(dict(
                        from_table  = t1,
                        from_col    = col1,
                        to_table    = t2,
                        to_col      = best_to_col,
                        detected_by = best_result["detected_by"],
                        confidence  = best_result["confidence"],
                        score       = best_result["score"],
                        reasons     = best_result["reasons"],
                        signals     = best_result["signals"],
                    ))

        results.sort(key=lambda r: (-CONF_RANK.get(r["confidence"], 0), -r["score"]))
        return results

    # ── Private helpers ───────────────────────────────────────────────────

    @staticmethod
    def _build_flags(
        method: str,
        overrides: dict[str, bool] | None,
    ) -> dict[str, bool]:
        if overrides is not None:
            return overrides
        use_naming  = method in ("naming", "both", "all")
        use_content = method in ("content", "both", "all", "uniqueness")
        return {
            "naming":        use_naming,
            "value_overlap": use_content,
            "cardinality":   use_content,
            "format":        use_content,
            "distribution":  use_content,
            "null_pattern":  use_content,
        }

    def _score_candidate(
        self,
        t1: str, col1: str, df1: pd.DataFrame,
        t2: str, col2: str, df2: pd.DataFrame,
        flags: dict[str, bool],
    ) -> dict | None:
        signals: dict[str, float] = {}
        reasons: list[str]        = []

        # 1. Naming
        if flags.get("naming"):
            c1  = clean_name(col1)
            t2c = clean_name(t2)
            if is_fk_for(c1, t2c):
                signals["naming_exact"] = 1.0
                reasons.append("exact FK naming")
            else:
                stem1 = re.sub(r"_(id|key|code|num|no)$", "", c1)
                stem2 = re.sub(r"_(id|key|code|num|no)$", "", clean_name(col2))
                sim   = _jaro_winkler(stem1, stem2)
                if sim >= NAME_SIM_HIGH:
                    signals["name_sim"]      = sim
                    reasons.append(f"name similarity {sim:.2f}")
                elif sim >= NAME_SIM_MED:
                    signals["name_sim_weak"] = sim
                    reasons.append(f"weak name similarity {sim:.2f}")

        # 2. Type compatibility guard
        if _col_dtype_class(df1[col1]) != _col_dtype_class(df2[col2]):
            return None

        # 3. Value overlap
        if flags.get("value_overlap") and len(df1) > 0 and len(df2) > 0:
            ov = _value_overlap(df1[col1], df2[col2])
            if ov >= OVERLAP_HIGH:
                signals["overlap_high"]   = ov
                reasons.append(f"value overlap {ov:.0%}")
            elif ov >= OVERLAP_MEDIUM:
                signals["overlap_medium"] = ov
                reasons.append(f"partial overlap {ov:.0%}")

        # 4. Cardinality
        if flags.get("cardinality") and len(df1) > 0 and len(df2) > 0:
            u1 = set(df1[col1].dropna().astype(str))
            u2 = set(df2[col2].dropna().astype(str))
            if u1 and u2 and u1 == u2:
                signals["cardinality_match"] = 1.0
                reasons.append("identical value sets")

        # 5. Format fingerprint
        if flags.get("format") and len(df1) > 0 and len(df2) > 0:
            fmt1 = _format_fingerprint(df1[col1])
            fmt2 = _format_fingerprint(df2[col2])
            if fmt1 and fmt2 and fmt1 == fmt2:
                signals["format_match"] = 0.6
                reasons.append(f"shared format [{fmt1}]")

        # 6. Distribution similarity
        if flags.get("distribution") and len(df1) > 0 and len(df2) > 0:
            ds = _distribution_similarity(df1[col1], df2[col2])
            if ds >= DIST_SIM_HIGH:
                signals["dist_high"] = ds
                reasons.append(f"distribution similarity {ds:.2f}")
            elif ds >= DIST_SIM_MED:
                signals["dist_med"]  = ds
                reasons.append(f"weak distribution similarity {ds:.2f}")

        # 7. Null pattern correlation
        if flags.get("null_pattern") and len(df1) == len(df2):
            nr = _null_pattern_correlation(df1, col1, df2, col2)
            if nr >= 0.80:
                signals["null_corr"] = nr
                reasons.append(f"null pattern corr {nr:.2f}")

        if not signals:
            return None

        # Composite score — noisy OR: 1 - Π(1 - wᵢ)
        score = round(
            min(1.0, 1.0 - math.prod(1.0 - WEIGHT_MAP.get(k, 0.1) for k in signals)),
            3,
        )

        top_signal  = max(signals, key=lambda k: WEIGHT_MAP.get(k, 0.1))
        detected_by = LABEL_MAP.get(top_signal, "content")
        confidence  = "high" if score >= 0.85 else "medium" if score >= 0.55 else "low"

        return {
            "signals":     signals,
            "reasons":     reasons,
            "score":       score,
            "confidence":  confidence,
            "detected_by": detected_by,
        }