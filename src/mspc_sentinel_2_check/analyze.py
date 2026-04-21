from collections import Counter
from dataclasses import dataclass, field

import duckdb

_PARSED_CTE = """
WITH parsed AS (
    SELECT
        split_part(name, '_', 1) AS mission,
        split_part(name, '_', 2) AS product_level,
        split_part(name, '_', 3) AS sensing_start,
        split_part(name, '_', 4) AS baseline,
        split_part(name, '_', 5) AS relative_orbit,
        split_part(name, '_', 6) AS mgrs_tile
    FROM (
        SELECT regexp_replace(
            regexp_replace(prefix, '.*/', ''),
            '\\.SAFE$',
            ''
        ) AS name
        FROM read_parquet(?)
    )
)
"""


@dataclass
class Analysis:
    """Summary statistics computed over a parquet file of .SAFE prefixes."""

    total: int = 0
    duplicates: int = 0
    below_baseline_5: int = 0
    by_baseline: Counter[str] = field(default_factory=Counter)


def analyze(path: str) -> Analysis:
    """Compute summary statistics over a parquet file of .SAFE prefixes using DuckDB.

    A duplicate is a row whose identity — mission, product level, sensing start,
    relative orbit, and MGRS tile — matches another row, ignoring processing
    baseline and processing timestamp.
    """
    con = duckdb.connect()
    (total, duplicates, below_baseline_5) = con.execute(
        _PARSED_CTE
        + """
        SELECT
            (SELECT COUNT(*) FROM parsed),
            (SELECT COALESCE(SUM(c - 1), 0) FROM (
                SELECT COUNT(*) AS c
                FROM parsed
                GROUP BY mission, product_level, sensing_start, relative_orbit, mgrs_tile
                HAVING c > 1
            )),
            (SELECT COUNT(*) FROM parsed WHERE CAST(SUBSTR(baseline, 2, 2) AS INTEGER) < 5)
        """,
        [path],
    ).fetchone()

    by_baseline_rows = con.execute(
        _PARSED_CTE
        + """
        SELECT baseline, COUNT(*) AS c
        FROM parsed
        GROUP BY baseline
        ORDER BY baseline
        """,
        [path],
    ).fetchall()

    return Analysis(
        total=total,
        duplicates=duplicates,
        below_baseline_5=below_baseline_5,
        by_baseline=Counter({baseline: count for baseline, count in by_baseline_rows}),
    )
