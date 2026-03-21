from __future__ import annotations

import os

from dotenv import load_dotenv
from psycopg import connect



def validate_iso_deliverables(load_batch_id: int) -> dict[str, int]:
    """
    Validate a single pipeline batch across raw, staging, and warehouse layers.
    """
    load_dotenv()

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    queries = {
        "raw_batch_row_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "staging_clean_batch_row_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "warehouse_snapshot_batch_row_count": """
            SELECT COUNT(*)
            FROM warehouse.fact_deliverable_snapshot
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "staging_language_row_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverable_languages
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "warehouse_language_row_count": """
            SELECT COUNT(*)
            FROM warehouse.bridge_deliverable_snapshot_languages
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "staging_ics_row_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverable_ics
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "warehouse_ics_row_count": """
            SELECT COUNT(*)
            FROM warehouse.bridge_deliverable_snapshot_ics
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "staging_relation_row_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverable_relations
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "warehouse_relation_row_count": """
            SELECT COUNT(*)
            FROM warehouse.factless_deliverable_relation_snapshot
            WHERE load_batch_id = %(load_batch_id)s
        """,
        "staging_language_orphan_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverable_languages l
            LEFT JOIN staging.iso_deliverables_clean c
              ON l.raw_row_id = c.raw_row_id
            WHERE l.load_batch_id = %(load_batch_id)s
              AND c.raw_row_id IS NULL
        """,
        "staging_ics_orphan_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverable_ics i
            LEFT JOIN staging.iso_deliverables_clean c
              ON i.raw_row_id = c.raw_row_id
            WHERE i.load_batch_id = %(load_batch_id)s
              AND c.raw_row_id IS NULL
        """,
        "staging_relation_orphan_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverable_relations r
            LEFT JOIN staging.iso_deliverables_clean c
              ON r.raw_row_id = c.raw_row_id
            WHERE r.load_batch_id = %(load_batch_id)s
              AND c.raw_row_id IS NULL
        """,
        "malformed_raw_id_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE load_batch_id = %(load_batch_id)s
              AND id IS NOT NULL
              AND id !~ '^\\d+$'
        """,
        "staging_null_deliverable_id_from_malformed_raw_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean c
            JOIN raw.iso_deliverables r
              ON c.raw_row_id = r.raw_row_id
            WHERE c.load_batch_id = %(load_batch_id)s
              AND r.id IS NOT NULL
              AND r.id !~ '^\\d+$'
              AND c.deliverable_id IS NULL
        """,
        "malformed_raw_publication_date_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE load_batch_id = %(load_batch_id)s
              AND publication_date IS NOT NULL
              AND NOT (
                    publication_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
                AND TO_CHAR(TO_DATE(publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = publication_date
              )
        """,
        "staging_null_publication_date_from_malformed_raw_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean c
            JOIN raw.iso_deliverables r
              ON c.raw_row_id = r.raw_row_id
            WHERE c.load_batch_id = %(load_batch_id)s
              AND r.publication_date IS NOT NULL
              AND NOT (
                    r.publication_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
                AND TO_CHAR(TO_DATE(r.publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = r.publication_date
              )
              AND c.publication_date IS NULL
        """,
        "malformed_raw_pages_en_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE load_batch_id = %(load_batch_id)s
              AND pages_en IS NOT NULL
              AND pages_en !~ '^\\d+$'
        """,
        "staging_null_pages_en_from_malformed_raw_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean c
            JOIN raw.iso_deliverables r
              ON c.raw_row_id = r.raw_row_id
            WHERE c.load_batch_id = %(load_batch_id)s
              AND r.pages_en IS NOT NULL
              AND r.pages_en !~ '^\\d+$'
              AND c.pages_en IS NULL
        """,
        "warehouse_false_has_title_fr_count": """
            SELECT COUNT(*)
            FROM warehouse.fact_deliverable_snapshot
            WHERE load_batch_id = %(load_batch_id)s
              AND has_title_fr = false
        """,
        "warehouse_null_title_fr_count": """
            SELECT COUNT(*)
            FROM warehouse.fact_deliverable_snapshot
            WHERE load_batch_id = %(load_batch_id)s
              AND title_fr IS NULL
        """,
    }

    results: dict[str, int] = {}

    with connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for metric_name, sql_text in queries.items():
                cur.execute(sql_text, {"load_batch_id": load_batch_id})
                results[metric_name] = cur.fetchone()[0]

    if results["raw_batch_row_count"] != results["staging_clean_batch_row_count"]:
        raise RuntimeError(
            "Validation failed: raw_batch_row_count does not match staging_clean_batch_row_count "
            f"({results['raw_batch_row_count']} != {results['staging_clean_batch_row_count']})"
        )

    if results["staging_clean_batch_row_count"] != results["warehouse_snapshot_batch_row_count"]:
        raise RuntimeError(
            "Validation failed: staging_clean_batch_row_count does not match warehouse_snapshot_batch_row_count "
            f"({results['staging_clean_batch_row_count']} != {results['warehouse_snapshot_batch_row_count']})"
        )

    if results["staging_language_row_count"] != results["warehouse_language_row_count"]:
        raise RuntimeError(
            "Validation failed: staging_language_row_count does not match warehouse_language_row_count "
            f"({results['staging_language_row_count']} != {results['warehouse_language_row_count']})"
        )

    if results["staging_ics_row_count"] != results["warehouse_ics_row_count"]:
        raise RuntimeError(
            "Validation failed: staging_ics_row_count does not match warehouse_ics_row_count "
            f"({results['staging_ics_row_count']} != {results['warehouse_ics_row_count']})"
        )

    if results["staging_relation_row_count"] != results["warehouse_relation_row_count"]:
        raise RuntimeError(
            "Validation failed: staging_relation_row_count does not match warehouse_relation_row_count "
            f"({results['staging_relation_row_count']} != {results['warehouse_relation_row_count']})"
        )

    for orphan_metric in (
        "staging_language_orphan_count",
        "staging_ics_orphan_count",
        "staging_relation_orphan_count",
    ):
        if results[orphan_metric] != 0:
            raise RuntimeError(f"Validation failed: {orphan_metric} != 0 ({results[orphan_metric]})")

    if results["malformed_raw_id_count"] != results["staging_null_deliverable_id_from_malformed_raw_count"]:
        raise RuntimeError(
            "Validation failed: malformed_raw_id_count does not match staging_null_deliverable_id_from_malformed_raw_count "
            f"({results['malformed_raw_id_count']} != {results['staging_null_deliverable_id_from_malformed_raw_count']})"
        )

    if results["malformed_raw_publication_date_count"] != results["staging_null_publication_date_from_malformed_raw_count"]:
        raise RuntimeError(
            "Validation failed: malformed_raw_publication_date_count does not match "
            "staging_null_publication_date_from_malformed_raw_count "
            f"({results['malformed_raw_publication_date_count']} != {results['staging_null_publication_date_from_malformed_raw_count']})"
        )

    if results["malformed_raw_pages_en_count"] != results["staging_null_pages_en_from_malformed_raw_count"]:
        raise RuntimeError(
            "Validation failed: malformed_raw_pages_en_count does not match "
            "staging_null_pages_en_from_malformed_raw_count "
            f"({results['malformed_raw_pages_en_count']} != {results['staging_null_pages_en_from_malformed_raw_count']})"
        )

    if results["warehouse_false_has_title_fr_count"] != results["warehouse_null_title_fr_count"]:
        raise RuntimeError(
            "Validation failed: warehouse_false_has_title_fr_count does not match warehouse_null_title_fr_count "
            f"({results['warehouse_false_has_title_fr_count']} != {results['warehouse_null_title_fr_count']})"
        )

    return results