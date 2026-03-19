import os

from dotenv import load_dotenv
from psycopg import connect


def validate_iso_deliverables() -> dict[str, int]:
    load_dotenv()

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    queries = {
        "raw_row_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
        """,
        "staging_row_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean
        """,
        "invalid_id_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE id IS NULL
               OR id !~ '^\\d+$'
        """,
        "staging_null_id_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean
            WHERE id IS NULL
        """,
        "invalid_publication_date_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE publication_date IS NULL
               OR NOT (
                   publication_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
                   AND to_char(to_date(publication_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') = publication_date
               )
        """,
        "staging_null_publication_date_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean
            WHERE publication_date IS NULL
        """,
        "invalid_pages_en_count": """
            SELECT COUNT(*)
            FROM raw.iso_deliverables
            WHERE pages_en IS NULL
               OR pages_en !~ '^\\d+$'
        """,
        "staging_null_pages_en_count": """
            SELECT COUNT(*)
            FROM staging.iso_deliverables_clean
            WHERE pages_en IS NULL
        """,
    }

    results: dict[str, int] = {}

    with connect(**conn_params) as conn:
        with conn.cursor() as cur:
            for metric_name, sql_text in queries.items():
                cur.execute(sql_text)
                results[metric_name] = cur.fetchone()[0]

    if results["raw_row_count"] != results["staging_row_count"]:
        raise RuntimeError(
            "Validation failed: raw_row_count does not match staging_row_count "
            f"({results['raw_row_count']} != {results['staging_row_count']})"
        )

    if results["invalid_id_count"] != results["staging_null_id_count"]:
        raise RuntimeError(
            "Validation failed: invalid_id_count does not match staging_null_id_count "
            f"({results['invalid_id_count']} != {results['staging_null_id_count']})"
        )

    if (
        results["invalid_publication_date_count"]
        != results["staging_null_publication_date_count"]
    ):
        raise RuntimeError(
            "Validation failed: invalid_publication_date_count does not match "
            "staging_null_publication_date_count "
            f"({results['invalid_publication_date_count']} != "
            f"{results['staging_null_publication_date_count']})"
        )

    if results["invalid_pages_en_count"] != results["staging_null_pages_en_count"]:
        raise RuntimeError(
            "Validation failed: invalid_pages_en_count does not match "
            f"staging_null_pages_en_count "
            f"({results['invalid_pages_en_count']} != "
            f"{results['staging_null_pages_en_count']})"
        )

    return results