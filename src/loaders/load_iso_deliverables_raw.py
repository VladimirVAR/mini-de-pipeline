from __future__ import annotations

from hashlib import sha256
from pathlib import Path
import os

import pandas as pd
from dotenv import load_dotenv
from psycopg import connect


EXPECTED_SOURCE_COLUMNS = [
    "id",
    "deliverableType",
    "supplementType",
    "reference",
    "title.en",
    "title.fr",
    "publicationDate",
    "edition",
    "icsCode",
    "ownerCommittee",
    "currentStage",
    "replaces",
    "replacedBy",
    "languages",
    "pages.en",
    "scope.en",
]

COLUMN_MAPPING = {
    "id": "id",
    "deliverableType": "deliverable_type",
    "supplementType": "supplement_type",
    "reference": "reference",
    "title.en": "title_en",
    "title.fr": "title_fr",
    "publicationDate": "publication_date",
    "edition": "edition",
    "icsCode": "ics_code",
    "ownerCommittee": "owner_committee",
    "currentStage": "current_stage",
    "replaces": "replaces",
    "replacedBy": "replaced_by",
    "languages": "languages",
    "pages.en": "pages_en",
    "scope.en": "scope_en",
}

RAW_INSERT_COLUMNS = [
    "load_batch_id",
    "source_row_num",
    "id",
    "deliverable_type",
    "supplement_type",
    "reference",
    "title_en",
    "title_fr",
    "publication_date",
    "edition",
    "ics_code",
    "owner_committee",
    "current_stage",
    "replaces",
    "replaced_by",
    "languages",
    "pages_en",
    "scope_en",
    "source_file",
]

NULL_LIKE_STRINGS = {"nan", "none", "null", "n/a", "na"}


def get_connection_params() -> dict[str, str]:
    load_dotenv()
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def normalize_cell_value(value):
    if value is None:
        return None

    if pd.isna(value):
        return None

    if isinstance(value, str):
        stripped = value.strip()
        if stripped == "":
            return None
        if stripped.lower() in NULL_LIKE_STRINGS:
            return None
        return stripped

    return value


def compute_file_hash(file_path: Path) -> str:
    hasher = sha256()
    with file_path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def read_and_prepare_source(source_path: Path) -> pd.DataFrame:
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    df = pd.read_csv(source_path, dtype=str)

    if df.columns.tolist() != EXPECTED_SOURCE_COLUMNS:
        raise ValueError(
            "Unexpected source schema.\n"
            f"Expected: {EXPECTED_SOURCE_COLUMNS}\n"
            f"Actual:   {df.columns.tolist()}"
        )

    df = df.rename(columns=COLUMN_MAPPING)
    df = df.apply(lambda col: col.map(normalize_cell_value))
    df = df.astype(object)
    df = df.where(pd.notna(df), None)
    return df


def create_load_batch(
    *,
    source_name: str,
    source_file: str,
    file_hash_sha256: str,
    load_mode: str,
    load_method: str,
) -> int:
    insert_sql = """
        INSERT INTO raw.load_batches (
            source_name,
            source_file,
            file_hash_sha256,
            load_mode,
            load_method,
            status
        )
        VALUES (%s, %s, %s, %s, %s, 'started')
        RETURNING load_batch_id
    """

    with connect(**get_connection_params()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (source_name, source_file, file_hash_sha256, load_mode, load_method),
            )
            return cur.fetchone()[0]


def update_load_batch_status(
    load_batch_id: int,
    *,
    status: str,
    loaded_row_count: int | None = None,
    error_message: str | None = None,
    finished: bool = False,
) -> None:
    update_sql = """
        UPDATE raw.load_batches
        SET status = %s,
            loaded_row_count = COALESCE(%s, loaded_row_count),
            error_message = %s,
            finished_at = CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE finished_at END
        WHERE load_batch_id = %s
    """

    with connect(**get_connection_params()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                update_sql,
                (status, loaded_row_count, error_message, finished, load_batch_id),
            )


def build_raw_rows(df: pd.DataFrame, *, load_batch_id: int, source_file: str) -> list[tuple]:
    df = df.copy()
    df.insert(0, "source_row_num", range(1, len(df) + 1))
    df.insert(0, "load_batch_id", load_batch_id)
    df["source_file"] = source_file
    df = df[RAW_INSERT_COLUMNS]
    return [tuple(row) for row in df.itertuples(index=False, name=None)]


def insert_rows_executemany(rows: list[tuple]) -> None:
    insert_sql = """
        INSERT INTO raw.iso_deliverables (
            load_batch_id,
            source_row_num,
            id,
            deliverable_type,
            supplement_type,
            reference,
            title_en,
            title_fr,
            publication_date,
            edition,
            ics_code,
            owner_committee,
            current_stage,
            replaces,
            replaced_by,
            languages,
            pages_en,
            scope_en,
            source_file
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    with connect(**get_connection_params()) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)


def insert_rows_copy(rows: list[tuple]) -> None:
    copy_sql = """
        COPY raw.iso_deliverables (
            load_batch_id,
            source_row_num,
            id,
            deliverable_type,
            supplement_type,
            reference,
            title_en,
            title_fr,
            publication_date,
            edition,
            ics_code,
            owner_committee,
            current_stage,
            replaces,
            replaced_by,
            languages,
            pages_en,
            scope_en,
            source_file
        )
        FROM STDIN
    """

    with connect(**get_connection_params()) as conn:
        with conn.cursor() as cur:
            with cur.copy(copy_sql) as copy:
                for row in rows:
                    copy.write_row(row)


def load_iso_deliverables_raw(
    *,
    load_mode: str,
    load_method: str = "executemany",
) -> tuple[int, int]:
    source_path = Path("data/landing/iso_deliverables_metadata.csv")
    source_name = "iso_deliverables_metadata"
    file_hash = compute_file_hash(source_path)
    df = read_and_prepare_source(source_path)

    load_batch_id = create_load_batch(
        source_name=source_name,
        source_file=source_path.name,
        file_hash_sha256=file_hash,
        load_mode=load_mode,
        load_method=load_method,
    )

    try:
        rows = build_raw_rows(df, load_batch_id=load_batch_id, source_file=source_path.name)

        if load_method == "executemany":
            insert_rows_executemany(rows)
        elif load_method == "copy":
            insert_rows_copy(rows)
        else:
            raise ValueError(f"Unsupported load method: {load_method}")

        update_load_batch_status(
            load_batch_id,
            status="raw_loaded",
            loaded_row_count=len(rows),
        )
        return load_batch_id, len(rows)
    except Exception as exc:
        update_load_batch_status(
            load_batch_id,
            status="failed",
            error_message=str(exc),
            finished=True,
        )
        raise


def main() -> None:
    load_batch_id, loaded_rows = load_iso_deliverables_raw(
        load_mode="full_refresh",
        load_method="executemany",
    )
    print(
        f"Loaded {loaded_rows} rows into raw.iso_deliverables "
        f"for load_batch_id={load_batch_id}"
    )


if __name__ == "__main__":
    main()
