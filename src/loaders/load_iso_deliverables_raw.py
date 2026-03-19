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

INSERT_COLUMNS = [
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

        return value

    return value


def load_iso_deliverables_raw(load_method: str = "executemany") -> int:
    load_dotenv()

    source_path = Path("data/landing/iso_deliverables_metadata.csv")
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

    # Force all pandas missing markers to real Python None
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    df["source_file"] = source_path.name
    df = df[INSERT_COLUMNS]

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    insert_sql = """
        INSERT INTO raw.iso_deliverables (
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
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """

    with connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM raw.iso_deliverables
                WHERE source_file = %s
                LIMIT 1
                """,
                (source_path.name,),
            )
            if cur.fetchone():
                raise RuntimeError(
                    f"Data from {source_path.name} has already been loaded into raw.iso_deliverables"
                )

            if load_method == "executemany":
                cur.executemany(insert_sql, rows)
            elif load_method == "copy":
                raise NotImplementedError(
                    "COPY load method is planned but not implemented yet"
                )
            else:
                raise ValueError(f"Unsupported load method: {load_method}")

    return len(rows)


def main() -> None:
    loaded_rows = load_iso_deliverables_raw(load_method="executemany")
    print(f"Loaded {loaded_rows} rows into raw.iso_deliverables")


if __name__ == "__main__":
    main()