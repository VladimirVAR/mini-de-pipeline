from pathlib import Path
import os

from dotenv import load_dotenv
from psycopg import connect

from src.common.pipeline_config import get_pipeline_config
from src.loaders.load_iso_deliverables_raw import load_iso_deliverables_raw


def run_sql_file(sql_file_path: Path) -> None:
    load_dotenv()

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    sql_text = sql_file_path.read_text(encoding="utf-8")

    with connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)


def reset_layers_for_full_refresh() -> None:
    load_dotenv()

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    reset_sql = """
        TRUNCATE TABLE staging.iso_deliverables_clean;
        TRUNCATE TABLE raw.iso_deliverables;
    """

    with connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(reset_sql)


def main() -> None:
    config = get_pipeline_config()

    project_root = Path(__file__).resolve().parents[2]
    staging_sql_path = (
        project_root / "sql" / "staging" / "load_iso_deliverables_clean.sql"
    )

    if config.load_mode == "full_refresh":
        reset_layers_for_full_refresh()
    elif config.load_mode == "append":
        pass
    else:
        raise ValueError(f"Unsupported load mode: {config.load_mode}")

    loaded_rows = load_iso_deliverables_raw(load_method=config.load_method)
    run_sql_file(staging_sql_path)

    print(
        "Pipeline finished successfully | "
        f"mode={config.load_mode} | "
        f"method={config.load_method} | "
        f"loaded_rows={loaded_rows}"
    )


if __name__ == "__main__":
    main()