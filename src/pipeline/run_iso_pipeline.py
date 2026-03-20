from pathlib import Path
import os

from dotenv import load_dotenv
from psycopg import connect

from src.common.logger import get_logger, setup_logging
from src.common.pipeline_config import get_pipeline_config
from src.common.validation import validate_iso_deliverables
from src.loaders.load_iso_deliverables_raw import load_iso_deliverables_raw


logger = get_logger(__name__)


def run_sql_file(sql_file_path: Path) -> None:
    """
    Execute a SQL file against the configured PostgreSQL database.

    Args:
        sql_file_path: Path to the SQL file that should be executed.
    """
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
    """
    Reset pipeline target layers for a full refresh run.

    Truncates warehouse, staging, and raw tables in dependency-safe order
    so the pipeline can be rebuilt from scratch.
    """
    load_dotenv()

    conn_params = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5433"),
        "dbname": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }

    reset_sql = """
        TRUNCATE TABLE warehouse.iso_deliverables_core;
        TRUNCATE TABLE staging.iso_deliverables_clean;
        TRUNCATE TABLE raw.iso_deliverables;
    """

    with connect(**conn_params) as conn:
        with conn.cursor() as cur:
            cur.execute(reset_sql)


def main() -> None:
    """
    Run the full ISO pipeline workflow.

    Pipeline steps:
    - load pipeline configuration
    - reset layers in full refresh mode
    - load source data into raw
    - build staging layer
    - build warehouse layer
    - run validation checks
    - write pipeline status logs
    """
    setup_logging()

    config = get_pipeline_config()

    project_root = Path(__file__).resolve().parents[2]
    staging_sql_path = project_root / "sql" / "staging" / "load_iso_deliverables_clean.sql"
    warehouse_sql_path = project_root / "sql" / "warehouse" / "load_iso_deliverables_core.sql"

    logger.info(
        "Pipeline started | mode=%s | method=%s",
        config.load_mode,
        config.load_method,
    )

    if config.load_mode == "full_refresh":
        logger.info("Full refresh mode detected: resetting raw, staging, and warehouse layers")
        reset_layers_for_full_refresh()
        logger.info("Layer reset completed")
    elif config.load_mode == "append":
        logger.info("Append mode detected: raw layer will not be truncated")
    else:
        raise ValueError(f"Unsupported load mode: {config.load_mode}")

    logger.info("Starting raw load")
    loaded_rows = load_iso_deliverables_raw(load_method=config.load_method)
    logger.info("Raw load completed | loaded_rows=%s", loaded_rows)

    logger.info("Starting staging load")
    run_sql_file(staging_sql_path)
    logger.info("Staging load completed")

    logger.info("Starting warehouse load")
    run_sql_file(warehouse_sql_path)
    logger.info("Warehouse load completed")

    logger.info("Starting validation")
    validation_results = validate_iso_deliverables()
    logger.info(
        "Validation passed | raw_rows=%s | staging_rows=%s | warehouse_rows=%s",
        validation_results["raw_row_count"],
        validation_results["staging_row_count"],
        validation_results["warehouse_row_count"],
    )

    logger.info("Pipeline finished successfully")


if __name__ == "__main__":
    main()