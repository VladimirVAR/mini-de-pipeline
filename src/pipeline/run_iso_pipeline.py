from __future__ import annotations

from pathlib import Path
from typing import Any

from psycopg import connect

from src.common.logger import get_logger, setup_logging
from src.common.pipeline_config import get_connection_params, get_pipeline_config
from src.common.validation import validate_iso_deliverables
from src.loaders.load_iso_deliverables_raw import (
    load_iso_deliverables_raw,
    update_load_batch_status,
)

logger = get_logger(__name__)


def split_sql_statements(sql_text: str) -> list[str]:
    """
    Simple splitter for this project:
    splits SQL script into individual statements by semicolon.
    Good enough for current DDL/DML files.
    """
    parts = sql_text.split(";")
    statements = [part.strip() for part in parts if part.strip()]
    return statements


def run_sql_file(sql_file_path: Path, params: dict[str, Any] | None = None) -> None:
    sql_text = sql_file_path.read_text(encoding="utf-8")
    statements = split_sql_statements(sql_text)

    with connect(**get_connection_params()) as conn:
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement, params or {})


def main() -> None:
    setup_logging()
    config = get_pipeline_config()

    project_root = Path(__file__).resolve().parents[2]

    schema_sql_file = project_root / "sql" / "ddl" / "create_schemas.sql"
    drop_sql_file = project_root / "sql" / "ddl" / "drop_iso_pipeline_objects.sql"

    base_ddl_sql_files = [
        project_root / "sql" / "raw" / "create_load_batches.sql",
        project_root / "sql" / "raw" / "create_iso_deliverables.sql",
        project_root / "sql" / "staging" / "create_iso_deliverables_clean.sql",
        project_root / "sql" / "staging" / "create_iso_deliverable_languages.sql",
        project_root / "sql" / "staging" / "create_iso_deliverable_ics.sql",
        project_root / "sql" / "staging" / "create_iso_deliverable_relations.sql",
        project_root / "sql" / "warehouse" / "create_fact_deliverable_snapshot.sql",
        project_root / "sql" / "warehouse" / "create_bridge_deliverable_snapshot_languages.sql",
        project_root / "sql" / "warehouse" / "create_bridge_deliverable_snapshot_ics.sql",
        project_root / "sql" / "warehouse" / "create_factless_deliverable_relation_snapshot.sql",
        project_root / "sql" / "warehouse" / "create_vw_deliverables_current.sql",
    ]

    hardening_ddl_sql_files = [
        project_root / "sql" / "ddl" / "add_basic_constraints.sql",
        project_root / "sql" / "ddl" / "create_supporting_indexes.sql",
    ]

    staging_sql_files = [
        project_root / "sql" / "staging" / "load_iso_deliverables_clean.sql",
        project_root / "sql" / "staging" / "load_iso_deliverable_languages.sql",
        project_root / "sql" / "staging" / "load_iso_deliverable_ics.sql",
        project_root / "sql" / "staging" / "load_iso_deliverable_relations.sql",
    ]

    warehouse_sql_files = [
        project_root / "sql" / "warehouse" / "load_fact_deliverable_snapshot.sql",
        project_root / "sql" / "warehouse" / "load_bridge_deliverable_snapshot_languages.sql",
        project_root / "sql" / "warehouse" / "load_bridge_deliverable_snapshot_ics.sql",
        project_root / "sql" / "warehouse" / "load_factless_deliverable_relation_snapshot.sql",
        project_root / "sql" / "warehouse" / "create_vw_deliverables_current.sql",
    ]

    logger.info(
        "Pipeline started | mode=%s | method=%s",
        config.load_mode,
        config.load_method,
    )

    logger.info("Ensuring schemas exist")
    run_sql_file(schema_sql_file)

    if config.load_mode == "full_refresh":
        logger.info("Full refresh mode detected: dropping pipeline objects")
        run_sql_file(drop_sql_file)
    elif config.load_mode == "append":
        logger.info("Append mode detected: preserving prior batches")
    else:
        raise ValueError(f"Unsupported load mode: {config.load_mode}")

    logger.info("Creating / recreating pipeline objects")
    for ddl_file in base_ddl_sql_files:
        logger.info("Running DDL: %s", ddl_file.name)
        run_sql_file(ddl_file)

    if config.load_mode == "full_refresh":
        logger.info("Applying constraints and indexes for full refresh")
        for ddl_file in hardening_ddl_sql_files:
            logger.info("Running DDL: %s", ddl_file.name)
            run_sql_file(ddl_file)

    load_batch_id = None

    try:
        logger.info("Starting raw load")
        load_batch_id, loaded_rows = load_iso_deliverables_raw(
            load_mode=config.load_mode,
            load_method=config.load_method,
        )
        logger.info(
            "Raw load completed | load_batch_id=%s | loaded_rows=%s",
            load_batch_id,
            loaded_rows,
        )

        logger.info("Starting staging load")
        for sql_file in staging_sql_files:
            logger.info("Running staging SQL: %s", sql_file.name)
            run_sql_file(sql_file, {"load_batch_id": load_batch_id})
        logger.info("Staging load completed | load_batch_id=%s", load_batch_id)

        logger.info("Starting warehouse load")
        for sql_file in warehouse_sql_files:
            logger.info("Running warehouse SQL: %s", sql_file.name)
            run_sql_file(sql_file, {"load_batch_id": load_batch_id})
        logger.info("Warehouse load completed | load_batch_id=%s", load_batch_id)

        update_load_batch_status(
            load_batch_id=load_batch_id,
            status="warehouse_loaded",
            finished=True,
        )

        logger.info("Starting validation")
        validation_results = validate_iso_deliverables(load_batch_id)
        logger.info(
            "Validation passed | load_batch_id=%s | raw_rows=%s | staging_rows=%s | warehouse_rows=%s",
            load_batch_id,
            validation_results["raw_batch_row_count"],
            validation_results["staging_clean_batch_row_count"],
            validation_results["warehouse_snapshot_batch_row_count"],
        )

        logger.info("Pipeline finished successfully | load_batch_id=%s", load_batch_id)

    except Exception as exc:
        if load_batch_id is not None:
            update_load_batch_status(
                load_batch_id=load_batch_id,
                status="failed",
                error_message=str(exc),
                finished=True,
            )
        logger.exception("Pipeline failed")
        raise


if __name__ == "__main__":
    main()