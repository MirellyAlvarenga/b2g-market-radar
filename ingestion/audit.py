import json
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine
from datetime import date, datetime, timedelta, timezone

logger = logging.getLogger(__name__)


def ensure_audit_table(engine: Engine) -> None:
    """Garante que a tabela de logs de execução exista no schema audit."""
    ddl = """
    CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
        run_id BIGSERIAL PRIMARY KEY,
        pipeline_name VARCHAR(100) NOT NULL,
        status VARCHAR(20) NOT NULL,
        started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        finished_at TIMESTAMPTZ,
        duration_seconds NUMERIC(10, 2),
        records_extracted INTEGER DEFAULT 0,
        records_inserted INTEGER DEFAULT 0,
        metadata JSONB,
        error_message TEXT
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def start_pipeline_run(
    engine: Engine,
    pipeline_name: str,
    metadata: dict[str, Any] | None = None,
) -> tuple[int, datetime]:
    """Abre um registro de execução com status RUNNING e retorna (run_id, started_at)."""
    ensure_audit_table(engine)
    started_at = datetime.now(timezone.utc)

    sql = text("""
        INSERT INTO audit.pipeline_runs (pipeline_name, status, started_at, metadata)
        VALUES (:pipeline_name, 'RUNNING', :started_at, CAST(:metadata AS JSONB))
        RETURNING run_id
    """)

    with engine.begin() as conn:
        result = conn.execute(
            sql,
            {
                "pipeline_name": pipeline_name,
                "started_at": started_at,
                "metadata": json.dumps(metadata or {}, ensure_ascii=False),
            },
        )
        run_id = result.scalar_one()

    return run_id, started_at


def finish_pipeline_run(
    engine: Engine,
    run_id: int,
    started_at: datetime,
    status: str,
    records_extracted: int = 0,
    records_inserted: int = 0,
    error_message: str | None = None,
) -> None:
    """Atualiza o registro da execução com status final (SUCCESS ou FAILED) e métricas."""
    finished_at = datetime.now(timezone.utc)
    duration = round((finished_at - started_at).total_seconds(), 2)

    sql = text("""
        UPDATE audit.pipeline_runs
        SET status = :status,
            finished_at = :finished_at,
            duration_seconds = :duration_seconds,
            records_extracted = :records_extracted,
            records_inserted = :records_inserted,
            error_message = :error_message
        WHERE run_id = :run_id
    """)

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "run_id": run_id,
                "status": status,
                "finished_at": finished_at,
                "duration_seconds": duration,
                "records_extracted": records_extracted,
                "records_inserted": records_inserted,
                "error_message": error_message,
            },
        )

def get_last_watermark(
    engine: Engine,
    pipeline_name: str,
    default_lookback_days: int = 3,
    safety_overlap_days: int = 2,
) -> tuple[str, str]:
    """
    Calcula a janela incremental (data_inicial, data_final) no formato YYYYMMDD.
    Usa o último data_final registrado com sucesso menos uma margem de segurança
    para capturar atualizações tardias (como homologações).
    """
    ensure_audit_table(engine)
    hoje = datetime.now(timezone.utc).date()

    sql = text("""
        SELECT metadata ->> 'data_final' AS last_data_final
        FROM audit.pipeline_runs
        WHERE pipeline_name = :pipeline_name
          AND status = 'SUCCESS'
          AND metadata ->> 'data_final' IS NOT NULL
        ORDER BY finished_at DESC
        LIMIT 1
    """)

    with engine.begin() as conn:
        last_str = conn.execute(sql, {"pipeline_name": pipeline_name}).scalar_one_or_none()

    if last_str:
        last_date = datetime.strptime(last_str, "%Y%m%d").date()
        start_date = min(last_date - timedelta(days=safety_overlap_days), hoje)
    else:
        start_date = hoje - timedelta(days=default_lookback_days)

    return start_date.strftime("%Y%m%d"), hoje.strftime("%Y%m%d")