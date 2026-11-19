import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT_DIR / ".env")
except ImportError:
    pass

from ingestion.audit import finish_pipeline_run, start_pipeline_run

logger = logging.getLogger(__name__)

BRASILAPI_CNPJ_URL = "https://brasilapi.com.br/api/cnpj/v1"


def get_engine() -> Engine:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    db = os.getenv("POSTGRES_DB", "b2g_market_radar")
    user = os.getenv("POSTGRES_USER", "b2g_admin")
    password = os.getenv("POSTGRES_PASSWORD", "b2g_secret_password")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ensure_bronze_brasilapi_table(engine: Engine) -> None:
    """Garante a tabela de cache/histórico de CNPJs consultados na BrasilAPI."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS bronze;

    CREATE TABLE IF NOT EXISTS bronze.brasilapi_empresas (
        cnpj VARCHAR(14) PRIMARY KEY,
        http_status INTEGER NOT NULL,
        raw_payload JSONB NOT NULL,
        extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def get_pending_cnpjs(engine: Engine, limit: int = 150) -> list[str]:
    """
    Busca CNPJs únicos (14 dígitos) de fornecedores contratados no PNCP
    que ainda não foram consultados na BrasilAPI, priorizando maiores volumes.
    """
    sql = text("""
        WITH fornecedores_contratos AS (
            SELECT
                REGEXP_REPLACE(raw_payload ->> 'niFornecedor', '[^0-9]', '', 'g') AS cnpj_limpo,
                SUM(COALESCE(NULLIF(raw_payload ->> 'valorGlobal', '')::NUMERIC, 0)) AS volume_total
            FROM bronze.pncp_contratos
            WHERE raw_payload ->> 'niFornecedor' IS NOT NULL
            GROUP BY 1
        )
        SELECT fc.cnpj_limpo
        FROM fornecedores_contratos fc
        LEFT JOIN bronze.brasilapi_empresas be
            ON fc.cnpj_limpo = be.cnpj
        WHERE LENGTH(fc.cnpj_limpo) = 14
          AND be.cnpj IS NULL
        ORDER BY fc.volume_total DESC
        LIMIT :limit
    """)

    with engine.begin() as conn:
        rows = conn.execute(sql, {"limit": limit}).fetchall()

    return [row[0] for row in rows]


def fetch_cnpj(client: httpx.Client, cnpj: str, max_retries: int = 3) -> tuple[int, dict[str, Any]]:
    """Consulta um CNPJ na BrasilAPI tratando rate limit (429) e não encontrado (404)."""
    url = f"{BRASILAPI_CNPJ_URL}/{cnpj}"

    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(url)

            if response.status_code == 200:
                return 200, response.json()

            if response.status_code in (400, 404):
                return response.status_code, {"error": "CNPJ não encontrado ou inválido", "cnpj": cnpj}

            if response.status_code in (429, 500, 502, 503, 504):
                wait_seconds = min(3 * attempt, 20)
                logger.warning("BrasilAPI HTTP %s para CNPJ %s. Aguardando %ss...", response.status_code, cnpj, wait_seconds)
                time.sleep(wait_seconds)
                continue

            return response.status_code, {"error": f"HTTP {response.status_code}", "cnpj": cnpj}

        except httpx.RequestError as exc:
            if attempt == max_retries:
                raise
            time.sleep(2 * attempt)

    return 429, {"error": "Rate limit excedido após retries", "cnpj": cnpj}


def upsert_empresa(engine: Engine, cnpj: str, http_status: int, payload: dict[str, Any]) -> None:
    sql = text("""
        INSERT INTO bronze.brasilapi_empresas (cnpj, http_status, raw_payload, extracted_at)
        VALUES (:cnpj, :http_status, CAST(:raw_payload AS JSONB), NOW())
        ON CONFLICT (cnpj) DO UPDATE
        SET http_status = EXCLUDED.http_status,
            raw_payload = EXCLUDED.raw_payload,
            extracted_at = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(
            sql,
            {
                "cnpj": cnpj,
                "http_status": http_status,
                "raw_payload": json.dumps(payload, ensure_ascii=False),
            },
        )


def enrich_fornecedores(batch_limit: int = 150) -> None:
    engine = get_engine()
    ensure_bronze_brasilapi_table(engine)

    pendentes = get_pending_cnpjs(engine, limit=batch_limit)
    run_id, started_at = start_pipeline_run(
        engine,
        pipeline_name="brasilapi_fornecedores_bronze",
        metadata={"batch_limit": batch_limit, "pendentes_encontrados": len(pendentes)},
    )

    if not pendentes:
        logger.info("Nenhum CNPJ novo pendente de enriquecimento.")
        finish_pipeline_run(engine, run_id, started_at, status="SUCCESS", records_extracted=0, records_inserted=0)
        return

    processados = 0
    enriquecidos_200 = 0

    try:
        with httpx.Client(timeout=20.0) as client:
            for idx, cnpj in enumerate(pendentes, start=1):
                status_code, payload = fetch_cnpj(client, cnpj)
                upsert_empresa(engine, cnpj, status_code, payload)
                processados += 1

                if status_code == 200:
                    enriquecidos_200 += 1

                if idx % 25 == 0 or idx == len(pendentes):
                    logger.info(
                        "BrasilAPI progresso: %s/%s CNPJs processados (%s válidos)",
                        idx,
                        len(pendentes),
                        enriquecidos_200,
                    )

                time.sleep(0.35)

        finish_pipeline_run(
            engine,
            run_id=run_id,
            started_at=started_at,
            status="SUCCESS",
            records_extracted=processados,
            records_inserted=enriquecidos_200,
        )
        logger.info("Enriquecimento concluído (run_id=%s): %s/%s CNPJs válidos.", run_id, enriquecidos_200, processados)

    except Exception as exc:
        finish_pipeline_run(
            engine,
            run_id=run_id,
            started_at=started_at,
            status="FAILED",
            records_extracted=processados,
            records_inserted=enriquecidos_200,
            error_message=str(exc),
        )
        logger.exception("Falha no enriquecimento BrasilAPI (run_id=%s).", run_id)
        raise


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Enriquecimento incremental de CNPJs via BrasilAPI.")
    parser.add_argument("--limit", type=int, default=150, help="Máximo de CNPJs novos por execução")
    args = parser.parse_args()

    enrich_fornecedores(batch_limit=args.limit)