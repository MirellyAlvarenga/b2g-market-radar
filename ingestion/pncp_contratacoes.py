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

# Garante importação correta mesmo se executado fora de `python -m`
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

try:
    from dotenv import load_dotenv

    load_dotenv(ROOT_DIR / ".env")
except ImportError:
    pass

from ingestion.audit import finish_pipeline_run, get_last_watermark, start_pipeline_run

logger = logging.getLogger(__name__)

PNCP_BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

MODALIDADES_ALVO: dict[int, str] = {
    6: "Pregão - Eletrônico",
    8: "Dispensa",
}


def get_engine() -> Engine:
    """Cria a engine SQLAlchemy conectada ao PostgreSQL do container Docker."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    db = os.getenv("POSTGRES_DB", "b2g_market_radar")
    user = os.getenv("POSTGRES_USER", "b2g_admin")
    password = os.getenv("POSTGRES_PASSWORD", "b2g_secret_password")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ensure_bronze_table(engine: Engine) -> None:
    """Garante a existência do schema bronze e da tabela bruta de contratações."""
    ddl = """
    CREATE SCHEMA IF NOT EXISTS bronze;

    CREATE TABLE IF NOT EXISTS bronze.pncp_contratacoes (
        id BIGSERIAL PRIMARY KEY,
        numero_controle_pncp VARCHAR(100) NOT NULL,
        codigo_modalidade INTEGER NOT NULL,
        data_publicacao_pncp TIMESTAMPTZ,
        raw_payload JSONB NOT NULL,
        extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_bronze_pncp_controle
        ON bronze.pncp_contratacoes (numero_controle_pncp);

    CREATE INDEX IF NOT EXISTS idx_bronze_pncp_extracted_at
        ON bronze.pncp_contratacoes (extracted_at);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def fetch_page_with_retry(
    client: httpx.Client,
    params: dict[str, Any],
    max_retries: int = 5,
) -> dict[str, Any] | None:
    """Consulta uma página da API do PNCP com exponential backoff para 429 e 5xx."""
    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(PNCP_BASE_URL, params=params)

            if response.status_code == 200:
                return response.json()

            if response.status_code == 204:
                return None

            if response.status_code in (429, 500, 502, 503, 504):
                wait_seconds = min(2**attempt, 30)
                logger.warning(
                    "HTTP %s na página %s (tentativa %s/%s). Aguardando %ss...",
                    response.status_code,
                    params.get("pagina"),
                    attempt,
                    max_retries,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

        except httpx.RequestError as exc:
            if attempt == max_retries:
                raise
            wait_seconds = min(2**attempt, 30)
            logger.warning(
                "Erro de rede (%s) na tentativa %s/%s. Aguardando %ss...",
                exc,
                attempt,
                max_retries,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise RuntimeError(f"Falha após {max_retries} tentativas para params={params}")


def save_batch(
    engine: Engine,
    items: list[dict[str, Any]],
    codigo_modalidade: int,
) -> int:
    """Persiste um lote de registros brutos (JSONB) na camada bronze."""
    if not items:
        return 0

    rows = []
    for item in items:
        numero_controle = item.get("numeroControlePNCP")
        if not numero_controle:
            continue

        rows.append(
            {
                "numero_controle_pncp": numero_controle,
                "codigo_modalidade": codigo_modalidade,
                "data_publicacao_pncp": item.get("dataPublicacaoPncp"),
                "raw_payload": json.dumps(item, ensure_ascii=False),
            }
        )

    if not rows:
        return 0

    sql = text("""
        INSERT INTO bronze.pncp_contratacoes (
            numero_controle_pncp,
            codigo_modalidade,
            data_publicacao_pncp,
            raw_payload
        )
        VALUES (
            :numero_controle_pncp,
            :codigo_modalidade,
            CAST(:data_publicacao_pncp AS TIMESTAMPTZ),
            CAST(:raw_payload AS JSONB)
        )
    """)

    with engine.begin() as conn:
        conn.execute(sql, rows)

    return len(rows)


def ingest_contratacoes(
    data_inicial: str,
    data_final: str,
    modalidades: dict[int, str] | None = None,
    tamanho_pagina: int = 50,
) -> None:
    """Orquestra a extração paginada por modalidade com registro de auditoria."""
    engine = get_engine()
    ensure_bronze_table(engine)

    target_modalidades = modalidades or MODALIDADES_ALVO
    run_id, started_at = start_pipeline_run(
        engine,
        pipeline_name="pncp_contratacoes_bronze",
        metadata={
            "data_inicial": data_inicial,
            "data_final": data_final,
            "modalidades": list(target_modalidades.keys()),
        },
    )

    total_extraido = 0
    total_inserido = 0

    try:
        with httpx.Client(timeout=30.0) as client:
            for cod_mod, nome_mod in target_modalidades.items():
                pagina = 1
                total_modalidade = 0
                logger.info(
                    "Iniciando extração: [%s - %s] (%s a %s)",
                    cod_mod,
                    nome_mod,
                    data_inicial,
                    data_final,
                )

                while True:
                    params = {
                        "dataInicial": data_inicial,
                        "dataFinal": data_final,
                        "codigoModalidadeContratacao": cod_mod,
                        "pagina": pagina,
                        "tamanhoPagina": tamanho_pagina,
                    }

                    payload = fetch_page_with_retry(client, params)
                    if not payload:
                        logger.info("Sem mais registros (HTTP 204) na página %s.", pagina)
                        break

                    items = payload.get("data", [])
                    if not items:
                        break

                    total_extraido += len(items)
                    inserted = save_batch(engine, items, cod_mod)
                    total_modalidade += inserted
                    paginas_restantes = payload.get("paginasRestantes", 0)

                    logger.info(
                        "Modalidade %s | Página %s salva (%s registros) | Páginas restantes: %s",
                        cod_mod,
                        pagina,
                        inserted,
                        paginas_restantes,
                    )

                    if paginas_restantes <= 0:
                        break

                    pagina += 1
                    time.sleep(0.3)

                logger.info("Concluído [%s]: %s registros carregados.", nome_mod, total_modalidade)
                total_inserido += total_modalidade

        finish_pipeline_run(
            engine,
            run_id=run_id,
            started_at=started_at,
            status="SUCCESS",
            records_extracted=total_extraido,
            records_inserted=total_inserido,
        )
        logger.info("Ingestão finalizada (run_id=%s). Total inserido: %s.", run_id, total_inserido)

    except Exception as exc:
        finish_pipeline_run(
            engine,
            run_id=run_id,
            started_at=started_at,
            status="FAILED",
            records_extracted=total_extraido,
            records_inserted=total_inserido,
            error_message=str(exc),
        )
        logger.exception("Falha crítica na ingestão (run_id=%s).", run_id)
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extrator incremental de contratações do PNCP.")
    parser.add_argument("--data-inicial", type=str, default=None, help="Data inicial no formato YYYYMMDD")
    parser.add_argument("--data-final", type=str, default=None, help="Data final no formato YYYYMMDD")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    args = parse_args()
    engine = get_engine()

    if args.data_inicial and args.data_final:
        dt_ini, dt_fim = args.data_inicial, args.data_final
        logger.info("Executando em modo MANUAL (backfill): %s até %s", dt_ini, dt_fim)
    else:
        dt_ini, dt_fim = get_last_watermark(engine, pipeline_name="pncp_contratacoes_bronze")
        logger.info("Executando em modo INCREMENTAL (watermark): %s até %s", dt_ini, dt_fim)

    ingest_contratacoes(data_inicial=dt_ini, data_final=dt_fim)