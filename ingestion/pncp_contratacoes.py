import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.audit import finish_pipeline_run, start_pipeline_run

from ingestion.db import get_engine

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

# Principais modalidades do PNCP para inteligência comercial
MODALIDADES_ALVO = {
    6: "Pregão - Eletrônico",
    8: "Dispensa de Licitação",
}


def ensure_bronze_table(engine: Engine) -> None:
    """Cria a tabela bruta no schema bronze caso ainda não exista."""
    ddl = """
    CREATE TABLE IF NOT EXISTS bronze.pncp_contratacoes (
        id BIGSERIAL PRIMARY KEY,
        numero_controle_pncp VARCHAR(100),
        codigo_modalidade INTEGER NOT NULL,
        data_publicacao_pncp VARCHAR(30),
        raw_payload JSONB NOT NULL,
        extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_bronze_pncp_controle
        ON bronze.pncp_contratacoes (numero_controle_pncp);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def fetch_page_with_retry(
    client: httpx.Client,
    params: dict[str, Any],
    max_retries: int = 4,
) -> dict[str, Any] | None:
    """Consulta uma página da API com backoff exponencial para rate limit (429) e instabilidades (5xx)."""
    for attempt in range(1, max_retries + 1):
        try:
            response = client.get(BASE_URL, params=params)

            # PNCP retorna 204 No Content quando não existem registros para o filtro/página
            if response.status_code == 204:
                return None

            if response.status_code == 429 or response.status_code >= 500:
                wait_seconds = 2**attempt
                logger.warning(
                    "Status %s na página %s. Tentativa %s/%s. Aguardando %ss...",
                    response.status_code,
                    params["pagina"],
                    attempt,
                    max_retries,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue

            response.raise_for_status()
            return response.json()

        except httpx.RequestError as exc:
            wait_seconds = 2**attempt
            logger.warning(
                "Erro de rede (%s). Tentativa %s/%s. Aguardando %ss...",
                exc,
                attempt,
                max_retries,
                wait_seconds,
            )
            time.sleep(wait_seconds)

    raise RuntimeError(
        f"Falha ao consultar PNCP após {max_retries} tentativas: {params}"
    )


def save_batch(
    engine: Engine, items: list[dict[str, Any]], codigo_modalidade: int
) -> int:
    """Insere um lote de registros brutos na tabela bronze.pncp_contratacoes."""
    if not items:
        return 0

    extracted_at = datetime.now(timezone.utc)
    rows = [
        {
            "numero_controle_pncp": item.get("numeroControlePNCP"),
            "codigo_modalidade": codigo_modalidade,
            "data_publicacao_pncp": item.get("dataPublicacaoPncp"),
            "raw_payload": json.dumps(item, ensure_ascii=False),
            "extracted_at": extracted_at,
        }
        for item in items
    ]

    insert_sql = text("""
        INSERT INTO bronze.pncp_contratacoes (
            numero_controle_pncp,
            codigo_modalidade,
            data_publicacao_pncp,
            raw_payload,
            extracted_at
        )
        VALUES (
            :numero_controle_pncp,
            :codigo_modalidade,
            :data_publicacao_pncp,
            CAST(:raw_payload AS JSONB),
            :extracted_at
        )
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, rows)

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
                        logger.info(
                            "Sem mais registros (HTTP 204) na página %s.", pagina
                        )
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

                logger.info(
                    "Concluído [%s]: %s registros carregados.",
                    nome_mod,
                    total_modalidade,
                )
                total_inserido += total_modalidade

        finish_pipeline_run(
            engine,
            run_id=run_id,
            started_at=started_at,
            status="SUCCESS",
            records_extracted=total_extraido,
            records_inserted=total_inserido,
        )
        logger.info(
            "Ingestão finalizada (run_id=%s). Total inserido: %s.",
            run_id,
            total_inserido,
        )

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


if __name__ == "__main__":
    # Janela curta de 3 dias para validação rápida do MVP
    ingest_contratacoes(
        data_inicial="20261103",
        data_final="20261105",
    )
