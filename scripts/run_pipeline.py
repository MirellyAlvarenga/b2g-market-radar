import logging
import subprocess
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from ingestion.audit import get_last_watermark
from ingestion.brasilapi_cnpj import enrich_fornecedores
from ingestion.pncp_contratacoes import get_engine, ingest_contratacoes
from ingestion.pncp_contratos import ingest_contratos

logger = logging.getLogger("b2g_pipeline")


def run_dbt_step(command: list[str]) -> None:
    transformation_dir = ROOT_DIR / "transformation"
    logger.info("Executando dbt: %s", " ".join(command))
    subprocess.run(command, cwd=transformation_dir, check=True)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    engine = get_engine()

    # 1. Ingestão incremental de contratações (editais)
    dt_ini_comp, dt_fim_comp = get_last_watermark(engine, pipeline_name="pncp_contratacoes_bronze")
    logger.info("[1/5] Extraindo contratações PNCP (%s a %s)...", dt_ini_comp, dt_fim_comp)
    ingest_contratacoes(data_inicial=dt_ini_comp, data_final=dt_fim_comp)

    # 2. Ingestão incremental de contratos assinados
    dt_ini_cont, dt_fim_cont = get_last_watermark(engine, pipeline_name="pncp_contratos_bronze")
    logger.info("[2/5] Extraindo contratos PNCP (%s a %s)...", dt_ini_cont, dt_fim_cont)
    ingest_contratos(data_inicial=dt_ini_cont, data_final=dt_fim_cont, max_paginas=20)

    # 3. Enriquecimento incremental de fornecedores via BrasilAPI
    logger.info("[3/5] Enriquecendo fornecedores pendentes via BrasilAPI...")
    enrich_fornecedores(batch_limit=100)

    # 4. Transformação Silver + Gold (dbt run)
    logger.info("[4/5] Executando modelos dimensionais no dbt...")
    run_dbt_step(["dbt", "run", "--profiles-dir", "."])

    # 5. Testes de qualidade e integridade (dbt test)
    logger.info("[5/5] Validando testes de qualidade no dbt...")
    run_dbt_step(["dbt", "test", "--profiles-dir", "."])

    logger.info("Pipeline end-to-end concluído com sucesso!")


if __name__ == "__main__":
    main()