from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "b2g_data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="b2g_market_radar_etl",
    description="Pipeline end-to-end PNCP + BrasilAPI + dbt Core (Medallion Architecture)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 11, 1),
    schedule="0 6 * * *",  # Todos os dias às 06:00
    catchup=False,
    max_active_runs=1,
    tags=["b2g", "pncp", "brasilapi", "dbt", "medallion"],
) as dag:

    extract_contratacoes = BashOperator(
        task_id="extract_pncp_contratacoes",
        bash_command="cd /opt/airflow/project && python -m ingestion.pncp_contratacoes",
    )

    extract_contratos = BashOperator(
        task_id="extract_pncp_contratos",
        bash_command="cd /opt/airflow/project && python -m ingestion.pncp_contratos --max-paginas 20",
    )

    enrich_fornecedores = BashOperator(
        task_id="enrich_brasilapi_fornecedores",
        bash_command="cd /opt/airflow/project && python -m ingestion.brasilapi_cnpj --limit 100",
    )

    dbt_transform = BashOperator(
        task_id="dbt_run_silver_gold",
        bash_command="cd /opt/airflow/project/transformation && dbt run --profiles-dir .",
    )

    dbt_quality_gate = BashOperator(
        task_id="dbt_test_quality_gate",
        bash_command="cd /opt/airflow/project/transformation && dbt test --profiles-dir .",
    )

    # Encadeamento visual da DAG
    extract_contratacoes >> extract_contratos >> enrich_fornecedores >> dbt_transform >> dbt_quality_gate