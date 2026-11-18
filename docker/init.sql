-- Criação dos schemas da Arquitetura Medallion e Auditoria
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- Documentação no catálogo do banco
COMMENT ON SCHEMA bronze IS 'Dados brutos extraídos das APIs (PNCP, IBGE, BrasilAPI) sem transformação.';
COMMENT ON SCHEMA silver IS 'Dados limpos, tipados, deduplicados e enriquecidos via dbt.';
COMMENT ON SCHEMA gold IS 'Modelagem dimensional (Star Schema: Fatos e Dimensões) para consumo no Power BI.';
COMMENT ON SCHEMA audit IS 'Logs de execução e métricas de observabilidade/qualidade de dados.';