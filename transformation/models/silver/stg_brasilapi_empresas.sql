SELECT
    cnpj,
    TRIM(raw_payload ->> 'razao_social') AS razao_social,
    NULLIF(TRIM(raw_payload ->> 'nome_fantasia'), '') AS nome_fantasia,
    (raw_payload ->> 'cnae_fiscal')::VARCHAR(20) AS cnae_codigo,
    TRIM(raw_payload ->> 'cnae_fiscal_descricao') AS cnae_descricao,
    COALESCE(NULLIF(TRIM(raw_payload ->> 'porte'), ''), 'NÃO INFORMADO') AS porte_empresa,
    UPPER(TRIM(raw_payload ->> 'uf')) AS uf_sede,
    TRIM(raw_payload ->> 'municipio') AS municipio_sede,
    TRIM(raw_payload ->> 'descricao_situacao_cadastral') AS situacao_cadastral,
    NULLIF(raw_payload ->> 'data_inicio_atividade', '')::DATE AS data_inicio_atividade,
    COALESCE(NULLIF(raw_payload ->> 'capital_social', '')::NUMERIC(18, 2), 0) AS capital_social,
    extracted_at
FROM {{ source('bronze', 'brasilapi_empresas') }}
WHERE http_status = 200