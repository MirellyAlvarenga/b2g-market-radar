WITH localidades_normalizadas AS (
    SELECT
        COALESCE(NULLIF(TRIM(codigo_ibge), ''), 'NAO_INFORMADO') AS codigo_ibge,
        COALESCE(NULLIF(TRIM(municipio_nome), ''), 'Não Informado') AS municipio_nome,
        COALESCE(NULLIF(TRIM(uf_sigla), ''), 'NI') AS uf_sigla,
        data_publicacao
    FROM {{ ref('stg_pncp_contratacoes') }}
)

SELECT DISTINCT ON (codigo_ibge)
    codigo_ibge,
    municipio_nome,
    uf_sigla,
    CASE
        WHEN uf_sigla IN ('SP', 'RJ', 'MG', 'ES') THEN 'Sudeste'
        WHEN uf_sigla IN ('PR', 'SC', 'RS') THEN 'Sul'
        WHEN uf_sigla IN ('DF', 'GO', 'MT', 'MS') THEN 'Centro-Oeste'
        WHEN uf_sigla IN ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Nordeste'
        WHEN uf_sigla IN ('AM', 'PA', 'AC', 'RO', 'RR', 'AP', 'TO') THEN 'Norte'
        ELSE 'Não Informado'
    END AS regiao_nome
FROM localidades_normalizadas
ORDER BY codigo_ibge, data_publicacao DESC