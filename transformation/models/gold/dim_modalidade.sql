SELECT DISTINCT ON (codigo_modalidade)
    codigo_modalidade,
    COALESCE(modalidade_nome, 'Não Informado') AS modalidade_nome
FROM {{ ref('stg_pncp_contratacoes') }}
WHERE codigo_modalidade IS NOT NULL
ORDER BY codigo_modalidade, data_publicacao DESC