WITH fornecedores_unicos AS (
    SELECT DISTINCT ON (fornecedor_documento)
        COALESCE(NULLIF(fornecedor_documento, ''), 'NAO_INFORMADO') AS fornecedor_documento,
        COALESCE(NULLIF(fornecedor_razao_social, ''), 'Não Informado') AS razao_social_pncp,
        fornecedor_tipo_pessoa
    FROM {{ ref('stg_pncp_contratos') }}
    ORDER BY fornecedor_documento, data_publicacao DESC
)

SELECT
    f.fornecedor_documento,
    COALESCE(b.razao_social, f.razao_social_pncp) AS fornecedor_razao_social,
    b.nome_fantasia,
    f.fornecedor_tipo_pessoa,
    CASE
        WHEN f.fornecedor_tipo_pessoa = 'PF' OR LENGTH(f.fornecedor_documento) = 11 THEN 'Pessoa Física'
        WHEN b.porte_empresa IS NOT NULL THEN b.porte_empresa
        ELSE 'Pendente Enriquecimento'
    END AS porte_empresa,
    COALESCE(b.cnae_codigo, 'N/A') AS cnae_codigo,
    COALESCE(b.cnae_descricao, 'Não Classificado') AS cnae_descricao,
    COALESCE(b.uf_sede, 'NI') AS uf_sede,
    COALESCE(b.municipio_sede, 'Não Informado') AS municipio_sede,
    COALESCE(b.capital_social, 0) AS capital_social,
    (b.cnpj IS NOT NULL) AS is_enriquecido_brasilapi
FROM fornecedores_unicos f
LEFT JOIN {{ ref('stg_brasilapi_empresas') }} b
    ON f.fornecedor_documento = b.cnpj