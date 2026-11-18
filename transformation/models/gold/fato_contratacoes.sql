SELECT
    numero_controle_pncp,

    -- Chaves Estrangeiras para as Dimensões
    orgao_cnpj,
    COALESCE(NULLIF(TRIM(codigo_ibge), ''), 'NAO_INFORMADO') AS codigo_ibge,
    codigo_modalidade,

    -- Atributos da Contratação
    ano_compra,
    numero_compra,
    objeto_compra,
    modo_disputa_nome,
    situacao_compra_nome,
    is_registro_precos,

    -- Datas (formato DATE facilita relacionar com a dCalendario no Power BI)
    CAST(data_publicacao AS DATE) AS data_publicacao,
    CAST(data_abertura_proposta AS DATE) AS data_abertura_proposta,
    CAST(data_encerramento_proposta AS DATE) AS data_encerramento_proposta,

    -- Métricas Financeiras e Inteligência de Pricing
    valor_total_estimado,
    valor_total_homologado,
    CASE
        WHEN valor_total_homologado IS NOT NULL AND valor_total_homologado > 0
            THEN 'Homologado'
        ELSE 'Em Andamento / Não Homologado'
    END AS status_homologacao,
    CASE
        WHEN valor_total_homologado IS NOT NULL
             AND valor_total_homologado > 0
             AND valor_total_estimado > 0
            THEN valor_total_estimado - valor_total_homologado
        ELSE NULL
    END AS valor_economia,
    CASE
        WHEN valor_total_homologado IS NOT NULL
             AND valor_total_homologado > 0
             AND valor_total_estimado > 0
            THEN ROUND(((valor_total_estimado - valor_total_homologado) / valor_total_estimado) * 100, 2)
        ELSE NULL
    END AS taxa_desagio_pct
FROM {{ ref('stg_pncp_contratacoes') }}