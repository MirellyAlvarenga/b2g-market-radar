SELECT
    numero_controle_pncp,
    numero_controle_compra,
    orgao_cnpj,
    COALESCE(NULLIF(fornecedor_documento, ''), 'NAO_INFORMADO') AS fornecedor_documento,
    ano_contrato,
    numero_contrato,
    objeto_contrato,
    CAST(data_publicacao AS DATE) AS data_publicacao,
    data_assinatura,
    data_vigencia_inicio,
    data_vigencia_fim,
    CASE
        WHEN data_vigencia_inicio IS NOT NULL AND data_vigencia_fim IS NOT NULL
            THEN (data_vigencia_fim - data_vigencia_inicio)
        ELSE NULL
    END AS prazo_vigencia_dias,
    valor_inicial,
    valor_global,
    valor_parcela,
    numero_parcelas
FROM {{ ref('stg_pncp_contratos') }}