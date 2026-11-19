{{
    config(
        materialized='incremental',
        unique_key='numero_controle_pncp',
        incremental_strategy='delete+insert'
    )
}}

WITH raw_deduplicated AS (
    SELECT DISTINCT ON (numero_controle_pncp)
        numero_controle_pncp,
        numero_controle_compra,
        raw_payload,
        extracted_at
    FROM {{ source('bronze', 'pncp_contratos') }}
    WHERE numero_controle_pncp IS NOT NULL
    {% if is_incremental() %}
        AND extracted_at > (
            SELECT COALESCE(MAX(extracted_at), '1900-01-01'::TIMESTAMPTZ)
            FROM {{ this }}
        )
    {% endif %}
    ORDER BY numero_controle_pncp, extracted_at DESC
)

SELECT
    numero_controle_pncp,
    NULLIF(TRIM(numero_controle_compra), '') AS numero_controle_compra,
    (raw_payload ->> 'anoContrato')::INTEGER AS ano_contrato,
    (raw_payload ->> 'sequencialContrato')::INTEGER AS sequencial_contrato,
    raw_payload ->> 'numeroContratoEmpenho' AS numero_contrato,
    TRIM(raw_payload ->> 'objetoContrato') AS objeto_contrato,

    -- Órgão Contratante
    REGEXP_REPLACE(COALESCE(raw_payload -> 'orgaoEntidade' ->> 'cnpj', ''), '[^0-9]', '', 'g') AS orgao_cnpj,
    TRIM(raw_payload -> 'orgaoEntidade' ->> 'razaoSocial') AS orgao_razao_social,

    -- Fornecedor Vencedor
    REGEXP_REPLACE(COALESCE(raw_payload ->> 'niFornecedor', ''), '[^0-9]', '', 'g') AS fornecedor_documento,
    TRIM(raw_payload ->> 'nomeRazaoSocialFornecedor') AS fornecedor_razao_social,
    COALESCE(raw_payload ->> 'tipoPessoa', 'PJ') AS fornecedor_tipo_pessoa,

    -- Valores Financeiros
    COALESCE(NULLIF(raw_payload ->> 'valorInicial', '')::NUMERIC(18, 2), 0) AS valor_inicial,
    COALESCE(NULLIF(raw_payload ->> 'valorGlobal', '')::NUMERIC(18, 2), 0) AS valor_global,
    NULLIF(raw_payload ->> 'valorParcela', '')::NUMERIC(18, 2) AS valor_parcela,
    (raw_payload ->> 'numeroParcelas')::INTEGER AS numero_parcelas,

    -- Datas
    NULLIF(raw_payload ->> 'dataPublicacaoPncp', '')::TIMESTAMPTZ AS data_publicacao,
    NULLIF(raw_payload ->> 'dataAssinatura', '')::DATE AS data_assinatura,
    NULLIF(raw_payload ->> 'dataVigenciaInicio', '')::DATE AS data_vigencia_inicio,
    NULLIF(raw_payload ->> 'dataVigenciaFim', '')::DATE AS data_vigencia_fim,
    extracted_at
FROM raw_deduplicated