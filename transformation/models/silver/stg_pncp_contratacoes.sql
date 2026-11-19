{{
    config(
        materialized='incremental',
        unique_key='numero_controle_pncp',
        incremental_strategy='delete+insert'
    )
}}

WITH raw_deduplicated AS (
    SELECT DISTINCT ON (numero_controle_pncp)
        id AS bronze_id,
        numero_controle_pncp,
        codigo_modalidade,
        raw_payload,
        extracted_at
    FROM {{ source('bronze', 'pncp_contratacoes') }}
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
    (raw_payload ->> 'anoCompra')::INTEGER AS ano_compra,
    (raw_payload ->> 'sequencialCompra')::INTEGER AS sequencial_compra,
    raw_payload ->> 'numeroCompra' AS numero_compra,
    TRIM(raw_payload ->> 'objetoCompra') AS objeto_compra,

    -- Modalidade, modo de disputa e situação
    codigo_modalidade,
    raw_payload ->> 'modalidadeNome' AS modalidade_nome,
    (raw_payload ->> 'modoDisputaId')::INTEGER AS modo_disputa_id,
    raw_payload ->> 'modoDisputaNome' AS modo_disputa_nome,
    (raw_payload ->> 'situacaoCompraId')::INTEGER AS situacao_compra_id,
    raw_payload ->> 'situacaoCompraNome' AS situacao_compra_nome,
    COALESCE((raw_payload ->> 'srp')::BOOLEAN, FALSE) AS is_registro_precos,

    -- Dados do Órgão Comprador
    raw_payload -> 'orgaoEntidade' ->> 'cnpj' AS orgao_cnpj,
    TRIM(raw_payload -> 'orgaoEntidade' ->> 'razaoSocial') AS orgao_razao_social,
    raw_payload -> 'orgaoEntidade' ->> 'poderId' AS orgao_poder_id,
    raw_payload -> 'orgaoEntidade' ->> 'esferaId' AS orgao_esfera_id,

    -- Localidade da Unidade Compradora
    raw_payload -> 'unidadeOrgao' ->> 'codigoUnidade' AS unidade_codigo,
    TRIM(raw_payload -> 'unidadeOrgao' ->> 'nomeUnidade') AS unidade_nome,
    UPPER(TRIM(raw_payload -> 'unidadeOrgao' ->> 'ufSigla')) AS uf_sigla,
    TRIM(raw_payload -> 'unidadeOrgao' ->> 'municipioNome') AS municipio_nome,
    raw_payload -> 'unidadeOrgao' ->> 'codigoIbge' AS codigo_ibge,

    -- Métricas Financeiras
    COALESCE(NULLIF(raw_payload ->> 'valorTotalEstimado', '')::NUMERIC(18, 2), 0) AS valor_total_estimado,
    NULLIF(raw_payload ->> 'valorTotalHomologado', '')::NUMERIC(18, 2) AS valor_total_homologado,

    -- Datas
    NULLIF(raw_payload ->> 'dataPublicacaoPncp', '')::TIMESTAMPTZ AS data_publicacao,
    NULLIF(raw_payload ->> 'dataAberturaProposta', '')::TIMESTAMPTZ AS data_abertura_proposta,
    NULLIF(raw_payload ->> 'dataEncerramentoProposta', '')::TIMESTAMPTZ AS data_encerramento_proposta,
    extracted_at
FROM raw_deduplicated