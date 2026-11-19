SELECT
    numero_controle_pncp,
    valor_total_estimado,
    valor_total_homologado
FROM {{ ref('fato_contratacoes') }}
WHERE valor_total_estimado < 0
   OR valor_total_homologado < 0