-- A Lei 14.133 (PNCP) foi promulgada em abril de 2021; datas anteriores ou futuras indicam erro na fonte
SELECT
    numero_controle_pncp,
    data_publicacao
FROM {{ ref('fato_contratacoes') }}
WHERE data_publicacao < DATE '2021-04-01'
   OR data_publicacao > CURRENT_DATE + INTERVAL '1 day'