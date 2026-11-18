SELECT DISTINCT ON (orgao_cnpj)
    orgao_cnpj,
    orgao_razao_social,
    CASE orgao_esfera_id
        WHEN 'F' THEN 'Federal'
        WHEN 'E' THEN 'Estadual'
        WHEN 'M' THEN 'Municipal'
        WHEN 'D' THEN 'Distrital'
        ELSE 'Não Informado'
    END AS esfera_nome,
    CASE orgao_poder_id
        WHEN 'E' THEN 'Executivo'
        WHEN 'L' THEN 'Legislativo'
        WHEN 'J' THEN 'Judiciário'
        ELSE 'Não Informado'
    END AS poder_nome
FROM {{ ref('stg_pncp_contratacoes') }}
WHERE orgao_cnpj IS NOT NULL
ORDER BY orgao_cnpj, data_publicacao DESC