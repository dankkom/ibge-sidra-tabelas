SELECT
    d.d3c                                                   AS periodo,
    l.nc                                                    AS nivel_territorial_id,
    l.nn                                                    AS nivel_territorial,
    l.d1c                                                   AS localidade_id,
    l.d1n                                                   AS localidade,
    dim.d2c                                                 AS variavel_id,
    dim.d2n                                                 AS variavel,
    dim.mc                                                  AS unidade_id,
    dim.mn                                                  AS unidade,
    dim.d4c                                                 AS produto_id,
    dim.d4n                                                 AS produto,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('839', '1000', '1001', '1002', '1612')
  AND d.ativo = true
