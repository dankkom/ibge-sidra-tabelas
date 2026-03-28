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
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id = '6579'
  AND d.ativo = true
