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
    dim.d4c                                                 AS classificacao_1_id,
    dim.d4n                                                 AS classificacao_1,
    dim.d5c                                                 AS classificacao_2_id,
    dim.d5n                                                 AS classificacao_2,
    CASE WHEN d.v ~ '^-?[0-9]' THEN d.v::numeric END       AS valor
FROM dados d
JOIN dimensao   dim ON d.dimensao_id   = dim.id
JOIN localidade l   ON d.localidade_id = l.id
WHERE d.sidra_tabela_id IN ('305', '793')
  AND d.ativo = true
