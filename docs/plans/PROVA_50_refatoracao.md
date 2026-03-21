# Relatório de Refatoração — `PROVA_50.py`

Premissas aplicadas:
- Política respeitada: alterações somente nos blocos de solução.
- Fonte primária: `docs/plans/PROVA_50_plano.md`.
- Diretrizes técnicas: `docs/agents/especialista_pyspark.md`.

### Q1
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: bloco já atendia ao enunciado; risco principal era custo, não erro funcional crítico.

### Q2
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: implementação aderente (API + SQL) e sem desvios semânticos.

### Q3
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: método IQR correto e alinhado ao pedido.

### Q4
- Status: otimizado
- Baseado no plano: sim
- Ajustes realizados: removido loop com múltiplos `df.stat.corr`; cálculo consolidado via agregação de pares em uma ação principal.
- Justificativa técnica: reduz actions e recomputações, mantendo matriz e top 3 pares.

### Q5
- Status: otimizado
- Baseado no plano: sim
- Ajustes realizados: substituído loop de `df.agg(...).first()` por agregação consolidada única e montagem do relatório em memória do driver.
- Justificativa técnica: menor custo de varredura e melhor escalabilidade para schema largo.

### Q6
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: normalização de `artists` para artista individual; remoção de coluna extra no output.
- Justificativa técnica: corrige semântica de “respectivo artista” e alinha saída ao enunciado.

### Q7
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: filtro dinâmico já estava correto e idiomático.

### Q8
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: normalização por artista individual; contagem por `countDistinct(id)`; anti-join no eixo correto.
- Justificativa técnica: remove erro semântico identificado no plano.

### Q9
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: regex/classificação atendem ao objetivo pedido.

### Q10
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: cálculo de crescimento com `lag` já estava consistente.

### Q11
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: regras condicionais corretas e claras.

### Q12
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: normalização min-max e score global adequados.

### Q13
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: `ntile(5)` e perfil por bin corretos.

### Q14
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: features e correlação com `popularity` já atendiam.

### Q15
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: já fazia split/explode e comparação collab vs solo.

### Q16
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: normalização fuzzy e agrupamento estavam corretos.

### Q17
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: adicionada validação explícita de domínio da `key` e checagem de consistência da soma do one-hot.
- Justificativa técnica: evita falso sentimento de validade para chaves nulas/inválidas.

### Q18
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: cálculo de z-score migrado para artista individual (explode + window por `artist`).
- Justificativa técnica: corrige semântica por grupo e melhora consistência com demais questões.

### Q19
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: pivot e gap Major/Minor já corretos.

### Q20
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: `rollup` e `cube` corretamente demonstrados.

### Q21
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: `dense_rank` passou a ordenar somente por `popularity` (sem desempate por `name`).
- Justificativa técnica: corrige semântica de empate e comparação com `row_number`.

### Q22
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: variação ano a ano e score simultâneo estavam corretos.

### Q23
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: entropia e tom dominante já aderentes.

### Q24
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: mantida política de participação por artista após explode; ambiguidade do share documentada no plano.

### Q25
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: média móvel e divergência corretas.

### Q26
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: query SQL refatorada para artista individual usando `LATERAL VIEW EXPLODE`.
- Justificativa técnica: elimina ambiguidade de `artists` agregado como string de lista.

### Q27
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: SQL window já atendia ao enunciado.

### Q28
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: critério adotado permaneceu médio por artista; ambiguidade de interpretação preservada.

### Q29
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: one-hit wonders migrado para artista individual (CTE normalizada com explode).
- Justificativa técnica: corrige eixo semântico do agrupamento.

### Q30
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: acumulado alterado para baseline da primeira década via `LAG` + `SUM` de deltas.
- Justificativa técnica: remove distorção de somatório direto de percentuais.

### Q31
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: score composto com `percent_rank` já adequado.

### Q32
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: regra atual foi mantida para preservar comportamento existente; possível refinamento ficou fora do escopo prioritário.

### Q33
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: técnica de gaps/islands correta.

### Q34
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: normalização para artista individual; ordenação temporal estabilizada (`year`, `name`, `id`).
- Justificativa técnica: melhora reprodutibilidade e consistência semântica.

### Q35
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: segmentação por decis e variação percentual adequadas.

### Q36
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: comparação Top 1% vs Bottom 50% correta.

### Q37
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: UDF exigida e funcional.

### Q38
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: UDF `StructType` aderente ao enunciado.

### Q39
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: estatísticas por array e ranking de inconsistência corretos.

### Q40
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: perfil `MapType`/JSON adequado.

### Q41
- Status: otimizado
- Baseado no plano: sim
- Ajustes realizados: benchmark com warm-up e múltiplas execuções; comparação por mediana em vez de single-run.
- Justificativa técnica: melhora robustez da leitura de performance entre Pandas UDF e UDF tradicional.

### Q42
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: pré-bucket + filtros finos já estavam corretos.

### Q43
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: eixo de artista individual; broadcast condicionado por cardinalidade estimada.
- Justificativa técnica: reduz risco de OOM e corrige semântica de `relative_pop` por artista.

### Q44
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: blacklist com critério de robustez (`artist_tracks >= 5`) e exclusão no nível de faixa via `id`.
- Justificativa técnica: reduz viés por outlier e melhora definição de “sem mainstream”.

### Q45
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: escrita com fallback de caminho para Databricks (`dbfs:/tmp/...`) e validação de partições via Spark/Hadoop FS.
- Justificativa técnica: melhora portabilidade e aderência a ambiente distribuído.

### Q46
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: comparativos de plano já estavam adequados.

### Q47
- Status: otimizado
- Baseado no plano: sim
- Ajustes realizados: tie-break determinístico por ordenação de structs de distância; coleta de centróides via `toLocalIterator`.
- Justificativa técnica: reduz não determinismo em empate e diminui pressão no driver.

### Q48
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: critério bilateral por resíduo absoluto foi preservado para manter comportamento.

### Q49
- Status: mantido
- Baseado no plano: sim
- Ajustes realizados: nenhum.
- Justificativa técnica: rede de colaboração já adequada ao enunciado.

### Q50
- Status: corrigido
- Baseado no plano: sim
- Ajustes realizados: top artistas com `countDistinct(id)` e ordenação estável no payload consolidado (rank + `F.sort_array`).
- Justificativa técnica: melhora consistência de saída e robustez da métrica de artista.

## Ambiguidades e decisões documentadas
- Q24: mantida política de share por participação de artista (explode), pois o enunciado permite interpretação; decisão preservada.
- Q28: mantido critério de “artista mais popular” por média no contexto existente; sem mudança para evitar alteração arbitrária de regra.
- Q32: algoritmo atual de sessão foi preservado para não alterar a lógica funcional já implementada.
- Q48: critério bilateral de anomalia (`|resíduo| > 2*std`) mantido.
