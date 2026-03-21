# Relatório de Debug — `PROVA_50.py`

Contexto de validação:
- Debug orientado por `PROVA_50_refatoracao.md`, `especialista_pyspark.md`, `politica.md` e `databricks_skill.md`.
- Escopo desta rodada: análise estática + correções em código (sem execução fim-a-fim no cluster Databricks neste ambiente).

## Q1
- Tipo de erro: performance (potencial)
- Sintoma observado: múltiplas ações por coluna possíveis, mas sem falha evidente.
- Causa raiz: desenho orientado a robustez de cast, com validações por coluna.
- Correção aplicada: nenhuma (mantido).
- Baseado em: refatoração? não | especialista? sim

## Q2
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma (mantido).
- Baseado em: refatoração? não | especialista? sim

## Q3
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q4
- Tipo de erro: performance
- Sintoma observado: risco de várias ações no cálculo de correlação.
- Causa raiz: abordagem anterior N² com chamadas repetidas.
- Correção aplicada: cálculo consolidado de correlações em agregação única.
- Baseado em: refatoração? sim | especialista? sim

## Q5
- Tipo de erro: performance
- Sintoma observado: múltiplos scans por coluna para relatório de qualidade.
- Causa raiz: loop com `agg().first()` por coluna.
- Correção aplicada: agregação consolidada e montagem do relatório via dicionário de métricas.
- Baseado em: refatoração? sim | especialista? sim

## Q6
- Tipo de erro: lógico
- Sintoma observado: saída incluía coluna extra e média por `artists` bruto.
- Causa raiz: granularidade de artista inconsistente.
- Correção aplicada: normalização para artista individual e saída aderente ao enunciado.
- Baseado em: refatoração? sim | especialista? sim

## Q7
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q8
- Tipo de erro: lógico
- Sintoma observado: classificação de artistas prolíficos incorreta no eixo `artists` bruto.
- Causa raiz: ausência de normalização da lista de artistas.
- Correção aplicada: explode de artistas + anti-join por artista individual.
- Baseado em: refatoração? sim | especialista? sim

## Q9
- Tipo de erro: não identificado
- Sintoma observado: sem erro crítico.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q10
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q11
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q12
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q13
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q14
- Tipo de erro: não identificado
- Sintoma observado: sem erro de API ou execução observado.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q15
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q16
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q17
- Tipo de erro: lógico
- Sintoma observado: validação do one-hot não distinguia `key` inválida.
- Causa raiz: ausência de flag de domínio.
- Correção aplicada: adicionada validação de domínio (`is_valid_key`) e checks separados.
- Baseado em: refatoração? sim | especialista? sim

## Q18
- Tipo de erro: lógico
- Sintoma observado: z-score por artista potencialmente distorcido.
- Causa raiz: particionamento por `artists` bruto.
- Correção aplicada: explode/normalização de artista e window por artista individual.
- Baseado em: refatoração? sim | especialista? sim

## Q19
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q20
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q21
- Tipo de erro: lógico
- Sintoma observado: `dense_rank` não representava corretamente empates.
- Causa raiz: ordenação secundária por `name` na mesma janela do `dense_rank`.
- Correção aplicada: janela específica de `dense_rank` apenas por `popularity`.
- Baseado em: refatoração? sim | especialista? sim

## Q22
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q23
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q24
- Tipo de erro: lógico (ambiguidade de definição)
- Sintoma observado: share/HHI pode variar conforme política de contagem em colaboração.
- Causa raiz: enunciado permite interpretação por participação ou por música.
- Correção aplicada: nenhuma; política atual preservada para consistência da versão refatorada.
- Baseado em: refatoração? sim | especialista? sim

## Q25
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q26
- Tipo de erro: lógico / API misuse
- Sintoma observado: análise por artista era feita no campo bruto.
- Causa raiz: ausência de normalização em SQL.
- Correção aplicada: CTE `normalized` com `LATERAL VIEW EXPLODE`.
- Baseado em: refatoração? sim | especialista? sim

## Q27
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q28
- Tipo de erro: lógico (ambiguidade)
- Sintoma observado: “artista mais popular” admite múltiplos critérios.
- Causa raiz: critério não totalmente fechado no enunciado.
- Correção aplicada: nenhuma; mantida estratégia existente para não alterar contrato funcional.
- Baseado em: refatoração? sim | especialista? sim

## Q29
- Tipo de erro: lógico / API misuse
- Sintoma observado: one-hit wonders agregados por `artists` bruto.
- Causa raiz: ausência de explode na query SQL.
- Correção aplicada: CTE normalizada por artista individual e ajustes de joins/agregações.
- Baseado em: refatoração? sim | especialista? sim

## Q30
- Tipo de erro: lógico
- Sintoma observado: acumulado percentual era soma direta de percentuais.
- Causa raiz: modelagem acumulada sem baseline explícito.
- Correção aplicada: acumulado via `SUM` de deltas com baseline da primeira década (`FIRST_VALUE`).
- Baseado em: refatoração? sim | especialista? sim

## Q31
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q32
- Tipo de erro: lógico (potencial de fronteira)
- Sintoma observado: comportamento em limites de sessão pode variar por interpretação.
- Causa raiz: regra de sessão não 100% inequívoca no enunciado.
- Correção aplicada: nenhuma; mantido para preservar resultado da versão refatorada.
- Baseado em: refatoração? sim | especialista? sim

## Q33
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q34
- Tipo de erro: lógico
- Sintoma observado: momentum por artista com granularidade inconsistente.
- Causa raiz: uso de `artists` bruto e ordenação menos estável.
- Correção aplicada: artista individual + ordenação estável (`year`, `name`, `id`).
- Baseado em: refatoração? sim | especialista? sim

## Q35
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q36
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q37
- Tipo de erro: não identificado
- Sintoma observado: sem erro de execução observado.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q38
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q39
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q40
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q41
- Tipo de erro: performance / API misuse
- Sintoma observado: benchmark single-run pouco confiável.
- Causa raiz: ausência de warm-up e repetição.
- Correção aplicada: benchmark com múltiplas rodadas e mediana.
- Baseado em: refatoração? sim | especialista? sim

## Q42
- Tipo de erro: não identificado
- Sintoma observado: sem erro de execução estático.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q43
- Tipo de erro: lógico / performance
- Sintoma observado: risco de broadcast inadequado e semântica por artista inconsistente.
- Causa raiz: join no campo bruto + hint fixo de broadcast.
- Correção aplicada: normalização por artista e broadcast condicional por cardinalidade.
- Baseado em: refatoração? sim | especialista? sim

## Q44
- Tipo de erro: lógico
- Sintoma observado: blacklist sensível a outliers de baixa amostra.
- Causa raiz: ausência de critério mínimo de faixas por artista.
- Correção aplicada: mínimo de faixas e exclusão por `id` no nível de música.
- Baseado em: refatoração? sim | especialista? sim

## Q45
- Tipo de erro: execução / dependência de ambiente
- Sintoma observado: validação de pastas dependia de FS local.
- Causa raiz: uso de inspeção local em cenário distribuído.
- Correção aplicada: fallback de path Databricks e inspeção via Spark/Hadoop FS.
- Baseado em: refatoração? sim | especialista? sim

## Q46
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q47
- Tipo de erro: lógico / performance
- Sintoma observado: tie-break por igualdade de float e coleta agressiva no driver.
- Causa raiz: comparação direta de distâncias e `.collect()`.
- Correção aplicada: tie-break determinístico via `array_sort(struct)` e `toLocalIterator`.
- Baseado em: refatoração? sim | especialista? sim

## Q48
- Tipo de erro: lógico (ambiguidade de critério)
- Sintoma observado: critério unilateral vs bilateral para anomalia pode divergir.
- Causa raiz: enunciado aberto na interpretação de resíduo.
- Correção aplicada: nenhuma; critério bilateral preservado.
- Baseado em: refatoração? sim | especialista? sim

## Q49
- Tipo de erro: não identificado
- Sintoma observado: sem erro aparente.
- Causa raiz: n/a
- Correção aplicada: nenhuma.
- Baseado em: refatoração? não | especialista? sim

## Q50
- Tipo de erro: lógico
- Sintoma observado: ordenação do payload consolidado podia ficar não determinística.
- Causa raiz: agregação sem ordenação explícita no JSON final.
- Correção aplicada: inclusão de `rank` e ordenação estável com `F.sort_array`.
- Baseado em: refatoração? sim | especialista? sim

## Ambiguidades documentadas
- Q24: definição de share para HHI em cenários de colaboração.
- Q28: critério de “artista mais popular” não fechado no enunciado.
- Q32: interpretação de fronteira para sessão de 30 min.
- Q48: resíduo anômalo unilateral vs bilateral.
