# Plano de Revisão — `PROVA_50`

## 1. Resumo executivo
Classificação: **Reprovado tecnicamente**.

Motivo:
- Aderência estrutural aparente é boa (50 questões preenchidas e sem placeholders remanescentes), mas há lacunas semânticas e de engenharia que comprometem prontidão para produção.
- Há um problema recorrente de modelagem do eixo `artists` em questões que pedem análise por artista (uso da string bruta em vez de artista individual), gerando risco de resultado analítico incorreto.
- Há escolhas custosas em performance (múltiplas ações repetidas, ordenações globais frequentes, broadcasts sem critério explícito de tamanho) que são aceitáveis em prova didática, mas frágeis para escala.
- O arquivo está próximo de aprovação acadêmica, mas não de aprovação técnica sênior para produção.

## 2. Diagnóstico estrutural
- **Sem violações estruturais explícitas** detectáveis no artefato atual: 50 questões presentes e blocos preenchidos.
- Não há indício de questões removidas ou setup/cleanup quebrado.
- **Incerteza material**: sem snapshot/base original nesta revisão, não é possível comprovar linha a linha se houve edição fora de blocos autorizados; a análise estrutural foi inferencial sobre o arquivo final.
- Script permanece executável em tese (não validado em runtime Spark nesta revisão).

## 3. Diagnóstico técnico global
Pontos fortes:
- Cobertura completa de Q1–Q50.
- Boa variedade de APIs: DataFrame, Spark SQL, Window Functions, UDF/Pandas UDF, joins, rollup/cube.
- Em geral, há tratamento defensivo de divisão por zero e nulls em pontos críticos.

Fragilidades recorrentes:
- **Semântica por artista inconsistente**: várias questões agrupam por `artists` bruto (lista serializada), enquanto outras explodem artistas; isso quebra comparabilidade e pode distorcer resultados.
- **Custo de execução elevado em pontos-chave**: loops com múltiplos `.first()/.count()`, várias ordenações globais e agregações repetidas.
- **Broadcast sem validação de cardinalidade** em Q43.
- **Uso de ranking/ordenação que altera semântica de empate** (Q21 com `dense_rank` usando chave secundária).
- Algumas respostas atendem parcialmente ao enunciado, mas com aproximações não explicitadas (ex.: acumulado percentual em Q30).

Antipadrões observados:
- Driver actions repetidas para montar métricas onde caberia agregação consolidada.
- Dependência implícita de dataset pequeno em algumas escolhas.
- Benchmark simplista em Q41 sem controle de warm-up/repetição.

## 4. Revisão por questão
### Q1
- Status: **Parcial**
- Problemas encontrados: Heurística `>=95%` para detectar colunas numerificáveis pode perder colunas válidas; múltiplas passagens por coluna com `.first()`.
- Impacto técnico: Risco de falso negativo semântico e custo de execução elevado em datasets largos.
- Ajuste recomendado: Consolidar perfilamento em menos passes e explicitar critério de decisão; evitar threshold arbitrário sem justificativa.

### Q2
- Status: **Correta**
- Problemas encontrados: Sem desvios relevantes.
- Impacto técnico: Baixo.
- Ajuste recomendado: Opcionalmente explicitar tolerância numérica usada na comparação.

### Q3
- Status: **Correta**
- Problemas encontrados: Sem desvios relevantes.
- Impacto técnico: Baixo.
- Ajuste recomendado: Opcionalmente persistir limites/contagem em dataframe de auditoria.

### Q4
- Status: **Parcial**
- Problemas encontrados: `df.stat.corr` em loop N² gera múltiplas actions; custo cresce com número de variáveis.
- Impacto técnico: Ineficiência em escala e maior latência.
- Ajuste recomendado: Calcular matriz com abordagem vetorizada/SQL única ou reduzir ações materializando base projetada.

### Q5
- Status: **Parcial**
- Problemas encontrados: Loop por coluna com `df.agg(...).first()` para cada campo.
- Impacto técnico: Alto custo por recomputação e múltiplos scans.
- Ajuste recomendado: Reestruturar para agregações consolidadas por tipo/coluna com menor número de jobs.

### Q6
- Status: **Parcial**
- Problemas encontrados: Seleção final inclui `energy`, além do pedido; eixo `artists` permanece bruto.
- Impacto técnico: Pequeno desvio de contrato e possível distorção de “artista” em faixas com múltiplos artistas.
- Ajuste recomendado: Restringir colunas ao enunciado e padronizar conceito de artista.

### Q7
- Status: **Correta**
- Problemas encontrados: Sem desvios relevantes.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q8
- Status: **Incorreta**
- Problemas encontrados: Agrupamento por `artists` bruto (string de lista), não por artista individual.
- Impacto técnico: Resultado semântico incorreto para “artistas prolíficos”.
- Ajuste recomendado: Normalizar `artists` com split/explode antes do agrupamento.

### Q9
- Status: **Correta**
- Problemas encontrados: Usa apenas o primeiro parêntese quando existem múltiplos.
- Impacto técnico: Baixo/moderado em casos com múltiplos marcadores.
- Ajuste recomendado: Opcional tratar múltiplas ocorrências.

### Q10
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q11
- Status: **Correta**
- Problemas encontrados: Sem desvios relevantes de lógica.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q12
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q13
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q14
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Opcional tratar extremos para evitar caudas em `ratio`.

### Q15
- Status: **Correta**
- Problemas encontrados: Parsing de artistas por vírgula pode ser frágil para casos excepcionais.
- Impacto técnico: Moderado em dados sujos.
- Ajuste recomendado: Endurecer normalização do campo `artists`.

### Q16
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q17
- Status: **Parcial**
- Problemas encontrados: Validação “soma sempre 1” não é garantida para valores nulos/inválidos de `key`.
- Impacto técnico: Pode mascarar registros fora de domínio.
- Ajuste recomendado: Validar domínio explicitamente e reportar exceções separadamente.

### Q18
- Status: **Parcial**
- Problemas encontrados: Particionamento por `artists` bruto, não artista individual.
- Impacto técnico: Z-score por artista pode ficar semanticamente distorcido.
- Ajuste recomendado: Padronizar modelagem de artista antes de calcular janelas.

### Q19
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q20
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q21
- Status: **Parcial**
- Problemas encontrados: `dense_rank` usa janela com `orderBy(popularity DESC, name ASC)`, quebrando semântica de empate por popularidade.
- Impacto técnico: Diferença entre `row_number` e `dense_rank` pode ficar artificial.
- Ajuste recomendado: Rankear `dense_rank` apenas por `popularity`.

### Q22
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q23
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q24
- Status: **Parcial**
- Problemas encontrados: HHI usa referência artist-song após explode; definição de share pode divergir do enunciado em cenários de colaboração.
- Impacto técnico: Índice pode ficar enviesado.
- Ajuste recomendado: Definir explicitamente política de contagem (por música vs por participação).

### Q25
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q26
- Status: **Parcial**
- Problemas encontrados: SQL puro foi respeitado, mas agregação por `artists` bruto mantém ambiguidade semântica de artista.
- Impacto técnico: Resultado pode representar “combinações de artistas”, não artistas.
- Ajuste recomendado: Normalizar artista em view auxiliar antes da query.

### Q27
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q28
- Status: **Parcial**
- Problemas encontrados: “Artista mais popular” foi inferido por média; enunciado não fixa critério e `artists` bruto segue ambíguo.
- Impacto técnico: Possível divergência de interpretação.
- Ajuste recomendado: Fixar critério de popularidade e padronizar artista individual.

### Q29
- Status: **Parcial**
- Problemas encontrados: Lógica de one-hit está boa, porém ainda sobre `artists` bruto.
- Impacto técnico: Identificação de one-hit pode ficar deslocada.
- Ajuste recomendado: Avaliar no nível de artista individual.

### Q30
- Status: **Parcial**
- Problemas encontrados: Acumulado percentual foi modelado como soma de variações percentuais, não como variação acumulada desde baseline.
- Impacto técnico: Pode distorcer leitura de evolução acumulada.
- Ajuste recomendado: Calcular baseline da primeira década e derivar acumulado relativo a ela (ou composição multiplicativa explícita).

### Q31
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q32
- Status: **Parcial**
- Problemas encontrados: Sessão por bucket da soma cumulativa pode não refletir fielmente a regra operacional em fronteiras.
- Impacto técnico: Possível erro de alocação em sessões limítrofes.
- Ajuste recomendado: Implementar regra de rollover de sessão explicitamente.

### Q33
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q34
- Status: **Parcial**
- Problemas encontrados: Novamente usa `artists` bruto; ordenação intra-ano pode gerar variação de momentum não estável.
- Impacto técnico: Métrica pode ser pouco reprodutível em empates.
- Ajuste recomendado: Definir chave de ordenação temporal estável e artista normalizado.

### Q35
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q36
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos relevantes.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q37
- Status: **Parcial**
- Problemas encontrados: UDF recebe parâmetros não usados (`acousticness`, `danceability`), e fallback de null simplificado.
- Impacto técnico: Sinal de contrato frouxo e manutenção mais difícil.
- Ajuste recomendado: Alinhar assinatura ao uso real ou usar todos os parâmetros com regra explícita.

### Q38
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q39
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Opcional explicitar escolha de desvio populacional vs amostral.

### Q40
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q41
- Status: **Parcial**
- Problemas encontrados: Benchmark sem repetição, sem warm-up e com materialização simplificada por `.collect()`.
- Impacto técnico: Comparação de tempo pouco confiável para conclusão de performance.
- Ajuste recomendado: Rodar múltiplas iterações, descartar primeira execução e comparar medianas.

### Q42
- Status: **Correta**
- Problemas encontrados: Self-join é pesado por natureza, apesar do pré-bucket.
- Impacto técnico: Pode escalar mal sem partição/skew handling.
- Ajuste recomendado: Validar plano físico e skew por buckets.

### Q43
- Status: **Parcial**
- Problemas encontrados: Broadcast aplicado de forma cega em três auxiliares, incluindo `df_artist_stats` potencialmente grande.
- Impacto técnico: Risco de OOM e piora de plano em clusters reais.
- Ajuste recomendado: Condicionar broadcast por cardinalidade/tamanho estimado.

### Q44
- Status: **Parcial**
- Problemas encontrados: “Top 50 artistas mais populares” sem critério de robustez (mínimo de faixas), suscetível a outliers.
- Impacto técnico: Blacklist pode ficar enviesada.
- Ajuste recomendado: Fixar critério (ex.: média com mínimo de ocorrências).

### Q45
- Status: **Parcial**
- Problemas encontrados: Caminho local `data/tmp/...` e validação com `os.path` podem não refletir filesystem distribuído no Databricks.
- Impacto técnico: Falha de portabilidade e validação de partições incompleta.
- Ajuste recomendado: Usar caminho/FS do ambiente alvo e validação via APIs Spark/DBFS.

### Q46
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q47
- Status: **Parcial**
- Problemas encontrados: Uso de `.collect()` para centroides (pequeno, mas driver-bound) e empate por igualdade de distância sem critério explícito.
- Impacto técnico: Risco de comportamento não determinístico em empates.
- Ajuste recomendado: Definir tie-break deterministicamente e minimizar ida ao driver.

### Q48
- Status: **Parcial**
- Problemas encontrados: Critério de anomalia aplicado em valor absoluto do resíduo; enunciado textual admite interpretação, mas está implícito.
- Impacto técnico: Pode alterar conjunto de anos sinalizados.
- Ajuste recomendado: Declarar explicitamente critério adotado (unilateral vs bilateral) e justificar.

### Q49
- Status: **Correta**
- Problemas encontrados: Sem desvios críticos.
- Impacto técnico: Baixo.
- Ajuste recomendado: Nenhum crítico.

### Q50
- Status: **Parcial**
- Problemas encontrados: Agregações por artista usam explode (aceitável), mas sem política de ponderação para colaborações; JSON final sem ordenação explícita dos itens.
- Impacto técnico: Leitura de ranking pode variar em reexecuções.
- Ajuste recomendado: Fixar ordenação no payload consolidado e política de contagem de colaborações.

## 5. Itens prioritários de correção
1. **Modelagem de artista inconsistente (`artists` bruto vs artista individual)**  
Impacto: erro semântico em múltiplas questões (Q8, Q18, Q26, Q29, Q34, Q43, Q50).  
Correção esperada: normalizar artista com split/explode e padronizar regra no arquivo todo.

2. **Semântica de ranking em Q21 (`dense_rank` com chave secundária)**  
Impacto: resultado incorreto para empate por popularidade.  
Correção esperada: aplicar `dense_rank` sobre ordenação primária coerente com o enunciado.

3. **Acumulado percentual em Q30 não alinhado a baseline**  
Impacto: evolução acumulada potencialmente distorcida.  
Correção esperada: acumulado relativo à primeira década (ou composição multiplicativa explicitada).

4. **Broadcast indiscriminado em Q43**  
Impacto: risco de OOM e plano subótimo em ambiente distribuído.  
Correção esperada: decidir broadcast por tamanho estimado/estatísticas.

5. **Custos evitáveis por recomputação (Q4, Q5, Q41)**  
Impacto: latência alta e baixa escalabilidade.  
Correção esperada: consolidar agregações, reduzir actions e melhorar método de benchmark.

6. **Portabilidade de escrita/validação em Q45**  
Impacto: execução frágil fora de ambiente local.  
Correção esperada: alinhar path e inspeção ao filesystem do runtime Spark alvo.

## 6. Plano de ação recomendado
1. Padronizar contrato de artista para todo o arquivo (definir representação única e revalidar questões afetadas).
2. Corrigir semântica de ranking e acumulados (Q21, Q30) com critérios explícitos.
3. Revisar joins críticos e estratégia de broadcast (Q43), incluindo evidência de tamanho.
4. Reduzir recomputações em blocos caros (Q4, Q5) e endurecer benchmark de Q41.
5. Ajustar portabilidade de I/O e validação de partições em Q45 para Databricks/FS distribuído.
6. Rodar validação funcional completa no Databricks com `explain(True)` nos blocos mais pesados.
7. Executar revisão final de consistência cruzada entre questões (mesmas entidades, mesmas regras de contagem).

## 7. Critérios de aceite para nova revisão
- Todas as questões que analisam artista usam a mesma definição de artista individual.
- Q21 e Q30 aderem estritamente à semântica do enunciado.
- Nenhum broadcast é aplicado sem justificativa de tamanho/cardinalidade.
- Redução comprovada de recomputações/actions desnecessárias em Q4/Q5/Q41.
- Q45 executa e valida partições no filesystem do ambiente alvo.
- Nenhuma questão fica com status “Incorreta”; no máximo “Parcial” residual com justificativa técnica explícita.
- Execução integral em Databricks sem falhas de runtime.

## 8. Veredito final
**Precisa de refatoração relevante.**

Justificativa técnica:
- O arquivo está completo e majoritariamente funcional, mas ainda há falhas semânticas e de engenharia distribuída que impedem aprovação técnica sênior.
- O principal bloqueador é a inconsistência no tratamento de artista e os desvios de semântica em pontos analíticos centrais.
- Após as correções prioritárias e validação em runtime, o material tende a evoluir para “Aprovado com ajustes”.
