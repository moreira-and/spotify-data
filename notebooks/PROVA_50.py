# %% [markdown]
# # 🎧 PROVA DE EXERCÍCIOS — NÍVEL DIFÍCIL (50 Questões)
# **Aluno:** André Moreira Pimentel
# **Disciplina:** PySpark com Dados do Spotify
# **Total:** 50 questões | **Nível:** Difícil / Avançado
#
# **Instruções:**
# - Preencha o código onde indicado com `# SEU CÓDIGO AQUI`
# - Use o dataset `spotify-data.csv` já carregado
# - Não altere o código de setup
# - Cada questão exige raciocínio avançado — leia com atenção
# ---

# %% [markdown]
# ## Setup (NÃO ALTERAR)

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, lit, when, count, avg, min, max, sum, desc, asc, round, floor, ceil,
    lower, upper, length, trim, concat_ws, split, regexp_replace, regexp_extract, substring,
    year, month, dayofweek, to_date, datediff, date_format,
    row_number, rank, dense_rank, lag, lead, ntile, percent_rank, cume_dist,
    countDistinct, stddev, variance, skewness, kurtosis,
    broadcast, udf, coalesce, array, explode, collect_list, collect_set,
    first, last, greatest, least, abs as spark_abs, log, pow, sqrt, corr
)
from pyspark.sql.types import (
    StringType, IntegerType, FloatType, DoubleType, BooleanType,
    ArrayType, StructType, StructField, MapType
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Prova_moreira-and_50") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

CAMINHO_ARQUIVO = (
    "/Workspace/Repos/moreira-and@outlook.com/spotify-data/data/spotify-data.csv"
)

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(CAMINHO_ARQUIVO)

print(f"✅ Dataset carregado: {df.count()} registros, {len(df.columns)} colunas")

# %% [markdown]
# ---
# # BLOCO 1 — EXPLORAÇÃO E SCHEMA (Q1–Q5)
# ---

# %% [markdown]
# ## Q1 — Tipos e Casting (2 pts)
# Identifique todas as colunas cujo tipo é `string` mas que deveriam ser numéricas.
# Faça o cast correto. Exiba o schema antes e depois da correção.
# Verifique se algum valor ficou `null` após o cast (indicando dados sujos).

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q2 — Análise de Distribuição (2 pts)
# Calcule os **quartis** (25%, 50%, 75%) de `popularity`, `energy` e `danceability`
# usando `approxQuantile`. Compare com os valores obtidos via `percentile_approx`
# em SQL. Os resultados são iguais? Exiba ambos.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q3 — Detecção de Anomalias (2 pts)
# Encontre **outliers** em `duration_ms` usando o método IQR (Interquartile Range):
# - Calcule Q1, Q3 e IQR
# - Outliers: valores < Q1 - 1.5*IQR ou > Q3 + 1.5*IQR
# Quantos outliers existem? Exiba os 10 outliers com maior duração.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q4 — Correlações (2 pts)
# Calcule a **matriz de correlação** entre `popularity`, `energy`, `danceability`,
# `acousticness`, `valence`, `loudness` e `tempo`.
# Identifique os 3 pares de variáveis com **maior correlação absoluta**.
# Use `corr()` do PySpark.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q5 — Qualidade de Dados (2 pts)
# Crie um relatório de qualidade para CADA coluna contendo:
# - % de nulos
# - Contagem de valores distintos
# - Para numéricas: min, max, média, desvio padrão, skewness
# Exiba como um DataFrame organizado.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 2 — FILTRAGEM AVANÇADA (Q6–Q10)
# ---

# %% [markdown]
# ## Q6 — Filtragem com Subquery Lógica (2 pts)
# Encontre músicas cuja `popularity` é maior que a média de popularidade
# do seu respectivo artista E cuja `energy` é maior que a mediana global de energy.
# Use Window Functions para calcular a média por artista inline.
# Exiba as 20 primeiras com `name`, `artists`, `popularity`, `artist_avg_pop`.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q7 — Filtro Multi-Condicional Dinâmico (2 pts)
# Crie um dicionário de filtros:
# ```python
# filters = {
#     "energy": (0.6, 0.9),
#     "danceability": (0.5, 0.8),
#     "valence": (0.4, 1.0),
#     "popularity": (50, 100)
# }
# ```
# Aplique TODOS os filtros de forma programática (loop) ao DataFrame.
# Quantas músicas restam? Exiba as 10 mais populares.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q8 — Anti-Join Lógico (2 pts)
# Encontre artistas que têm pelo menos 5 músicas mas **nenhuma** com popularity > 60.
# Esses são os "artistas prolíficos mas impopulares".
# Exiba artista, total de músicas e popularidade média, ordenado por total DESC.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q9 — Padrões em Nomes (2 pts)
# Use **regex** para encontrar músicas que:
# - Contêm parênteses no nome (ex: "feat.", "remix", "live")
# - Extraia o conteúdo dentro dos parênteses usando `regexp_extract`
# - Classifique: "feat" → "Featuring", "remix" → "Remix", "live" → "Live", outro → "Outro"
# Exiba a contagem por categoria e a popularidade média de cada.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q10 — Análise Temporal com Filtros (2 pts)
# Para cada ano, calcule quantas músicas foram lançadas e a popularidade média.
# Filtre apenas anos com pelo menos 50 músicas.
# Encontre o ano com **maior crescimento percentual** de popularidade média em
# relação ao ano anterior. Use Window Functions com `lag`.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 3 — TRANSFORMAÇÕES COMPLEXAS (Q11–Q18)
# ---

# %% [markdown]
# ## Q11 — Coluna Composta com Lógica Complexa (2 pts)
# Crie uma coluna `mood` baseada em múltiplos atributos:
# - `"Eufórico"`: energy > 0.8 AND valence > 0.7 AND tempo > 120
# - `"Melancólico"`: energy < 0.4 AND valence < 0.3 AND acousticness > 0.5
# - `"Relaxante"`: energy < 0.5 AND acousticness > 0.6 AND instrumentalness > 0.3
# - `"Dançante"`: danceability > 0.7 AND energy > 0.6 AND tempo BETWEEN 100-130
# - `"Intenso"`: energy > 0.7 AND loudness > -5 AND speechiness > 0.1
# - `"Neutro"`: caso contrário
#
# Exiba a distribuição de moods e a popularidade média de cada.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q12 — Normalização Min-Max (2 pts)
# Normalize as colunas `popularity`, `energy`, `danceability`, `tempo` e `loudness`
# para o intervalo [0, 1] usando a fórmula: `(valor - min) / (max - min)`.
# Calcule o min e max globais e aplique a transformação.
# Crie uma coluna `overall_score` = média das 5 colunas normalizadas.
# Exiba as 10 músicas com maior `overall_score`.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q13 — Binning Avançado com Quantis (2 pts)
# Divida `popularity` em 5 bins de **igual frequência** (não igual largura) usando `ntile`.
# Para cada bin, calcule:
# - Quantidade de músicas
# - Ranges de popularity (min–max)
# - Médias de energy, danceability e valence
# Exiba o resultado como um "perfil" de cada bin.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q14 — Feature Engineering: Interações (2 pts)
# Crie as seguintes features de interação:
# - `dance_energy_ratio` = danceability / (energy + 0.001)
# - `acoustic_electronic` = acousticness * (1 - energy)
# - `mood_score` = (valence * 0.5 + danceability * 0.3 + energy * 0.2)
# - `vocal_instrumental` = speechiness / (instrumentalness + 0.001)
# Encontre a feature que tem maior correlação com `popularity`.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q15 — Tratamento de Artistas Múltiplos (2 pts)
# Muitas músicas têm vários artistas separados por vírgula na coluna `artists`.
# - Use `split` e `explode` para criar uma linha por artista
# - Remova espaços extras com `trim`
# - Encontre os 10 artistas com mais **colaborações** (aparições em músicas com outros artistas)
# - Calcule a popularidade média das collabs vs. músicas solo de cada um desses artistas

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q16 — Detecção de Duplicatas Fuzzy (2 pts)
# Encontre possíveis músicas duplicadas:
# - Normalize `name` (lowercase, remova caracteres especiais, trim)
# - Agrupe por nome normalizado
# - Filtre grupos com mais de 1 ocorrência
# - Para cada grupo, exiba as variações e diferenças de popularidade
# Quantos grupos de possíveis duplicatas existem?

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q17 — Encoding de Variáveis Categóricas (2 pts)
# Crie uma UDF que faz **one-hot encoding** manual da coluna `key` (0–11).
# O resultado deve ser um array de 12 elementos (0s e 1s).
# Aplique e verifique que a soma de cada array é sempre 1.
# Depois, "expanda" o array em 12 colunas separadas: `key_0`, `key_1`, ..., `key_11`.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q18 — Cálculo de Z-Score por Grupo (2 pts)
# Para cada artista (com mínimo 5 músicas), calcule o **z-score** de popularity:
# `z = (popularity - media_artista) / std_artista`
# Use Window Functions para calcular média e desvio por artista inline.
# Encontre as 10 músicas com maior z-score absoluto (maiores "surpresas").
# Colunas: `name`, `artists`, `popularity`, `artist_avg`, `artist_std`, `z_score`.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 4 — AGREGAÇÕES AVANÇADAS (Q19–Q25)
# ---

# %% [markdown]
# ## Q19 — Agregação com Pivot (2 pts)
# Crie uma tabela pivot onde:
# - Linhas: décadas (floor(year/10)*10)
# - Colunas: `mode` (0=Minor, 1=Major)
# - Valores: popularidade média
# Exiba a tabela e identifique em qual década a diferença entre Major e Minor
# foi maior.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q20 — Rollup e Cube (2 pts)
# Use `rollup` para calcular a popularidade média em 3 níveis:
# - Por década + explicit
# - Por década (subtotal)
# - Total geral
# Depois repita com `cube` e explique a diferença no resultado.
# Exiba ambos os resultados.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q21 — Top-N por Grupo (2 pts)
# Para cada década, encontre as **3 músicas mais populares** usando Window Functions
# com `row_number`. Exiba: década, ranking, name, artists, popularity.
# Depois, repita usando `dense_rank` — em quais décadas os resultados diferem?

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q22 — Análise de Tendência (2 pts)
# Para cada ano (com >= 30 músicas), calcule a média de `energy`, `acousticness`
# e `danceability`. Use Window Functions com `lag` para calcular a variação
# ano a ano de cada métrica. Identifique o ano com maior mudança simultânea
# nas 3 métricas (soma dos valores absolutos das variações).

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q23 — Entropia por Década (2 pts)
# Calcule a **entropia de Shannon** da distribuição de `key` (0–11) por década.
# Fórmula: `H = -Σ(p * log2(p))` onde p é probabilidade de cada key.
# Décadas com maior entropia têm distribuição mais uniforme de tons.
# Exiba década, entropia, e o tom dominante de cada década.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q24 — Índice de Diversidade de Artistas (2 pts)
# Para cada década, calcule:
# - Total de artistas distintos
# - Total de músicas
# - Índice HHI (Herfindahl–Hirschman): `HHI = Σ(share_i²)` onde share_i = songs_artist_i / total
# Um HHI baixo indica maior diversidade. Qual década foi mais diversa?
# (Use aggregation + self-join ou collect_list para calcular.)

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q25 — Média Móvel e Suavização (2 pts)
# Calcule a **média móvel de 5 anos** da popularidade média anual.
# Use Window Functions com `rowsBetween(-2, 2)`.
# Compare a série original vs. suavizada — em quais períodos houve maior
# divergência? Exiba ano, pop_média, pop_suavizada, diferença.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 5 — SQL AVANÇADO NO SPARK (Q26–Q30)
# ---

# %% [markdown]
# ## Q26 — CTE e Subqueries Correlacionadas (2 pts)
# Registre o DF como view `spotify`. Usando **apenas** spark.sql(), escreva uma query com
# CTEs (WITH) que encontre artistas cujas músicas têm popularidade média acima
# da mediana global E que tenham pelo menos 3 músicas acima de popularity 70.
# Ordene por popularidade média DESC. Limite a 15.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q27 — Window Functions em SQL (2 pts)
# Em SQL puro, para cada música calcule:
# - `rank_in_year`: ranking de popularidade dentro do seu ano
# - `pct_rank_overall`: percentil de popularidade geral
# - `running_avg`: média móvel de 3 anos da popularidade
# Exiba as músicas que são Top 3 do seu ano E estão no percentil 95+ geral.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q28 — CASE WHEN Complexo em SQL (2 pts)
# Em SQL, crie uma classificação `era_musical`:
# - year < 1960: "Golden Age"
# - 1960–1969: "British Invasion"
# - 1970–1979: "Disco & Rock"
# - 1980–1989: "Synth Pop"
# - 1990–1999: "Grunge & Hip-Hop"
# - 2000–2009: "Digital Revolution"
# - 2010–2019: "Streaming Era"
# - 2020+: "Post-Pandemic"
#
# Calcule para cada era: total de músicas, média de energy, acousticness e popularity,
# % de músicas explícitas, artista mais popular.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q29 — Query Analítica com HAVING e Subquery (2 pts)
# Em SQL, encontre artistas "one-hit wonders": artistas com apenas 1 música
# com popularity > 70, mas cujas outras músicas têm popularity < 30.
# Mínimo de 3 músicas totais. Exiba artista, hit (nome e popularity),
# e média das outras músicas.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q30 — Query Recursiva Simulada (2 pts)
# Em SQL, simule uma análise de "caminho" de evolução musical:
# Para cada década, calcule a variação percentual de cada métrica
# (energy, danceability, acousticness, valence) em relação à década anterior.
# Depois calcule a variação acumulada desde a primeira década.
# Use LAG e SUM com Window Functions em SQL.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 6 — WINDOW FUNCTIONS AVANÇADAS (Q31–Q36)
# ---

# %% [markdown]
# ## Q31 — Ranking Multi-Critério (2 pts)
# Crie um ranking composto para cada música baseado em:
# - rank por popularity (peso 0.4)
# - rank por energy (peso 0.3)
# - rank por danceability (peso 0.3)
# Use `percent_rank()` para normalizar cada ranking.
# Score final = `0.4*prank_pop + 0.3*prank_energy + 0.3*prank_dance`.
# Exiba as 15 músicas com maior score composto.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q32 — Sessões de Escuta Simuladas (2 pts)
# Ordene músicas por popularidade DESC. Simule "sessões" onde cada sessão
# tem no máximo 30 minutos de música (use `duration_ms`).
# Use Window Functions com soma cumulativa de duração e incremento de sessão
# quando a soma exceder 30 min (1800000 ms).
# Quantas sessões seriam necessárias? Qual sessão tem maior popularity média?

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q33 — Gaps and Islands (2 pts)
# Considere os anos com pelo menos 1 música no dataset.
# Identifique "ilhas" de anos consecutivos e "gaps" (anos sem músicas).
# Use a técnica de row_number para agrupar anos consecutivos.
# Para cada ilha: ano_inicio, ano_fim, total_musicas, pop_media.
# Para cada gap: ano_inicio_gap, ano_fim_gap, duração.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q34 — Análise Lead/Lag Multi-Nível (2 pts)
# Para cada artista (com mínimo 5 músicas), ordene suas músicas por ano.
# Calcule:
# - `prev_pop`: popularidade da música anterior (lag)
# - `next_pop`: popularidade da música seguinte (lead)
# - `momentum`: `next_pop - prev_pop` (tendência)
# - `consistency`: desvio padrão da popularidade em janela de 3 músicas
# Encontre os 5 artistas com maior momentum médio.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q35 — Ntile e Segmentação (2 pts)
# Divida as músicas em 10 decis de popularidade usando `ntile(10)`.
# Para cada decil, calcule o "perfil sonoro" médio (energy, danceability,
# acousticness, valence, tempo, loudness).
# Identifique quais atributos mudam mais entre o decil 1 (menos popular)
# e o decil 10 (mais popular). Use a variação percentual.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q36 — Cume_Dist e Percentis (2 pts)
# Calcule o `cume_dist()` e `percent_rank()` para popularity.
# Encontre músicas que estão no Top 1% de popularidade.
# Para essas músicas "elite", compare suas médias de energy, danceability,
# acousticness e valence com as médias do Bottom 50%.
# Exiba as diferenças como um DataFrame comparativo.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 7 — UDFs E TIPOS COMPLEXOS (Q37–Q41)
# ---

# %% [markdown]
# ## Q37 — UDF com Lógica de Negócio Complexa (2 pts)
# Crie uma UDF `classify_track` que recebe (energy, valence, tempo, acousticness,
# danceability) e retorna uma string descritiva multi-nível:
# - Primeiro nível: "High Energy" / "Low Energy" (threshold 0.5)
# - Segundo nível: "Positive" / "Negative" (valence threshold 0.5)
# - Terceiro nível: "Fast" / "Slow" (tempo threshold 120)
# Formato: "High Energy | Positive | Fast"
# Aplique e exiba a contagem de cada combinação.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q38 — UDF que Retorna Struct (2 pts)
# Crie uma UDF que recebe `duration_ms` e `tempo` e retorna um StructType com:
# - `minutes`: duração em minutos (float)
# - `beats_total`: número estimado de batidas (tempo * minutes)
# - `category`: "Short & Slow", "Short & Fast", "Long & Slow", "Long & Fast"
#   (threshold: 3.5 min, 120 bpm)
# Aplique e agrupe por category mostrando contagem e popularity média.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q39 — Collect_List e Análise de Arrays (2 pts)
# Para cada artista (com >= 5 músicas), colete todas as popularidades em um array
# usando `collect_list`. Crie uma UDF que recebe o array e calcula:
# - Mediana
# - Amplitude (max - min)
# - Coeficiente de variação (std/mean)
# Encontre os 10 artistas mais "inconsistentes" (maior CV).

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q40 — MapType e Perfis (2 pts)
# Para cada década, crie um `MapType` contendo o "perfil sonoro":
# chaves = nomes das métricas (energy, danceability, etc.)
# valores = médias arredondadas.
# Use `create_map` ou `to_json` + UDF para construir.
# Exiba como JSON legível por década.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q41 — UDF Vetorizada com Pandas (2 pts)
# Crie uma **Pandas UDF** (vectorized UDF) que normaliza uma coluna numérica
# usando z-score: `(x - mean) / std`. Aplique em `popularity`, `energy` e `danceability`.
# Compare o tempo de execução com uma UDF tradicional (row-at-a-time) para
# a mesma operação usando `spark.time()` ou medição manual.
# (Dica: `@pandas_udf(DoubleType())`)

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 8 — JOINS E OTIMIZAÇÃO (Q42–Q46)
# ---

# %% [markdown]
# ## Q42 — Self-Join com Lógica de Similaridade (2 pts)
# Encontre pares de músicas "similares":
# - Diferença de energy < 0.05
# - Diferença de danceability < 0.05
# - Diferença de valence < 0.05
# - Diferença de tempo < 5
# - Músicas diferentes (id diferente)
# Use um self-join. Para evitar duplicatas (A,B) e (B,A), filtre id1 < id2.
# Quantos pares existem? Exiba os 10 com maior diferença de popularity.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q43 — Multi-Join com DataFrames Derivados (2 pts)
# Crie 3 DataFrames auxiliares:
# 1. `df_artist_stats`: stats por artista (avg_pop, count, std_pop)
# 2. `df_year_stats`: stats por ano (avg_pop, avg_energy, total)
# 3. `df_key_mode`: stats por key+mode (avg_pop, avg_dance)
# Faça join do DF original com os 3, usando broadcast nos auxiliares.
# Crie `relative_pop = popularity - artist_avg_pop` e encontre as 10 músicas
# que mais superaram a média do seu artista.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q44 — Left Anti Join para Exclusões (2 pts)
# Crie um DataFrame `blacklist` com os 50 artistas mais populares.
# Use `left_anti` join para removê-los do dataset original.
# Compare as métricas médias (energy, danceability, valence, acousticness)
# do dataset completo vs. o "dataset sem mainstream".
# Qual a diferença? O underground soa diferente?

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q45 — Otimização: Repartition e Coalesce (2 pts)
# Verifique o número atual de partições do DataFrame.
# Reparticione por `year` e meça (ou observe no plano) o efeito.
# Depois, use `coalesce` para reduzir a 2 partições.
# Salve o resultado em Parquet particionado por `decade` (floor(year/10)*10).
# Verifique a estrutura de pastas criada.
# (Use `df.rdd.getNumPartitions()` e `df.explain()`)

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q46 — Explain e Catalyst (2 pts)
# Compare os planos de execução (explain) de:
# 1. `df.filter(...).groupBy(...).agg(...)` (filter antes do group)
# 2. `df.groupBy(...).agg(...).filter(...)` (filter depois do group)
# O Catalyst otimiza? São iguais?
# Depois compare um join normal vs. broadcast join usando `explain(True)`.
# Documente as diferenças observadas nos planos.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# # BLOCO 9 — ANÁLISES AVANÇADAS (Q47–Q50)
# ---

# %% [markdown]
# ## Q47 — Clusterização Manual por Perfil (2 pts)
# Sem usar MLlib, implemente um k-means simplificado:
# 1. Selecione features: energy, danceability, valence (já normalizadas 0–1)
# 2. Defina 4 centróides iniciais manualmente:
#    - C1=(0.2, 0.2, 0.2), C2=(0.8, 0.8, 0.8), C3=(0.8, 0.2, 0.5), C4=(0.2, 0.8, 0.5)
# 3. Calcule distância euclidiana de cada música a cada centróide usando colunas
# 4. Atribua cada música ao centróide mais próximo (use `least` + `when`)
# 5. Recalcule centróides (médias do cluster)
# 6. Repita a atribuição 1x com os novos centróides
# Exiba a distribuição e popularidade média por cluster.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q48 — Análise de Séries Temporais Completa (2 pts)
# Para a métrica `energy` ao longo dos anos:
# - Calcule média anual, média móvel de 5 anos, tendência (regressão linear simples)
# - Tendência: `slope = corr(year, metric) * (std_metric / std_year)`
# - Calcule o resíduo: `residual = valor_real - tendência`
# - Identifique os anos "anômalos" (resíduo > 2 desvios padrão)
# Exiba: ano, energy_media, media_movel, tendencia, residuo.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q49 — Rede de Colaborações (2 pts)
# Construa uma rede de colaborações entre artistas:
# - Separe artistas usando split/explode
# - Para músicas com 2+ artistas, crie pares (artista1, artista2)
# - Conte o número de colaborações por par
# - Encontre os 10 pares mais frequentes
# - Para cada artista no Top 10 de pares, calcule o "grau" (número de colaboradores únicos)
# - Quem é o artista mais "conectado"?

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ## Q50 — Dashboard Analítico Final (2 pts)
# Crie um "dashboard" em formato de DataFrame consolidado que mostre:
# 1. **Resumo geral**: total músicas, total artistas, span de anos
# 2. **Top 5 artistas** por popularidade média (min 10 músicas)
# 3. **Década de ouro**: década com maior diversidade (mais artistas distintos / música)
# 4. **Evolução**: se a música ficou mais ou menos energética ao longo do tempo (slope)
# 5. **Predição simples**: baseado na tendência de popularity, qual seria a predição
#    para o próximo ano? Use regressão linear simples (slope * next_year + intercept).
# Exiba cada insight como um show() ou print formatado.

# %%
# SEU CÓDIGO AQUI


# %% [markdown]
# ---
# ## 🧹 Cleanup
# %%
spark.stop()
print("✅ Prova finalizada! Boa sorte, Juan!")
