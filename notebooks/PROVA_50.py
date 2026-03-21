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

import os
from pathlib import Path
from typing import Any, cast

# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (array, asc, avg, broadcast, ceil, coalesce,
                                   col, collect_list, collect_set, concat_ws,
                                   corr, count, cume_dist, date_format,
                                   datediff, dayofweek, dense_rank, desc,
                                   explode, first, floor, greatest, kurtosis,
                                   lag, last, lead, least, length, lit, log,
                                   lower, max, min, month, ntile, percent_rank,
                                   pow, rank, regexp_extract, regexp_replace,
                                   round, row_number, skewness, split, sqrt,
                                   stddev, substring, sum, to_date, trim, udf,
                                   upper, variance, when, year)
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, FloatType,
                               IntegerType, MapType, StringType, StructField,
                               StructType)
from pyspark.sql.window import Window

# %%
# Criar SparkSession
spark = (
    SparkSession.builder.appName("SpotifyAnalysis")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.memory", "1g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

print(f"✅ Spark versão: {spark.version}")

spark.sql("SHOW VOLUMES").show()

# ## 📂 Carregamento dos Dados
# Caminho do arquivo CSV
CAMINHO_ARQUIVO = (
    "/Workspace/Repos/moreira-and@outlook.com/spotify-data/data/spotify-data.csv"
)

# Schema do arquivo
schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("artists", StringType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("release_date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("energy", DoubleType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("valence", DoubleType(), True),
        StructField("mode", IntegerType(), True),
        StructField("key", IntegerType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("explicit", IntegerType(), True),
    ]
)

# Carregar dados
df = (
    spark.read.schema(schema)
    .option("header", "true")
    .option("quote", '"')
    .option("escape", '"')
    .option("multiLine", "true")
    .csv(CAMINHO_ARQUIVO)
)

print(f"✅ Dataset carregado com {df.count():,} músicas")

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
# Interpretacao: identificar strings numerificaveis, aplicar cast seguro e medir nulls introduzidos por sujeira.
print("Schema antes do cast:")
df.printSchema()

string_cols_q1 = [
    field.name
    for field in df.schema.fields
    if field.dataType.simpleString() == "string"
]

candidate_casts_q1 = []
for col_name in string_cols_q1:
    metrics_q1 = cast(
        Any,
        df.select(
            F.sum(
                F.when(
                    F.col(col_name).isNotNull() & (F.trim(F.col(col_name)) != ""), 1
                ).otherwise(0)
            ).alias("non_empty"),
            F.sum(
                F.when(F.col(col_name).rlike(r"^\s*[+-]?\d+(\.\d+)?\s*$"), 1).otherwise(
                    0
                )
            ).alias("numeric_like"),
            F.sum(
                F.when(F.col(col_name).rlike(r"^\s*[+-]?\d+\s*$"), 1).otherwise(0)
            ).alias("integer_like"),
        ).first()
        or {"non_empty": 0, "numeric_like": 0, "integer_like": 0},
    )

    non_empty = metrics_q1["non_empty"] or 0
    numeric_like = metrics_q1["numeric_like"] or 0
    integer_like = metrics_q1["integer_like"] or 0

    if non_empty > 0 and (numeric_like / non_empty) >= 0.95:
        target_type = "int" if integer_like == non_empty else "double"
        candidate_casts_q1.append((col_name, target_type))

df_q1 = df
null_report_q1 = []
for col_name, target_type in candidate_casts_q1:
    introduced_nulls_row_q1 = cast(
        Any,
        df.select(
            F.sum(
                F.when(
                    F.col(col_name).isNotNull()
                    & (F.trim(F.col(col_name)) != "")
                    & F.col(col_name).cast(target_type).isNull(),
                    1,
                ).otherwise(0)
            ).alias("introduced_nulls")
        ).first()
        or {"introduced_nulls": 0},
    )
    introduced_nulls = introduced_nulls_row_q1["introduced_nulls"]

    null_report_q1.append((col_name, target_type, int(introduced_nulls or 0)))
    df_q1 = df_q1.withColumn(col_name, F.col(col_name).cast(target_type))

if candidate_casts_q1:
    print("Colunas convertidas:", candidate_casts_q1)
    spark.createDataFrame(
        null_report_q1, ["coluna", "tipo_destino", "nulls_pos_cast"]
    ).show(truncate=False)
else:
    print("Nenhuma coluna string com perfil numerico forte foi encontrada para cast.")

print("Schema depois do cast:")
df_q1.printSchema()


# %% [markdown]
# ## Q2 — Análise de Distribuição (2 pts)
# Calcule os **quartis** (25%, 50%, 75%) de `popularity`, `energy` e `danceability`
# usando `approxQuantile`. Compare com os valores obtidos via `percentile_approx`
# em SQL. Os resultados são iguais? Exiba ambos.

# %%
# Interpretacao: comparar quartis via API (approxQuantile) e SQL (percentile_approx) para validar consistencia.
quantile_cols_q2 = ["popularity", "energy", "danceability"]
approx_quantiles_q2 = {
    c: df.approxQuantile(c, [0.25, 0.50, 0.75], 0.001) for c in quantile_cols_q2
}

df.createOrReplaceTempView("spotify")
sql_quantiles_q2 = cast(
    Any,
    spark.sql("""
    SELECT
        percentile_approx(popularity, array(0.25, 0.50, 0.75), 10000) AS popularity_q,
        percentile_approx(energy, array(0.25, 0.50, 0.75), 10000) AS energy_q,
        percentile_approx(danceability, array(0.25, 0.50, 0.75), 10000) AS danceability_q
    FROM spotify
    """).first()
    or {
        "popularity_q": [None, None, None],
        "energy_q": [None, None, None],
        "danceability_q": [None, None, None],
    },
)

comparison_rows_q2 = []
for c in quantile_cols_q2:
    aq = approx_quantiles_q2[c]
    sq = [float(x) for x in sql_quantiles_q2[f"{c}_q"]]
    equal_with_tol = all(abs(aq[i] - sq[i]) <= 1e-6 for i in range(3))
    comparison_rows_q2.append(
        (c, aq[0], aq[1], aq[2], sq[0], sq[1], sq[2], equal_with_tol)
    )

result_q2 = spark.createDataFrame(
    comparison_rows_q2,
    [
        "metric",
        "approx_q25",
        "approx_q50",
        "approx_q75",
        "sql_q25",
        "sql_q50",
        "sql_q75",
        "mesmos_valores",
    ],
)
result_q2.show(truncate=False)


# %% [markdown]
# ## Q3 — Detecção de Anomalias (2 pts)
# Encontre **outliers** em `duration_ms` usando o método IQR (Interquartile Range):
# - Calcule Q1, Q3 e IQR
# - Outliers: valores < Q1 - 1.5*IQR ou > Q3 + 1.5*IQR
# Quantos outliers existem? Exiba os 10 outliers com maior duração.

# %%
# Interpretacao: aplicar IQR em duration_ms para detectar caudas extremas sem assumir normalidade.
q1_q3_q3 = cast(Any, df.approxQuantile("duration_ms", [0.25, 0.75], 0.001))
q1_duration_q3 = q1_q3_q3[0]
q3_duration_q3 = q1_q3_q3[1]
iqr_duration_q3 = q3_duration_q3 - q1_duration_q3

lower_bound_q3 = q1_duration_q3 - (1.5 * iqr_duration_q3)
upper_bound_q3 = q3_duration_q3 + (1.5 * iqr_duration_q3)

df_outliers_q3 = df.filter(
    (F.col("duration_ms") < F.lit(lower_bound_q3))
    | (F.col("duration_ms") > F.lit(upper_bound_q3))
)

print(
    f"Q1={q1_duration_q3:.2f}, Q3={q3_duration_q3:.2f}, IQR={iqr_duration_q3:.2f}, "
    f"Lower={lower_bound_q3:.2f}, Upper={upper_bound_q3:.2f}"
)
print(f"Total de outliers em duration_ms: {df_outliers_q3.count()}")

df_outliers_q3.select("id", "name", "artists", "duration_ms", "popularity").orderBy(
    F.desc("duration_ms")
).show(10, truncate=False)


# %% [markdown]
# ## Q4 — Correlações (2 pts)
# Calcule a **matriz de correlação** entre `popularity`, `energy`, `danceability`,
# `acousticness`, `valence`, `loudness` e `tempo`.
# Identifique os 3 pares de variáveis com **maior correlação absoluta**.
# Use `corr()` do PySpark.

# %%
# Interpretacao: gerar matriz de correlacao e destacar os pares com maior relacao linear absoluta.
# AGENT: ajuste de debug - consolidacao das correlacoes reduz o numero de actions e recomputacoes.
corr_cols_q4 = [
    "popularity",
    "energy",
    "danceability",
    "acousticness",
    "valence",
    "loudness",
    "tempo",
]

corr_pair_exprs_q4 = []
for i, c1 in enumerate(corr_cols_q4):
    for c2 in corr_cols_q4[i + 1 :]:
        corr_pair_exprs_q4.append(corr(c1, c2).alias(f"{c1}__{c2}"))

corr_values_row_q4 = df.select(*corr_cols_q4).agg(*corr_pair_exprs_q4).first()
corr_values_q4 = cast(Any, corr_values_row_q4.asDict() if corr_values_row_q4 else {})

corr_entries_q4 = []
top_pairs_q4 = []
for c1 in corr_cols_q4:
    for c2 in corr_cols_q4:
        if c1 == c2:
            corr_val = 1.0
        else:
            key_forward = f"{c1}__{c2}"
            key_reverse = f"{c2}__{c1}"
            corr_val = corr_values_q4.get(key_forward, corr_values_q4.get(key_reverse))
            corr_val = float(corr_val) if corr_val is not None else None
        corr_entries_q4.append((c1, c2, corr_val))

for i, c1 in enumerate(corr_cols_q4):
    for c2 in corr_cols_q4[i + 1 :]:
        pair_val = corr_values_q4.get(f"{c1}__{c2}", corr_values_q4.get(f"{c2}__{c1}"))
        pair_val = float(pair_val) if pair_val is not None else None
        if pair_val is not None:
            top_pairs_q4.append((c1, c2, pair_val, abs(pair_val)))

matrix_q4 = (
    spark.createDataFrame(corr_entries_q4, ["var_1", "var_2", "correlation"])
    .groupBy("var_1")
    .pivot("var_2", cast(Any, corr_cols_q4))
    .agg(first("correlation"))
    .orderBy("var_1")
)

print("Matriz de correlacao:")
matrix_q4.show(truncate=False)

print("Top 3 pares com maior correlacao absoluta:")
spark.createDataFrame(
    top_pairs_q4, ["var_1", "var_2", "correlation", "abs_correlation"]
).orderBy(F.desc("abs_correlation")).show(3, truncate=False)


# %% [markdown]
# ## Q5 — Qualidade de Dados (2 pts)
# Crie um relatório de qualidade para CADA coluna contendo:
# - % de nulos
# - Contagem de valores distintos
# - Para numéricas: min, max, média, desvio padrão, skewness
# Exiba como um DataFrame organizado.

# %%
# Interpretacao: consolidar qualidade de dados por coluna para apoiar confiabilidade de pipeline.
# AGENT: ajuste de debug - agregacoes consolidadas evitam varredura repetida por coluna.
total_rows_q5 = df.count()
numeric_types_q5 = {
    "tinyint",
    "smallint",
    "int",
    "bigint",
    "float",
    "double",
    "decimal",
}

agg_exprs_q5 = []
for field in df.schema.fields:
    col_name = field.name
    dtype = field.dataType.simpleString()
    is_numeric = any(dtype.startswith(t) for t in numeric_types_q5)

    null_condition = F.col(col_name).isNull()
    if dtype == "string":
        null_condition = null_condition | (F.trim(F.col(col_name)) == "")

    agg_exprs_q5.append(
        F.sum(F.when(null_condition, 1).otherwise(0)).alias(f"{col_name}__null_count")
    )
    agg_exprs_q5.append(
        F.count_distinct(F.col(col_name)).alias(f"{col_name}__distinct_count")
    )

    if is_numeric:
        agg_exprs_q5.append(F.min(F.col(col_name)).alias(f"{col_name}__min"))
        agg_exprs_q5.append(F.max(F.col(col_name)).alias(f"{col_name}__max"))
        agg_exprs_q5.append(avg(F.col(col_name)).alias(f"{col_name}__mean"))
        agg_exprs_q5.append(stddev(F.col(col_name)).alias(f"{col_name}__stddev"))
        agg_exprs_q5.append(skewness(F.col(col_name)).alias(f"{col_name}__skewness"))

stats_row_q5 = df.agg(*agg_exprs_q5).first()
stats_q5 = cast(Any, stats_row_q5.asDict() if stats_row_q5 else {})

quality_rows_q5 = []
py_round_q5 = __import__("builtins").round
for field in df.schema.fields:
    col_name = field.name
    dtype = field.dataType.simpleString()
    is_numeric = any(dtype.startswith(t) for t in numeric_types_q5)

    quality_rows_q5.append(
        (
            col_name,
            dtype,
            py_round_q5(
                ((stats_q5.get(f"{col_name}__null_count") or 0) / total_rows_q5) * 100,
                4,
            ),
            int(stats_q5.get(f"{col_name}__distinct_count") or 0),
            (
                float(stats_q5.get(f"{col_name}__min"))
                if is_numeric and stats_q5.get(f"{col_name}__min") is not None
                else None
            ),
            (
                float(stats_q5.get(f"{col_name}__max"))
                if is_numeric and stats_q5.get(f"{col_name}__max") is not None
                else None
            ),
            (
                float(stats_q5.get(f"{col_name}__mean"))
                if is_numeric and stats_q5.get(f"{col_name}__mean") is not None
                else None
            ),
            (
                float(stats_q5.get(f"{col_name}__stddev"))
                if is_numeric and stats_q5.get(f"{col_name}__stddev") is not None
                else None
            ),
            (
                float(stats_q5.get(f"{col_name}__skewness"))
                if is_numeric and stats_q5.get(f"{col_name}__skewness") is not None
                else None
            ),
        )
    )

result_q5 = spark.createDataFrame(
    quality_rows_q5,
    [
        "column_name",
        "data_type",
        "null_pct",
        "distinct_count",
        "min",
        "max",
        "mean",
        "stddev",
        "skewness",
    ],
).orderBy("column_name")

result_q5.show(len(quality_rows_q5), truncate=False)


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
# Interpretacao: filtrar musicas acima da media do proprio artista e acima da mediana global de energy.
# AGENT: ajuste de debug - normalizacao de artists para artista individual corrige a granularidade semantica.
median_energy_q6 = df.approxQuantile("energy", [0.5], 0.001)[0]
artists_clean_q6 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_artist_level_q6 = (
    df.withColumn("artist_array", F.array_distinct(split(artists_clean_q6, r",\s*")))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

window_artist_q6 = Window.partitionBy("artist")

result_q6 = (
    df_artist_level_q6.withColumn(
        "artist_avg_pop", avg("popularity").over(window_artist_q6)
    )
    .filter(
        (F.col("popularity") > F.col("artist_avg_pop"))
        & (F.col("energy") > F.lit(median_energy_q6))
    )
    .select("name", F.col("artist").alias("artists"), "popularity", "artist_avg_pop")
    .orderBy(F.desc("popularity"), F.desc("artist_avg_pop"))
)

print(f"Mediana global de energy: {median_energy_q6:.4f}")
result_q6.show(20, truncate=False)


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
# Interpretacao: aplicar filtros dinamicos em loop evita duplicacao e facilita manutencao de regras.
filters_q7 = {
    "energy": (0.6, 0.9),
    "danceability": (0.5, 0.8),
    "valence": (0.4, 1.0),
    "popularity": (50, 100),
}

df_q7 = df
for metric, bounds in filters_q7.items():
    lower_bound, upper_bound = bounds
    df_q7 = df_q7.filter(F.col(metric).between(lower_bound, upper_bound))

print(f"Quantidade de musicas apos filtros: {df_q7.count()}")
df_q7.select(
    "name", "artists", "popularity", "energy", "danceability", "valence"
).orderBy(F.desc("popularity")).show(10, truncate=False)


# %% [markdown]
# ## Q8 — Anti-Join Lógico (2 pts)
# Encontre artistas que têm pelo menos 5 músicas mas **nenhuma** com popularity > 60.
# Esses são os "artistas prolíficos mas impopulares".
# Exiba artista, total de músicas e popularidade média, ordenado por total DESC.

# %%
# Interpretacao: usar left_anti garante semantica exata de "nenhuma musica acima de 60".
# AGENT: ajuste de debug - contagem por artista individual com countDistinct(id) evita distorcao por string agregada.
artists_clean_q8 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_artist_level_q8 = (
    df.withColumn("artist_array", F.array_distinct(split(artists_clean_q8, r",\s*")))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

artist_base_q8 = (
    df_artist_level_q8.groupBy("artist")
    .agg(
        F.count_distinct("id").alias("total_songs"),
        round(avg("popularity"), 2).alias("avg_popularity"),
    )
    .filter(F.col("total_songs") >= 5)
)

artists_with_hits_q8 = (
    df_artist_level_q8.filter(F.col("popularity") > 60).select("artist").distinct()
)

result_q8 = artist_base_q8.join(artists_with_hits_q8, "artist", "left_anti").orderBy(
    F.desc("total_songs"), F.asc("artist")
)

result_q8.select(
    F.col("artist").alias("artists"), "total_songs", "avg_popularity"
).show(50, truncate=False)


# %% [markdown]
# ## Q9 — Padrões em Nomes (2 pts)
# Use **regex** para encontrar músicas que:
# - Contêm parênteses no nome (ex: "feat.", "remix", "live")
# - Extraia o conteúdo dentro dos parênteses usando `regexp_extract`
# - Classifique: "feat" → "Featuring", "remix" → "Remix", "live" → "Live", outro → "Outro"
# Exiba a contagem por categoria e a popularidade média de cada.

# %%
# Interpretacao: extrair conteudo dos parenteses e classificar por padrao sem UDF para manter performance.
df_q9 = (
    df.filter(F.col("name").rlike(r"\([^)]*\)"))
    .withColumn("inside_parentheses", regexp_extract(F.col("name"), r"\(([^)]*)\)", 1))
    .withColumn("inside_lower", lower(F.col("inside_parentheses")))
    .withColumn(
        "category",
        when(F.col("inside_lower").contains("feat"), lit("Featuring"))
        .when(F.col("inside_lower").contains("remix"), lit("Remix"))
        .when(F.col("inside_lower").contains("live"), lit("Live"))
        .otherwise(lit("Outro")),
    )
)

result_q9 = (
    df_q9.groupBy("category")
    .agg(
        count("*").alias("tracks"),
        round(avg("popularity"), 2).alias("avg_popularity"),
    )
    .orderBy(F.desc("tracks"))
)

result_q9.show(truncate=False)


# %% [markdown]
# ## Q10 — Análise Temporal com Filtros (2 pts)
# Para cada ano, calcule quantas músicas foram lançadas e a popularidade média.
# Filtre apenas anos com pelo menos 50 músicas.
# Encontre o ano com **maior crescimento percentual** de popularidade média em
# relação ao ano anterior. Use Window Functions com `lag`.

# %%
# Interpretacao: medir crescimento percentual anual com lag sobre media anual de popularidade.
annual_q10 = (
    df.groupBy("year")
    .agg(
        count("*").alias("total_tracks"),
        round(avg("popularity"), 4).alias("avg_popularity"),
    )
    .filter(F.col("total_tracks") >= 50)
)

window_year_q10 = Window.orderBy("year")
result_q10 = (
    annual_q10.withColumn(
        "prev_avg_popularity", lag("avg_popularity").over(window_year_q10)
    )
    .withColumn(
        "growth_pct_vs_prev_year",
        when(
            F.col("prev_avg_popularity").isNull() | (F.col("prev_avg_popularity") == 0),
            lit(None).cast("double"),
        ).otherwise(
            (
                (F.col("avg_popularity") - F.col("prev_avg_popularity"))
                / F.col("prev_avg_popularity")
            )
            * 100.0
        ),
    )
    .orderBy("year")
)

result_q10.show(200, truncate=False)
print("Ano com maior crescimento percentual de popularidade media:")
result_q10.orderBy(F.desc("growth_pct_vs_prev_year")).show(1, truncate=False)


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
# Interpretacao: derivar mood por regras hierarquicas para classificacao deterministica.
df_q11 = df.withColumn(
    "mood",
    when(
        (F.col("energy") > 0.8) & (F.col("valence") > 0.7) & (F.col("tempo") > 120),
        lit("Euforico"),
    )
    .when(
        (F.col("energy") < 0.4)
        & (F.col("valence") < 0.3)
        & (F.col("acousticness") > 0.5),
        lit("Melancolico"),
    )
    .when(
        (F.col("energy") < 0.5)
        & (F.col("acousticness") > 0.6)
        & (F.col("instrumentalness") > 0.3),
        lit("Relaxante"),
    )
    .when(
        (F.col("danceability") > 0.7)
        & (F.col("energy") > 0.6)
        & F.col("tempo").between(100, 130),
        lit("Dancante"),
    )
    .when(
        (F.col("energy") > 0.7)
        & (F.col("loudness") > -5)
        & (F.col("speechiness") > 0.1),
        lit("Intenso"),
    )
    .otherwise(lit("Neutro")),
)

result_q11 = (
    df_q11.groupBy("mood")
    .agg(
        count("*").alias("tracks"),
        round(avg("popularity"), 2).alias("avg_popularity"),
    )
    .orderBy(F.desc("tracks"))
)

result_q11.show(truncate=False)


# %% [markdown]
# ## Q12 — Normalização Min-Max (2 pts)
# Normalize as colunas `popularity`, `energy`, `danceability`, `tempo` e `loudness`
# para o intervalo [0, 1] usando a fórmula: `(valor - min) / (max - min)`.
# Calcule o min e max globais e aplique a transformação.
# Crie uma coluna `overall_score` = média das 5 colunas normalizadas.
# Exiba as 10 músicas com maior `overall_score`.

# %%
# Interpretacao: normalizar por min-max global preserva escala relativa entre features heterogeneas.
norm_cols_q12 = ["popularity", "energy", "danceability", "tempo", "loudness"]

stats_expr_q12 = []
for c in norm_cols_q12:
    stats_expr_q12.append(F.min(c).alias(f"{c}_min"))
    stats_expr_q12.append(F.max(c).alias(f"{c}_max"))
stats_row_q12 = df.agg(*stats_expr_q12).first()
stats_q12 = cast(Any, stats_row_q12.asDict() if stats_row_q12 else {})

df_q12 = df
norm_feature_cols_q12 = []
for c in norm_cols_q12:
    c_min = float(stats_q12[f"{c}_min"])
    c_max = float(stats_q12[f"{c}_max"])
    denom = c_max - c_min
    norm_col = f"{c}_norm"
    norm_feature_cols_q12.append(norm_col)

    if denom == 0:
        df_q12 = df_q12.withColumn(norm_col, lit(0.0))
    else:
        df_q12 = df_q12.withColumn(norm_col, (F.col(c) - F.lit(c_min)) / F.lit(denom))

overall_expr_q12 = F.lit(0.0)
for c in norm_feature_cols_q12:
    overall_expr_q12 = overall_expr_q12 + F.col(c)
overall_expr_q12 = overall_expr_q12 / F.lit(len(norm_feature_cols_q12))
df_q12 = df_q12.withColumn("overall_score", overall_expr_q12)

df_q12.select("name", "artists", "overall_score", *norm_feature_cols_q12).orderBy(
    F.desc("overall_score")
).show(10, truncate=False)


# %% [markdown]
# ## Q13 — Binning Avançado com Quantis (2 pts)
# Divida `popularity` em 5 bins de **igual frequência** (não igual largura) usando `ntile`.
# Para cada bin, calcule:
# - Quantidade de músicas
# - Ranges de popularity (min–max)
# - Médias de energy, danceability e valence
# Exiba o resultado como um "perfil" de cada bin.

# %%
# Interpretacao: ntile(5) produz bins de frequencia similar e evita distorcao por distribuicao assimetrica.
window_ntile_q13 = Window.orderBy(F.col("popularity"))
df_q13 = df.withColumn("pop_bin", ntile(5).over(window_ntile_q13))

result_q13 = (
    df_q13.groupBy("pop_bin")
    .agg(
        count("*").alias("tracks"),
        min("popularity").alias("pop_min"),
        max("popularity").alias("pop_max"),
        round(avg("energy"), 4).alias("avg_energy"),
        round(avg("danceability"), 4).alias("avg_danceability"),
        round(avg("valence"), 4).alias("avg_valence"),
    )
    .orderBy("pop_bin")
)

result_q13.show(truncate=False)


# %% [markdown]
# ## Q14 — Feature Engineering: Interações (2 pts)
# Crie as seguintes features de interação:
# - `dance_energy_ratio` = danceability / (energy + 0.001)
# - `acoustic_electronic` = acousticness * (1 - energy)
# - `mood_score` = (valence * 0.5 + danceability * 0.3 + energy * 0.2)
# - `vocal_instrumental` = speechiness / (instrumentalness + 0.001)
# Encontre a feature que tem maior correlação com `popularity`.

# %%
# Interpretacao: features de interacao capturam relacoes nao lineares entre atributos sonoros.
df_q14 = (
    df.withColumn(
        "dance_energy_ratio", F.col("danceability") / (F.col("energy") + F.lit(0.001))
    )
    .withColumn(
        "acoustic_electronic", F.col("acousticness") * (F.lit(1.0) - F.col("energy"))
    )
    .withColumn(
        "mood_score",
        (F.col("valence") * F.lit(0.5))
        + (F.col("danceability") * F.lit(0.3))
        + (F.col("energy") * F.lit(0.2)),
    )
    .withColumn(
        "vocal_instrumental",
        F.col("speechiness") / (F.col("instrumentalness") + F.lit(0.001)),
    )
)

feature_cols_q14 = [
    "dance_energy_ratio",
    "acoustic_electronic",
    "mood_score",
    "vocal_instrumental",
]
feature_corr_rows_q14 = []
for feat in feature_cols_q14:
    corr_row_q14 = cast(
        Any,
        df_q14.select(corr("popularity", feat).alias("corr_val")).first()
        or {"corr_val": 0.0},
    )
    corr_val = corr_row_q14["corr_val"]
    feature_corr_rows_q14.append((feat, float(corr_val), abs(float(corr_val))))

result_q14 = spark.createDataFrame(
    feature_corr_rows_q14, ["feature", "corr_with_popularity", "abs_corr"]
).orderBy(F.desc("abs_corr"))

result_q14.show(truncate=False)
print("Feature com maior correlacao absoluta com popularity:")
result_q14.show(1, truncate=False)


# %% [markdown]
# ## Q15 — Tratamento de Artistas Múltiplos (2 pts)
# Muitas músicas têm vários artistas separados por vírgula na coluna `artists`.
# - Use `split` e `explode` para criar uma linha por artista
# - Remova espaços extras com `trim`
# - Encontre os 10 artistas com mais **colaborações** (aparições em músicas com outros artistas)
# - Calcule a popularidade média das collabs vs. músicas solo de cada um desses artistas

# %%
# Interpretacao: split/explode transforma lista textual de artistas em grafo tabular analisavel.
artists_clean_q15 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_q15 = (
    df.withColumn("artist_array", split(artists_clean_q15, r",\s*"))
    .withColumn("num_artists_track", F.size("artist_array"))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

top_collab_artists_q15 = (
    df_q15.filter(F.col("num_artists_track") > 1)
    .groupBy("artist")
    .agg(count("*").alias("collab_appearances"))
    .orderBy(F.desc("collab_appearances"), F.asc("artist"))
    .limit(10)
)

pop_compare_q15 = (
    df_q15.join(top_collab_artists_q15.select("artist"), "artist", "inner")
    .groupBy("artist")
    .agg(
        round(avg(when(F.col("num_artists_track") > 1, F.col("popularity"))), 2).alias(
            "avg_pop_collab"
        ),
        round(avg(when(F.col("num_artists_track") == 1, F.col("popularity"))), 2).alias(
            "avg_pop_solo"
        ),
    )
)

result_q15 = top_collab_artists_q15.join(pop_compare_q15, "artist", "left").orderBy(
    F.desc("collab_appearances"), F.asc("artist")
)
result_q15.show(truncate=False)


# %% [markdown]
# ## Q16 — Detecção de Duplicatas Fuzzy (2 pts)
# Encontre possíveis músicas duplicadas:
# - Normalize `name` (lowercase, remova caracteres especiais, trim)
# - Agrupe por nome normalizado
# - Filtre grupos com mais de 1 ocorrência
# - Para cada grupo, exiba as variações e diferenças de popularidade
# Quantos grupos de possíveis duplicatas existem?

# %%
# Interpretacao: normalizacao fuzzy do nome reduz ruido de pontuacao/case para detectar duplicidade potencial.
normalized_name_q16 = trim(
    regexp_replace(
        regexp_replace(lower(F.col("name")), r"[^a-z0-9\s]+", " "),
        r"\s+",
        " ",
    )
)

result_q16 = (
    df.withColumn("name_normalized", normalized_name_q16)
    .groupBy("name_normalized")
    .agg(
        count("*").alias("occurrences"),
        collect_set("name").alias("name_variants"),
        min("popularity").alias("min_popularity"),
        max("popularity").alias("max_popularity"),
    )
    .withColumn("popularity_diff", F.col("max_popularity") - F.col("min_popularity"))
    .filter(F.col("occurrences") > 1)
    .orderBy(F.desc("occurrences"), F.desc("popularity_diff"))
)

print(f"Grupos de possiveis duplicatas: {result_q16.count()}")
result_q16.show(30, truncate=False)


# %% [markdown]
# ## Q17 — Encoding de Variáveis Categóricas (2 pts)
# Crie uma UDF que faz **one-hot encoding** manual da coluna `key` (0–11).
# O resultado deve ser um array de 12 elementos (0s e 1s).
# Aplique e verifique que a soma de cada array é sempre 1.
# Depois, "expanda" o array em 12 colunas separadas: `key_0`, `key_1`, ..., `key_11`.


# %%
# Interpretacao: UDF exigida para one-hot manual; expansao em colunas facilita uso em modelos/analytics.
# AGENT: ajuste de debug - validacao explicita de dominio de key evita falso positivo na regra soma=1.
@udf(ArrayType(IntegerType()))
def one_hot_key_q17(key_value):
    vec = [0] * 12
    if key_value is None:
        return vec
    key_int = int(key_value)
    if 0 <= key_int <= 11:
        vec[key_int] = 1
    return vec


df_q17 = (
    df.withColumn("key_ohe", one_hot_key_q17(F.col("key")))
    .withColumn("ohe_sum", F.expr("aggregate(key_ohe, 0, (acc, x) -> acc + x)"))
    .withColumn("is_valid_key", F.col("key").between(0, 11))
)

print("Validacao da soma do array one-hot por status de dominio da key:")
df_q17.groupBy("is_valid_key", "ohe_sum").count().orderBy(
    "is_valid_key", "ohe_sum"
).show()

invalid_rows_q17 = df_q17.filter(~F.col("is_valid_key")).count()
invalid_sum_q17 = df_q17.filter(F.col("is_valid_key") & (F.col("ohe_sum") != 1)).count()
print(f"Registros com key fora de [0,11]: {invalid_rows_q17}")
print(f"Registros validos com soma != 1 (esperado 0): {invalid_sum_q17}")

for i in range(12):
    df_q17 = df_q17.withColumn(f"key_{i}", F.col("key_ohe")[i])

df_q17.select("key", "key_ohe", "ohe_sum", *[f"key_{i}" for i in range(12)]).show(
    10, truncate=False
)


# %% [markdown]
# ## Q18 — Cálculo de Z-Score por Grupo (2 pts)
# Para cada artista (com mínimo 5 músicas), calcule o **z-score** de popularity:
# `z = (popularity - media_artista) / std_artista`
# Use Window Functions para calcular média e desvio por artista inline.
# Encontre as 10 músicas com maior z-score absoluto (maiores "surpresas").
# Colunas: `name`, `artists`, `popularity`, `artist_avg`, `artist_std`, `z_score`.

# %%
# Interpretacao: z-score por artista destaca faixas muito acima/abaixo do padrao individual.
# AGENT: ajuste de debug - z-score calculado em nivel de artista individual apos explode de artists.
artists_clean_q18 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_artist_level_q18 = (
    df.withColumn("artist_array", F.array_distinct(split(artists_clean_q18, r",\s*")))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

window_artist_q18 = Window.partitionBy("artist")

df_q18 = (
    df_artist_level_q18.withColumn("artist_count", count("*").over(window_artist_q18))
    .withColumn("artist_avg", avg("popularity").over(window_artist_q18))
    .withColumn("artist_std", stddev("popularity").over(window_artist_q18))
    .filter(
        (F.col("artist_count") >= 5)
        & F.col("artist_std").isNotNull()
        & (F.col("artist_std") > 0)
    )
    .withColumn(
        "z_score", (F.col("popularity") - F.col("artist_avg")) / F.col("artist_std")
    )
)

df_q18.select(
    "name",
    F.col("artist").alias("artists"),
    "popularity",
    "artist_avg",
    "artist_std",
    "z_score",
).orderBy(F.desc(spark_abs(F.col("z_score")))).show(10, truncate=False)


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
# Interpretacao: pivot por decada e modo evidencia diferenca harmonica major/minor na popularidade.
df_q19 = df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))

pivot_q19 = (
    df_q19.groupBy("decade")
    .pivot("mode", [0, 1])
    .agg(round(avg("popularity"), 4))
    .orderBy("decade")
)

result_q19 = (
    pivot_q19.withColumnRenamed("0", "minor_avg_pop")
    .withColumnRenamed("1", "major_avg_pop")
    .withColumn(
        "major_minor_gap", spark_abs(F.col("major_avg_pop") - F.col("minor_avg_pop"))
    )
)

result_q19.show(200, truncate=False)
print("Decada com maior diferenca absoluta entre Major e Minor:")
result_q19.orderBy(F.desc("major_minor_gap")).show(1, truncate=False)


# %% [markdown]
# ## Q20 — Rollup e Cube (2 pts)
# Use `rollup` para calcular a popularidade média em 3 níveis:
# - Por década + explicit
# - Por década (subtotal)
# - Total geral
# Depois repita com `cube` e explique a diferença no resultado.
# Exiba ambos os resultados.

# %%
# Interpretacao: rollup gera hierarquia de subtotal; cube expande para todas combinacoes de agregacao.
df_q20 = df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))

rollup_q20 = (
    df_q20.rollup("decade", "explicit")
    .agg(round(avg("popularity"), 4).alias("avg_popularity"))
    .orderBy("decade", "explicit")
)

cube_q20 = (
    df_q20.cube("decade", "explicit")
    .agg(round(avg("popularity"), 4).alias("avg_popularity"))
    .orderBy("decade", "explicit")
)

print("Resultado com ROLLUP:")
rollup_q20.show(200, truncate=False)

print("Resultado com CUBE:")
cube_q20.show(200, truncate=False)

print(
    "Diferenca tecnica: rollup retorna caminho hierarquico (decada+explicit, decada, total), "
    "enquanto cube inclui tambem subtotal por explicit independente da decada."
)


# %% [markdown]
# ## Q21 — Top-N por Grupo (2 pts)
# Para cada década, encontre as **3 músicas mais populares** usando Window Functions
# com `row_number`. Exiba: década, ranking, name, artists, popularity.
# Depois, repita usando `dense_rank` — em quais décadas os resultados diferem?

# %%
# Interpretacao: row_number limita exatamente 3 por decada; dense_rank pode retornar mais em caso de empate.
# AGENT: ajuste de debug - dense_rank deve ordenar apenas por popularity para preservar semantica de empate.
df_q21 = df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))
window_row_q21 = Window.partitionBy("decade").orderBy(
    F.desc("popularity"), F.asc("name")
)
window_dense_q21 = Window.partitionBy("decade").orderBy(F.desc("popularity"))

top3_row_number_q21 = (
    df_q21.withColumn("ranking", row_number().over(window_row_q21))
    .filter(F.col("ranking") <= 3)
    .select("id", "decade", "ranking", "name", "artists", "popularity")
    .orderBy("decade", "ranking", F.desc("popularity"))
)

top3_dense_rank_q21 = (
    df_q21.withColumn("ranking", dense_rank().over(window_dense_q21))
    .filter(F.col("ranking") <= 3)
    .select("id", "decade", "ranking", "name", "artists", "popularity")
    .orderBy("decade", "ranking", F.desc("popularity"))
)

print("Top 3 por decada com row_number:")
top3_row_number_q21.show(300, truncate=False)

print("Top 3 por decada com dense_rank:")
top3_dense_rank_q21.show(300, truncate=False)

diff_decades_q21 = (
    top3_row_number_q21.select("decade", "id")
    .subtract(top3_dense_rank_q21.select("decade", "id"))
    .select("decade")
    .union(
        top3_dense_rank_q21.select("decade", "id")
        .subtract(top3_row_number_q21.select("decade", "id"))
        .select("decade")
    )
    .distinct()
    .orderBy("decade")
)

print("Decadas onde row_number e dense_rank diferem:")
diff_decades_q21.show(100, truncate=False)


# %% [markdown]
# ## Q22 — Análise de Tendência (2 pts)
# Para cada ano (com >= 30 músicas), calcule a média de `energy`, `acousticness`
# e `danceability`. Use Window Functions com `lag` para calcular a variação
# ano a ano de cada métrica. Identifique o ano com maior mudança simultânea
# nas 3 métricas (soma dos valores absolutos das variações).

# %%
# Interpretacao: medir variacao ano a ano em 3 metricas para detectar mudancas estruturais de estilo.
annual_q22 = (
    df.groupBy("year")
    .agg(
        count("*").alias("total_tracks"),
        round(avg("energy"), 6).alias("avg_energy"),
        round(avg("acousticness"), 6).alias("avg_acousticness"),
        round(avg("danceability"), 6).alias("avg_danceability"),
    )
    .filter(F.col("total_tracks") >= 30)
)

window_q22 = Window.orderBy("year")
result_q22 = (
    annual_q22.withColumn("lag_energy", lag("avg_energy").over(window_q22))
    .withColumn("lag_acousticness", lag("avg_acousticness").over(window_q22))
    .withColumn("lag_danceability", lag("avg_danceability").over(window_q22))
    .withColumn("delta_energy", F.col("avg_energy") - F.col("lag_energy"))
    .withColumn(
        "delta_acousticness", F.col("avg_acousticness") - F.col("lag_acousticness")
    )
    .withColumn(
        "delta_danceability", F.col("avg_danceability") - F.col("lag_danceability")
    )
    .withColumn(
        "simultaneous_change_score",
        spark_abs(F.col("delta_energy"))
        + spark_abs(F.col("delta_acousticness"))
        + spark_abs(F.col("delta_danceability")),
    )
    .orderBy("year")
)

result_q22.show(200, truncate=False)
print("Ano com maior mudanca simultanea nas 3 metricas:")
result_q22.orderBy(F.desc("simultaneous_change_score")).show(1, truncate=False)


# %% [markdown]
# ## Q23 — Entropia por Década (2 pts)
# Calcule a **entropia de Shannon** da distribuição de `key` (0–11) por década.
# Fórmula: `H = -Σ(p * log2(p))` onde p é probabilidade de cada key.
# Décadas com maior entropia têm distribuição mais uniforme de tons.
# Exiba década, entropia, e o tom dominante de cada década.

# %%
# Interpretacao: entropia de Shannon mede uniformidade da distribuicao de tons (key) por decada.
df_q23 = df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))

key_counts_q23 = df_q23.groupBy("decade", "key").agg(count("*").alias("key_count"))
totals_q23 = key_counts_q23.groupBy("decade").agg(
    sum("key_count").alias("decade_total")
)

entropy_q23 = (
    key_counts_q23.join(totals_q23, "decade", "inner")
    .withColumn("p", F.col("key_count") / F.col("decade_total"))
    .withColumn("p_log2_p", F.col("p") * (F.log(F.col("p")) / F.log(F.lit(2.0))))
    .groupBy("decade")
    .agg((-sum("p_log2_p")).alias("shannon_entropy"))
)

dominant_key_q23 = (
    key_counts_q23.withColumn(
        "rn",
        row_number().over(
            Window.partitionBy("decade").orderBy(F.desc("key_count"), F.asc("key"))
        ),
    )
    .filter(F.col("rn") == 1)
    .select("decade", F.col("key").alias("dominant_key"), "key_count")
)

result_q23 = (
    entropy_q23.join(dominant_key_q23, "decade", "inner")
    .select(
        "decade",
        round(F.col("shannon_entropy"), 6).alias("shannon_entropy"),
        "dominant_key",
        "key_count",
    )
    .orderBy("decade")
)

result_q23.show(200, truncate=False)


# %% [markdown]
# ## Q24 — Índice de Diversidade de Artistas (2 pts)
# Para cada década, calcule:
# - Total de artistas distintos
# - Total de músicas
# - Índice HHI (Herfindahl–Hirschman): `HHI = Σ(share_i²)` onde share_i = songs_artist_i / total
# Um HHI baixo indica maior diversidade. Qual década foi mais diversa?
# (Use aggregation + self-join ou collect_list para calcular.)

# %%
# Interpretacao: HHI baixo implica distribuicao menos concentrada de participacao entre artistas.
artists_clean_q24 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_q24 = (
    df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))
    .withColumn("artist_array", split(artists_clean_q24, r",\s*"))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

artist_counts_q24 = df_q24.groupBy("decade", "artist").agg(
    count("*").alias("artist_song_count")
)

totals_q24 = artist_counts_q24.groupBy("decade").agg(
    sum("artist_song_count").alias("total_artist_song_refs"),
    F.count_distinct("artist").alias("distinct_artists"),
)

hhi_q24 = (
    artist_counts_q24.join(totals_q24, "decade", "inner")
    .withColumn("share", F.col("artist_song_count") / F.col("total_artist_song_refs"))
    .groupBy("decade")
    .agg(sum(pow(F.col("share"), 2)).alias("hhi"))
)

song_totals_q24 = (
    df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))
    .groupBy("decade")
    .agg(count("*").alias("total_songs"))
)

result_q24 = (
    hhi_q24.join(totals_q24.select("decade", "distinct_artists"), "decade", "inner")
    .join(song_totals_q24, "decade", "inner")
    .select(
        "decade",
        "distinct_artists",
        "total_songs",
        round(F.col("hhi"), 8).alias("hhi"),
    )
    .orderBy("decade")
)

result_q24.show(200, truncate=False)
print("Decada mais diversa (menor HHI):")
result_q24.orderBy("hhi").show(1, truncate=False)


# %% [markdown]
# ## Q25 — Média Móvel e Suavização (2 pts)
# Calcule a **média móvel de 5 anos** da popularidade média anual.
# Use Window Functions com `rowsBetween(-2, 2)`.
# Compare a série original vs. suavizada — em quais períodos houve maior
# divergência? Exiba ano, pop_média, pop_suavizada, diferença.

# %%
# Interpretacao: media movel de 5 anos suaviza ruido anual sem perder tendencia estrutural.
annual_pop_q25 = (
    df.groupBy("year")
    .agg(round(avg("popularity"), 6).alias("pop_media"))
    .orderBy("year")
)

window_q25 = Window.orderBy("year").rowsBetween(-2, 2)
result_q25 = (
    annual_pop_q25.withColumn("pop_suavizada", avg("pop_media").over(window_q25))
    .withColumn("diferenca", spark_abs(F.col("pop_media") - F.col("pop_suavizada")))
    .orderBy("year")
)

result_q25.show(200, truncate=False)
print("Periodos com maior divergencia entre serie original e suavizada:")
result_q25.orderBy(F.desc("diferenca")).show(10, truncate=False)


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
# Interpretacao: SQL puro com CTE + subquery correlacionada para filtrar artistas consistentes acima da mediana.
# AGENT: ajuste de debug - CTE normalizada por artista individual com LATERAL VIEW EXPLODE para semantica correta.
df.createOrReplaceTempView("spotify")

result_q26 = spark.sql("""
    WITH normalized AS (
        SELECT
            id,
            popularity,
            TRIM(artist) AS artist
        FROM spotify
        LATERAL VIEW EXPLODE(
            SPLIT(
                REGEXP_REPLACE(REGEXP_REPLACE(artists, '^\\[|\\]$', ''), "'", ''),
                ',\\s*'
            )
        ) exploded AS artist
        WHERE TRIM(artist) <> ''
    ),
    global_stats AS (
        SELECT percentile_approx(popularity, 0.5, 10000) AS median_popularity
        FROM normalized
    ),
    artist_agg AS (
        SELECT
            artist,
            AVG(popularity) AS avg_popularity,
            COUNT(*) AS tracks_total,
            SUM(CASE WHEN popularity > 70 THEN 1 ELSE 0 END) AS hits_above_70
        FROM normalized
        GROUP BY artist
    )
    SELECT
        a.artist AS artists,
        ROUND(a.avg_popularity, 4) AS avg_popularity,
        a.tracks_total,
        a.hits_above_70
    FROM artist_agg a
    WHERE a.avg_popularity > (SELECT median_popularity FROM global_stats)
      AND EXISTS (
            SELECT 1
            FROM normalized s
            WHERE s.artist = a.artist
              AND s.popularity > 70
            GROUP BY s.artist
            HAVING COUNT(*) >= 3
      )
    ORDER BY avg_popularity DESC, tracks_total DESC
    LIMIT 15
    """)

result_q26.show(truncate=False)


# %% [markdown]
# ## Q27 — Window Functions em SQL (2 pts)
# Em SQL puro, para cada música calcule:
# - `rank_in_year`: ranking de popularidade dentro do seu ano
# - `pct_rank_overall`: percentil de popularidade geral
# - `running_avg`: média móvel de 3 anos da popularidade
# Exiba as músicas que são Top 3 do seu ano E estão no percentil 95+ geral.

# %%
# Interpretacao: SQL puro com windows para ranking anual, percent_rank global e media movel por ano.
df.createOrReplaceTempView("spotify")

result_q27 = spark.sql("""
    WITH ranked_tracks AS (
        SELECT
            id,
            name,
            artists,
            year,
            popularity,
            RANK() OVER (PARTITION BY year ORDER BY popularity DESC, name ASC) AS rank_in_year,
            PERCENT_RANK() OVER (ORDER BY popularity) AS pct_rank_overall
        FROM spotify
    ),
    yearly_avg AS (
        SELECT year, AVG(popularity) AS avg_pop_year
        FROM spotify
        GROUP BY year
    ),
    yearly_running AS (
        SELECT
            year,
            AVG(avg_pop_year) OVER (
                ORDER BY year
                ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
            ) AS running_avg
        FROM yearly_avg
    )
    SELECT
        r.id,
        r.name,
        r.artists,
        r.year,
        r.popularity,
        r.rank_in_year,
        r.pct_rank_overall,
        y.running_avg
    FROM ranked_tracks r
    LEFT JOIN yearly_running y
        ON r.year = y.year
    WHERE r.rank_in_year <= 3
      AND r.pct_rank_overall >= 0.95
    ORDER BY r.year, r.popularity DESC, r.name
    """)

result_q27.show(200, truncate=False)


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
# Interpretacao: CASE WHEN define eras e ranking por artista escolhe lider de popularidade em cada era.
df.createOrReplaceTempView("spotify")

result_q28 = spark.sql("""
    WITH base AS (
        SELECT
            *,
            CASE
                WHEN year < 1960 THEN 'Golden Age'
                WHEN year BETWEEN 1960 AND 1969 THEN 'British Invasion'
                WHEN year BETWEEN 1970 AND 1979 THEN 'Disco & Rock'
                WHEN year BETWEEN 1980 AND 1989 THEN 'Synth Pop'
                WHEN year BETWEEN 1990 AND 1999 THEN 'Grunge & Hip-Hop'
                WHEN year BETWEEN 2000 AND 2009 THEN 'Digital Revolution'
                WHEN year BETWEEN 2010 AND 2019 THEN 'Streaming Era'
                ELSE 'Post-Pandemic'
            END AS era_musical
        FROM spotify
    ),
    era_stats AS (
        SELECT
            era_musical,
            COUNT(*) AS total_tracks,
            AVG(energy) AS avg_energy,
            AVG(acousticness) AS avg_acousticness,
            AVG(popularity) AS avg_popularity,
            AVG(explicit) * 100.0 AS explicit_pct
        FROM base
        GROUP BY era_musical
    ),
    artist_rank AS (
        SELECT
            era_musical,
            artists,
            AVG(popularity) AS artist_avg_popularity,
            ROW_NUMBER() OVER (
                PARTITION BY era_musical
                ORDER BY AVG(popularity) DESC, artists ASC
            ) AS rn
        FROM base
        GROUP BY era_musical, artists
    )
    SELECT
        e.era_musical,
        e.total_tracks,
        ROUND(e.avg_energy, 4) AS avg_energy,
        ROUND(e.avg_acousticness, 4) AS avg_acousticness,
        ROUND(e.avg_popularity, 4) AS avg_popularity,
        ROUND(e.explicit_pct, 2) AS explicit_pct,
        a.artists AS top_artist,
        ROUND(a.artist_avg_popularity, 4) AS top_artist_avg_popularity
    FROM era_stats e
    LEFT JOIN artist_rank a
        ON e.era_musical = a.era_musical
       AND a.rn = 1
    ORDER BY e.era_musical
    """)

result_q28.show(200, truncate=False)


# %% [markdown]
# ## Q29 — Query Analítica com HAVING e Subquery (2 pts)
# Em SQL, encontre artistas "one-hit wonders": artistas com apenas 1 música
# com popularity > 70, mas cujas outras músicas têm popularity < 30.
# Mínimo de 3 músicas totais. Exiba artista, hit (nome e popularity),
# e média das outras músicas.

# %%
# Interpretacao: one-hit wonders exigem exatamente 1 hit alto e todo restante abaixo de 30.
# AGENT: ajuste de debug - regra aplicada em eixo de artista individual, nao na string composta de artists.
df.createOrReplaceTempView("spotify")

result_q29 = spark.sql("""
    WITH normalized AS (
        SELECT
            id,
            name,
            popularity,
            TRIM(artist) AS artist
        FROM spotify
        LATERAL VIEW EXPLODE(
            SPLIT(
                REGEXP_REPLACE(REGEXP_REPLACE(artists, '^\\[|\\]$', ''), "'", ''),
                ',\\s*'
            )
        ) exploded AS artist
        WHERE TRIM(artist) <> ''
    ),
    artist_flags AS (
        SELECT
            artist,
            COUNT(DISTINCT id) AS total_tracks,
            SUM(CASE WHEN popularity > 70 THEN 1 ELSE 0 END) AS high_hits,
            SUM(CASE WHEN popularity BETWEEN 30 AND 70 THEN 1 ELSE 0 END) AS mid_tracks
        FROM normalized
        GROUP BY artist
    ),
    eligible AS (
        SELECT artist
        FROM artist_flags
        WHERE total_tracks >= 3
          AND high_hits = 1
          AND mid_tracks = 0
    ),
    hit_track AS (
        SELECT
            s.artist,
            s.name AS hit_name,
            s.popularity AS hit_popularity
        FROM normalized s
        INNER JOIN eligible e ON s.artist = e.artist
        WHERE s.popularity > 70
    ),
    other_avg AS (
        SELECT
            s.artist,
            AVG(s.popularity) AS avg_other_popularity
        FROM normalized s
        INNER JOIN eligible e ON s.artist = e.artist
        WHERE s.popularity < 30
        GROUP BY s.artist
    )
    SELECT
        h.artist AS artists,
        h.hit_name,
        h.hit_popularity,
        ROUND(o.avg_other_popularity, 4) AS avg_other_popularity
    FROM hit_track h
    INNER JOIN other_avg o
        ON h.artist = o.artist
    ORDER BY h.hit_popularity DESC, o.avg_other_popularity DESC
    """)

result_q29.show(200, truncate=False)


# %% [markdown]
# ## Q30 — Query Recursiva Simulada (2 pts)
# Em SQL, simule uma análise de "caminho" de evolução musical:
# Para cada década, calcule a variação percentual de cada métrica
# (energy, danceability, acousticness, valence) em relação à década anterior.
# Depois calcule a variação acumulada desde a primeira década.
# Use LAG e SUM com Window Functions em SQL.

# %%
# Interpretacao: variacao percentual por decada via LAG e acumulado desde baseline usando SUM das deltas.
# AGENT: ajuste de debug - acumulado passa a ser calculado com baseline da primeira decada para evitar soma indevida de percentuais.
df.createOrReplaceTempView("spotify")

result_q30 = spark.sql("""
    WITH decade_stats AS (
        SELECT
            FLOOR(year / 10) * 10 AS decade,
            AVG(energy) AS avg_energy,
            AVG(danceability) AS avg_danceability,
            AVG(acousticness) AS avg_acousticness,
            AVG(valence) AS avg_valence
        FROM spotify
        GROUP BY FLOOR(year / 10) * 10
    ),
    lagged AS (
        SELECT
            decade,
            avg_energy,
            avg_danceability,
            avg_acousticness,
            avg_valence,
            LAG(avg_energy) OVER (ORDER BY decade) AS prev_energy,
            LAG(avg_danceability) OVER (ORDER BY decade) AS prev_danceability,
            LAG(avg_acousticness) OVER (ORDER BY decade) AS prev_acousticness,
            LAG(avg_valence) OVER (ORDER BY decade) AS prev_valence
        FROM decade_stats
    ),
    deltas AS (
        SELECT
            decade,
            avg_energy,
            avg_danceability,
            avg_acousticness,
            avg_valence,
            (avg_energy - prev_energy) AS delta_energy,
            (avg_danceability - prev_danceability) AS delta_danceability,
            (avg_acousticness - prev_acousticness) AS delta_acousticness,
            (avg_valence - prev_valence) AS delta_valence,
            CASE WHEN prev_energy IS NULL OR prev_energy = 0 THEN NULL
                 ELSE ((avg_energy - prev_energy) / prev_energy) * 100 END AS energy_pct_change,
            CASE WHEN prev_danceability IS NULL OR prev_danceability = 0 THEN NULL
                 ELSE ((avg_danceability - prev_danceability) / prev_danceability) * 100 END AS danceability_pct_change,
            CASE WHEN prev_acousticness IS NULL OR prev_acousticness = 0 THEN NULL
                 ELSE ((avg_acousticness - prev_acousticness) / prev_acousticness) * 100 END AS acousticness_pct_change,
            CASE WHEN prev_valence IS NULL OR prev_valence = 0 THEN NULL
                 ELSE ((avg_valence - prev_valence) / prev_valence) * 100 END AS valence_pct_change
        FROM lagged
    ),
    cumulative AS (
        SELECT
            decade,
            avg_energy,
            avg_danceability,
            avg_acousticness,
            avg_valence,
            energy_pct_change,
            danceability_pct_change,
            acousticness_pct_change,
            valence_pct_change,
            SUM(COALESCE(delta_energy, 0.0)) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS energy_delta_cumulative,
            SUM(COALESCE(delta_danceability, 0.0)) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS danceability_delta_cumulative,
            SUM(COALESCE(delta_acousticness, 0.0)) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS acousticness_delta_cumulative,
            SUM(COALESCE(delta_valence, 0.0)) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS valence_delta_cumulative,
            FIRST_VALUE(avg_energy) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS base_energy,
            FIRST_VALUE(avg_danceability) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS base_danceability,
            FIRST_VALUE(avg_acousticness) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS base_acousticness,
            FIRST_VALUE(avg_valence) OVER (
                ORDER BY decade
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS base_valence
        FROM deltas
    )
    SELECT
        decade,
        ROUND(avg_energy, 6) AS avg_energy,
        ROUND(avg_danceability, 6) AS avg_danceability,
        ROUND(avg_acousticness, 6) AS avg_acousticness,
        ROUND(avg_valence, 6) AS avg_valence,
        ROUND(energy_pct_change, 6) AS energy_pct_change,
        ROUND(danceability_pct_change, 6) AS danceability_pct_change,
        ROUND(acousticness_pct_change, 6) AS acousticness_pct_change,
        ROUND(valence_pct_change, 6) AS valence_pct_change,
        ROUND(
            CASE WHEN base_energy IS NULL OR base_energy = 0 THEN NULL
                 ELSE (energy_delta_cumulative / base_energy) * 100 END,
            6
        ) AS energy_pct_cumulative,
        ROUND(
            CASE WHEN base_danceability IS NULL OR base_danceability = 0 THEN NULL
                 ELSE (danceability_delta_cumulative / base_danceability) * 100 END,
            6
        ) AS danceability_pct_cumulative,
        ROUND(
            CASE WHEN base_acousticness IS NULL OR base_acousticness = 0 THEN NULL
                 ELSE (acousticness_delta_cumulative / base_acousticness) * 100 END,
            6
        ) AS acousticness_pct_cumulative,
        ROUND(
            CASE WHEN base_valence IS NULL OR base_valence = 0 THEN NULL
                 ELSE (valence_delta_cumulative / base_valence) * 100 END,
            6
        ) AS valence_pct_cumulative
    FROM cumulative
    ORDER BY decade
    """)

result_q30.show(200, truncate=False)


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
# Interpretacao: percent_rank normaliza cada criterio e permite score composto comparavel.
window_pop_q31 = Window.orderBy("popularity")
window_energy_q31 = Window.orderBy("energy")
window_dance_q31 = Window.orderBy("danceability")

result_q31 = (
    df.withColumn("prank_pop", percent_rank().over(window_pop_q31))
    .withColumn("prank_energy", percent_rank().over(window_energy_q31))
    .withColumn("prank_dance", percent_rank().over(window_dance_q31))
    .withColumn(
        "composite_score",
        (F.col("prank_pop") * F.lit(0.4))
        + (F.col("prank_energy") * F.lit(0.3))
        + (F.col("prank_dance") * F.lit(0.3)),
    )
    .select(
        "name",
        "artists",
        "popularity",
        "energy",
        "danceability",
        "prank_pop",
        "prank_energy",
        "prank_dance",
        "composite_score",
    )
    .orderBy(F.desc("composite_score"), F.desc("popularity"))
)

result_q31.show(15, truncate=False)


# %% [markdown]
# ## Q32 — Sessões de Escuta Simuladas (2 pts)
# Ordene músicas por popularidade DESC. Simule "sessões" onde cada sessão
# tem no máximo 30 minutos de música (use `duration_ms`).
# Use Window Functions com soma cumulativa de duração e incremento de sessão
# quando a soma exceder 30 min (1800000 ms).
# Quantas sessões seriam necessárias? Qual sessão tem maior popularity média?

# %%
# Interpretacao: sessao deriva de soma cumulativa de duracao; floor mapeia blocos de 30 minutos.
window_q32 = Window.orderBy(F.desc("popularity"), F.asc("id")).rowsBetween(
    Window.unboundedPreceding, 0
)

df_q32 = (
    df.select("id", "name", "artists", "popularity", "duration_ms")
    .withColumn("cumulative_duration_ms", sum("duration_ms").over(window_q32))
    .withColumn(
        "session_id",
        (
            floor((F.col("cumulative_duration_ms") - F.lit(1)) / F.lit(1800000))
            + F.lit(1)
        ).cast("int"),
    )
)

total_sessions_row_q32 = cast(
    Any,
    df_q32.agg(max("session_id").alias("total_sessions")).first()
    or {"total_sessions": 0},
)
total_sessions_q32 = total_sessions_row_q32["total_sessions"]
session_stats_q32 = (
    df_q32.groupBy("session_id")
    .agg(
        count("*").alias("tracks"),
        round(sum("duration_ms") / 60000.0, 2).alias("total_minutes"),
        round(avg("popularity"), 4).alias("avg_popularity"),
    )
    .orderBy("session_id")
)

print(f"Total de sessoes necessarias: {int(total_sessions_q32)}")
session_stats_q32.show(100, truncate=False)
print("Sessao com maior popularidade media:")
session_stats_q32.orderBy(F.desc("avg_popularity")).show(1, truncate=False)


# %% [markdown]
# ## Q33 — Gaps and Islands (2 pts)
# Considere os anos com pelo menos 1 música no dataset.
# Identifique "ilhas" de anos consecutivos e "gaps" (anos sem músicas).
# Use a técnica de row_number para agrupar anos consecutivos.
# Para cada ilha: ano_inicio, ano_fim, total_musicas, pop_media.
# Para cada gap: ano_inicio_gap, ano_fim_gap, duração.

# %%
# Interpretacao: tecnica year-row_number identifica ilhas consecutivas; lag identifica gaps.
years_q33 = (
    df.groupBy("year")
    .agg(
        count("*").alias("songs_in_year"),
        round(avg("popularity"), 6).alias("avg_pop_year"),
    )
    .orderBy("year")
)

window_year_q33 = Window.orderBy("year")
islands_base_q33 = years_q33.withColumn(
    "rn", row_number().over(window_year_q33)
).withColumn("island_key", F.col("year") - F.col("rn"))

islands_q33 = (
    islands_base_q33.groupBy("island_key")
    .agg(
        min("year").alias("ano_inicio"),
        max("year").alias("ano_fim"),
        sum("songs_in_year").alias("total_musicas"),
        (
            sum(F.col("avg_pop_year") * F.col("songs_in_year")) / sum("songs_in_year")
        ).alias("pop_media"),
    )
    .select(
        "ano_inicio",
        "ano_fim",
        "total_musicas",
        round("pop_media", 6).alias("pop_media"),
    )
    .orderBy("ano_inicio")
)

gaps_q33 = (
    years_q33.withColumn("prev_year", lag("year").over(window_year_q33))
    .filter((F.col("year") - F.col("prev_year")) > 1)
    .select(
        (F.col("prev_year") + 1).alias("ano_inicio_gap"),
        (F.col("year") - 1).alias("ano_fim_gap"),
        (F.col("year") - F.col("prev_year") - 1).alias("duracao"),
    )
    .orderBy("ano_inicio_gap")
)

print("Ilhas de anos consecutivos:")
islands_q33.show(200, truncate=False)
print("Gaps (anos ausentes):")
gaps_q33.show(200, truncate=False)


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
# Interpretacao: combinar lag/lead com janela local captura momentum e consistencia temporal por artista.
# AGENT: ajuste de debug - ordenacao estavel e granularidade por artista individual melhoram reprodutibilidade.
artists_clean_q34 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_artist_level_q34 = (
    df.withColumn("artist_array", F.array_distinct(split(artists_clean_q34, r",\s*")))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

artists_min5_q34 = (
    df_artist_level_q34.groupBy("artist")
    .agg(F.count_distinct("id").alias("artist_tracks"))
    .filter(F.col("artist_tracks") >= 5)
)

window_artist_order_q34 = Window.partitionBy("artist").orderBy(
    F.asc("year"), F.asc("name"), F.asc("id")
)
window_artist_3rows_q34 = window_artist_order_q34.rowsBetween(-1, 1)

df_q34 = (
    df_artist_level_q34.join(artists_min5_q34.select("artist"), "artist", "inner")
    .withColumn("prev_pop", lag("popularity").over(window_artist_order_q34))
    .withColumn("next_pop", lead("popularity").over(window_artist_order_q34))
    .withColumn("momentum", F.col("next_pop") - F.col("prev_pop"))
    .withColumn("consistency", stddev("popularity").over(window_artist_3rows_q34))
)

artist_momentum_q34 = (
    df_q34.groupBy("artist")
    .agg(
        round(avg("momentum"), 6).alias("avg_momentum"),
        round(avg("consistency"), 6).alias("avg_consistency"),
        count("*").alias("tracks_considered"),
    )
    .orderBy(F.desc("avg_momentum"), F.desc("tracks_considered"))
)

artist_momentum_q34.select(
    F.col("artist").alias("artists"),
    "avg_momentum",
    "avg_consistency",
    "tracks_considered",
).show(5, truncate=False)


# %% [markdown]
# ## Q35 — Ntile e Segmentação (2 pts)
# Divida as músicas em 10 decis de popularidade usando `ntile(10)`.
# Para cada decil, calcule o "perfil sonoro" médio (energy, danceability,
# acousticness, valence, tempo, loudness).
# Identifique quais atributos mudam mais entre o decil 1 (menos popular)
# e o decil 10 (mais popular). Use a variação percentual.

# %%
# Interpretacao: decis por ntile(10) criam segmentos equiprovaveis para comparar perfis sonoros.
window_q35 = Window.orderBy("popularity")
df_q35 = df.withColumn("decil", ntile(10).over(window_q35))

profile_q35 = (
    df_q35.groupBy("decil")
    .agg(
        round(avg("energy"), 6).alias("avg_energy"),
        round(avg("danceability"), 6).alias("avg_danceability"),
        round(avg("acousticness"), 6).alias("avg_acousticness"),
        round(avg("valence"), 6).alias("avg_valence"),
        round(avg("tempo"), 6).alias("avg_tempo"),
        round(avg("loudness"), 6).alias("avg_loudness"),
    )
    .orderBy("decil")
)

profile_q35.show(20, truncate=False)

extremes_q35 = profile_q35.agg(
    max(when(F.col("decil") == 1, F.col("avg_energy"))).alias("d1_energy"),
    max(when(F.col("decil") == 10, F.col("avg_energy"))).alias("d10_energy"),
    max(when(F.col("decil") == 1, F.col("avg_danceability"))).alias("d1_danceability"),
    max(when(F.col("decil") == 10, F.col("avg_danceability"))).alias(
        "d10_danceability"
    ),
    max(when(F.col("decil") == 1, F.col("avg_acousticness"))).alias("d1_acousticness"),
    max(when(F.col("decil") == 10, F.col("avg_acousticness"))).alias(
        "d10_acousticness"
    ),
    max(when(F.col("decil") == 1, F.col("avg_valence"))).alias("d1_valence"),
    max(when(F.col("decil") == 10, F.col("avg_valence"))).alias("d10_valence"),
    max(when(F.col("decil") == 1, F.col("avg_tempo"))).alias("d1_tempo"),
    max(when(F.col("decil") == 10, F.col("avg_tempo"))).alias("d10_tempo"),
    max(when(F.col("decil") == 1, F.col("avg_loudness"))).alias("d1_loudness"),
    max(when(F.col("decil") == 10, F.col("avg_loudness"))).alias("d10_loudness"),
)

variation_q35 = extremes_q35.select(
    when(spark_abs(F.col("d1_energy")) == 0, lit(None))
    .otherwise(
        ((F.col("d10_energy") - F.col("d1_energy")) / spark_abs(F.col("d1_energy")))
        * 100
    )
    .alias("energy_pct_change"),
    when(spark_abs(F.col("d1_danceability")) == 0, lit(None))
    .otherwise(
        (
            (F.col("d10_danceability") - F.col("d1_danceability"))
            / spark_abs(F.col("d1_danceability"))
        )
        * 100
    )
    .alias("danceability_pct_change"),
    when(spark_abs(F.col("d1_acousticness")) == 0, lit(None))
    .otherwise(
        (
            (F.col("d10_acousticness") - F.col("d1_acousticness"))
            / spark_abs(F.col("d1_acousticness"))
        )
        * 100
    )
    .alias("acousticness_pct_change"),
    when(spark_abs(F.col("d1_valence")) == 0, lit(None))
    .otherwise(
        ((F.col("d10_valence") - F.col("d1_valence")) / spark_abs(F.col("d1_valence")))
        * 100
    )
    .alias("valence_pct_change"),
    when(spark_abs(F.col("d1_tempo")) == 0, lit(None))
    .otherwise(
        ((F.col("d10_tempo") - F.col("d1_tempo")) / spark_abs(F.col("d1_tempo"))) * 100
    )
    .alias("tempo_pct_change"),
    when(spark_abs(F.col("d1_loudness")) == 0, lit(None))
    .otherwise(
        (
            (F.col("d10_loudness") - F.col("d1_loudness"))
            / spark_abs(F.col("d1_loudness"))
        )
        * 100
    )
    .alias("loudness_pct_change"),
)

variation_long_q35 = variation_q35.select(
    explode(
        F.create_map(
            lit("energy"),
            F.col("energy_pct_change"),
            lit("danceability"),
            F.col("danceability_pct_change"),
            lit("acousticness"),
            F.col("acousticness_pct_change"),
            lit("valence"),
            F.col("valence_pct_change"),
            lit("tempo"),
            F.col("tempo_pct_change"),
            lit("loudness"),
            F.col("loudness_pct_change"),
        )
    ).alias("atributo", "variacao_pct_decil_1_para_10")
).orderBy(F.desc(spark_abs(F.col("variacao_pct_decil_1_para_10"))))

variation_long_q35.show(truncate=False)


# %% [markdown]
# ## Q36 — Cume_Dist e Percentis (2 pts)
# Calcule o `cume_dist()` e `percent_rank()` para popularity.
# Encontre músicas que estão no Top 1% de popularidade.
# Para essas músicas "elite", compare suas médias de energy, danceability,
# acousticness e valence com as médias do Bottom 50%.
# Exiba as diferenças como um DataFrame comparativo.

# %%
# Interpretacao: comparar elite (top 1%) vs metade inferior ajuda identificar assinatura sonora de popularidade extrema.
window_q36 = Window.orderBy("popularity")
df_q36 = df.withColumn("cume_dist_popularity", cume_dist().over(window_q36)).withColumn(
    "percent_rank_popularity", percent_rank().over(window_q36)
)

elite_q36 = df_q36.filter(F.col("cume_dist_popularity") >= 0.99)
bottom50_q36 = df_q36.filter(F.col("percent_rank_popularity") <= 0.50)

metrics_q36 = ["energy", "danceability", "acousticness", "valence"]
elite_row_q36 = elite_q36.agg(*[avg(m).alias(m) for m in metrics_q36]).first()
bottom_row_q36 = bottom50_q36.agg(*[avg(m).alias(m) for m in metrics_q36]).first()
elite_avg_q36 = cast(Any, elite_row_q36.asDict() if elite_row_q36 else {})
bottom_avg_q36 = cast(Any, bottom_row_q36.asDict() if bottom_row_q36 else {})

comparison_rows_q36 = []
for m in metrics_q36:
    elite_val = float(elite_avg_q36[m]) if elite_avg_q36[m] is not None else None
    bottom_val = float(bottom_avg_q36[m]) if bottom_avg_q36[m] is not None else None
    diff_val = (
        (elite_val - bottom_val)
        if (elite_val is not None and bottom_val is not None)
        else None
    )
    comparison_rows_q36.append((m, elite_val, bottom_val, diff_val))

result_q36 = spark.createDataFrame(
    comparison_rows_q36,
    [
        "metric",
        "elite_top_1pct_avg",
        "bottom_50pct_avg",
        "difference_elite_minus_bottom",
    ],
).orderBy("metric")

print(f"Quantidade de musicas elite (Top 1%): {elite_q36.count()}")
print(f"Quantidade de musicas Bottom 50%: {bottom50_q36.count()}")
result_q36.show(truncate=False)


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
# Interpretacao: UDF aplicada por requisito explicito; mantida enxuta para reduzir overhead Python.
@udf(returnType=StringType())
def classify_track_q37(energy, valence, tempo, acousticness, danceability):
    if energy is None or valence is None or tempo is None:
        return "Unknown | Unknown | Unknown"

    energy_label = "High Energy" if energy > 0.5 else "Low Energy"
    valence_label = "Positive" if valence > 0.5 else "Negative"
    tempo_label = "Fast" if tempo > 120 else "Slow"
    return f"{energy_label} | {valence_label} | {tempo_label}"


df_q37 = df.withColumn(
    "track_classification",
    classify_track_q37(
        F.col("energy"),
        F.col("valence"),
        F.col("tempo"),
        F.col("acousticness"),
        F.col("danceability"),
    ),
)

result_q37 = (
    df_q37.groupBy("track_classification")
    .agg(
        count("*").alias("tracks"),
        round(avg("popularity"), 4).alias("avg_popularity"),
    )
    .orderBy(F.desc("tracks"))
)

result_q37.show(50, truncate=False)


# %% [markdown]
# ## Q38 — UDF que Retorna Struct (2 pts)
# Crie uma UDF que recebe `duration_ms` e `tempo` e retorna um StructType com:
# - `minutes`: duração em minutos (float)
# - `beats_total`: número estimado de batidas (tempo * minutes)
# - `category`: "Short & Slow", "Short & Fast", "Long & Slow", "Long & Fast"
#   (threshold: 3.5 min, 120 bpm)
# Aplique e agrupe por category mostrando contagem e popularity média.

# %%
# Interpretacao: UDF retorna struct para encapsular multiplas derivacoes em uma unica transformacao.
duration_profile_schema_q38 = StructType(
    [
        StructField("minutes", DoubleType(), True),
        StructField("beats_total", DoubleType(), True),
        StructField("category", StringType(), True),
    ]
)


@udf(returnType=duration_profile_schema_q38)
def duration_profile_q38(duration_ms, tempo):
    if duration_ms is None or tempo is None:
        return (None, None, "Unknown")

    minutes = float(duration_ms) / 60000.0
    beats_total = float(tempo) * minutes

    duration_label = "Short" if minutes <= 3.5 else "Long"
    tempo_label = "Slow" if tempo <= 120 else "Fast"
    category = f"{duration_label} & {tempo_label}"

    return (minutes, beats_total, category)


df_q38 = df.withColumn(
    "duration_profile", duration_profile_q38(F.col("duration_ms"), F.col("tempo"))
)

result_q38 = (
    df_q38.withColumn("minutes", F.col("duration_profile.minutes"))
    .withColumn("beats_total", F.col("duration_profile.beats_total"))
    .withColumn("category", F.col("duration_profile.category"))
    .groupBy("category")
    .agg(
        count("*").alias("tracks"),
        round(avg("popularity"), 4).alias("avg_popularity"),
        round(avg("minutes"), 4).alias("avg_minutes"),
        round(avg("beats_total"), 4).alias("avg_beats_total"),
    )
    .orderBy(F.desc("tracks"))
)

result_q38.show(truncate=False)


# %% [markdown]
# ## Q39 — Collect_List e Análise de Arrays (2 pts)
# Para cada artista (com >= 5 músicas), colete todas as popularidades em um array
# usando `collect_list`. Crie uma UDF que recebe o array e calcula:
# - Mediana
# - Amplitude (max - min)
# - Coeficiente de variação (std/mean)
# Encontre os 10 artistas mais "inconsistentes" (maior CV).

# %%
# Interpretacao: collect_list por artista cria vetor de popularidades para estatisticas robustas customizadas.
artist_min5_q39 = (
    df.groupBy("artists")
    .agg(count("*").alias("artist_tracks"))
    .filter(F.col("artist_tracks") >= 5)
)

stats_schema_q39 = StructType(
    [
        StructField("median_popularity", DoubleType(), True),
        StructField("amplitude", DoubleType(), True),
        StructField("cv", DoubleType(), True),
    ]
)


@udf(returnType=stats_schema_q39)
def array_stats_q39(values):
    if values is None:
        return (None, None, None)

    clean_vals = [float(v) for v in values if v is not None]
    n = len(clean_vals)
    if n == 0:
        return (None, None, None)

    clean_vals.sort()
    if n % 2 == 1:
        median_val = clean_vals[n // 2]
    else:
        median_val = (clean_vals[(n // 2) - 1] + clean_vals[n // 2]) / 2.0

    min_val = clean_vals[0]
    max_val = clean_vals[-1]
    amplitude_val = max_val - min_val

    import builtins

    mean_val = builtins.sum(clean_vals) / n
    variance_val = builtins.sum((x - mean_val) ** 2 for x in clean_vals) / n
    std_val = variance_val**0.5
    cv_val = (std_val / mean_val) if mean_val != 0 else None

    return (
        float(median_val),
        float(amplitude_val),
        float(cv_val) if cv_val is not None else None,
    )


artist_arrays_q39 = (
    df.join(artist_min5_q39.select("artists"), "artists", "inner")
    .groupBy("artists")
    .agg(collect_list("popularity").alias("popularity_array"))
)

result_q39 = (
    artist_arrays_q39.withColumn("stats", array_stats_q39(F.col("popularity_array")))
    .select(
        "artists",
        F.col("stats.median_popularity").alias("median_popularity"),
        F.col("stats.amplitude").alias("amplitude"),
        F.col("stats.cv").alias("cv"),
    )
    .orderBy(F.desc("cv"))
)

result_q39.show(10, truncate=False)


# %% [markdown]
# ## Q40 — MapType e Perfis (2 pts)
# Para cada década, crie um `MapType` contendo o "perfil sonoro":
# chaves = nomes das métricas (energy, danceability, etc.)
# valores = médias arredondadas.
# Use `create_map` ou `to_json` + UDF para construir.
# Exiba como JSON legível por década.

# %%
# Interpretacao: MapType + to_json entrega perfil sonoro compacto e facilmente consumivel por dashboard/API.
df_q40 = (
    df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))
    .groupBy("decade")
    .agg(
        round(avg("energy"), 4).alias("energy"),
        round(avg("danceability"), 4).alias("danceability"),
        round(avg("acousticness"), 4).alias("acousticness"),
        round(avg("valence"), 4).alias("valence"),
        round(avg("tempo"), 4).alias("tempo"),
        round(avg("loudness"), 4).alias("loudness"),
    )
)

result_q40 = (
    df_q40.withColumn(
        "sound_profile_map",
        F.create_map(
            lit("energy"),
            F.col("energy"),
            lit("danceability"),
            F.col("danceability"),
            lit("acousticness"),
            F.col("acousticness"),
            lit("valence"),
            F.col("valence"),
            lit("tempo"),
            F.col("tempo"),
            lit("loudness"),
            F.col("loudness"),
        ),
    )
    .withColumn("sound_profile_json", F.to_json(F.col("sound_profile_map")))
    .select("decade", "sound_profile_json")
    .orderBy("decade")
)

result_q40.show(200, truncate=False)


# %% [markdown]
# ## Q41 — UDF Vetorizada com Pandas (2 pts)
# Crie uma **Pandas UDF** (vectorized UDF) que normaliza uma coluna numérica
# usando z-score: `(x - mean) / std`. Aplique em `popularity`, `energy` e `danceability`.
# Compare o tempo de execução com uma UDF tradicional (row-at-a-time) para
# a mesma operação usando `spark.time()` ou medição manual.
# (Dica: `@pandas_udf(DoubleType())`)

import time

import pandas as pd
# %%
# Interpretacao: comparar Pandas UDF vs UDF tradicional para mesma tarefa de z-score em escala.
# AGENT: ajuste de debug - benchmark com warm-up e multiplas rodadas reduz ruido de medicao.
from pyspark.sql.functions import pandas_udf

cols_q41 = ["popularity", "energy", "danceability"]
stats_q41 = df.agg(
    *[avg(c).alias(f"{c}_mean") for c in cols_q41],
    *[stddev(c).alias(f"{c}_std") for c in cols_q41],
).first()
stats_q41 = cast(Any, stats_q41.asDict() if stats_q41 else {})


def make_pandas_z_udf_q41(mean_value, std_value):
    safe_std = float(std_value) if std_value not in [None, 0] else 1.0

    @pandas_udf(DoubleType())  # type: ignore[misc]
    def _z(series: pd.Series) -> pd.Series:
        return (series - float(mean_value)) / safe_std

    return _z


def make_row_z_udf_q41(mean_value, std_value):
    safe_std = float(std_value) if std_value not in [None, 0] else 1.0

    @udf(DoubleType())
    def _z(value):
        if value is None:
            return None
        return float((float(value) - float(mean_value)) / safe_std)

    return _z


pandas_udfs_q41 = {
    c: make_pandas_z_udf_q41(stats_q41[f"{c}_mean"], stats_q41[f"{c}_std"])
    for c in cols_q41
}
row_udfs_q41 = {
    c: make_row_z_udf_q41(stats_q41[f"{c}_mean"], stats_q41[f"{c}_std"])
    for c in cols_q41
}

df_pandas_q41 = df
for c in cols_q41:
    df_pandas_q41 = df_pandas_q41.withColumn(
        f"{c}_z_pandas", cast(Any, pandas_udfs_q41[c])(F.col(c))
    )
df_row_q41 = df
for c in cols_q41:
    df_row_q41 = df_row_q41.withColumn(f"{c}_z_row_udf", row_udfs_q41[c](F.col(c)))


def benchmark_q41(df_input, metric_cols, runs=4, warmup=1):
    timings = []
    for i in range(runs):
        start = time.perf_counter()
        df_input.select(*[avg(c).alias(c) for c in metric_cols]).first()
        elapsed = time.perf_counter() - start
        if i >= warmup:
            timings.append(elapsed)
    return timings


pandas_timing_runs_q41 = benchmark_q41(
    df_pandas_q41, [f"{c}_z_pandas" for c in cols_q41], runs=4, warmup=1
)
row_timing_runs_q41 = benchmark_q41(
    df_row_q41, [f"{c}_z_row_udf" for c in cols_q41], runs=4, warmup=1
)

pandas_median_q41 = sorted(pandas_timing_runs_q41)[len(pandas_timing_runs_q41) // 2]
row_median_q41 = sorted(row_timing_runs_q41)[len(row_timing_runs_q41) // 2]

print(f"Tempos Pandas UDF (s): {pandas_timing_runs_q41}")
print(f"Tempos UDF tradicional (s): {row_timing_runs_q41}")
print(f"Mediana Pandas UDF (s): {pandas_median_q41:.4f}")
print(f"Mediana UDF tradicional (s): {row_median_q41:.4f}")
print("Amostra de saida z-score (Pandas UDF):")
df_pandas_q41.select(
    "name", "popularity_z_pandas", "energy_z_pandas", "danceability_z_pandas"
).show(10, truncate=False)


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
# Interpretacao: self-join com pre-bucket reduz espaco de busca antes dos filtros de similaridade fina.
from pyspark.sql import functions as F
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import broadcast, floor

# --- base original (mantida) ---
df_sim_q42 = (
    df.select(
        "id",
        "name",
        "artists",
        "popularity",
        "energy",
        "danceability",
        "valence",
        "tempo",
    )
    .withColumn("energy_bucket", floor(F.col("energy") / 0.05))
    .withColumn("dance_bucket", floor(F.col("danceability") / 0.05))
    .withColumn("valence_bucket", floor(F.col("valence") / 0.05))
    .withColumn("tempo_bucket", floor(F.col("tempo") / 5.0))
)

# --- expansão controlada de vizinhança ---
offsets = [-1, 0, 1]

df_expanded = (
    df_sim_q42.select(
        "*",
        F.explode(F.array(*[F.lit(x) for x in offsets])).alias("e_off"),
        F.explode(F.array(*[F.lit(x) for x in offsets])).alias("d_off"),
        F.explode(F.array(*[F.lit(x) for x in offsets])).alias("v_off"),
        F.explode(F.array(*[F.lit(x) for x in offsets])).alias("t_off"),
    )
    .withColumn("energy_bucket_n", F.col("energy_bucket") + F.col("e_off"))
    .withColumn("dance_bucket_n", F.col("dance_bucket") + F.col("d_off"))
    .withColumn("valence_bucket_n", F.col("valence_bucket") + F.col("v_off"))
    .withColumn("tempo_bucket_n", F.col("tempo_bucket") + F.col("t_off"))
    .drop("e_off", "d_off", "v_off", "t_off")
)

# --- aliases mantidos ---
a_q42 = df_sim_q42.alias("a")
b_q42 = broadcast(df_expanded).alias("b")

# --- join corrigido (equi-join) ---
pairs_q42 = (
    a_q42.join(
        b_q42,
        (
            (F.col("a.energy_bucket") == F.col("b.energy_bucket_n"))
            & (F.col("a.dance_bucket") == F.col("b.dance_bucket_n"))
            & (F.col("a.valence_bucket") == F.col("b.valence_bucket_n"))
            & (F.col("a.tempo_bucket") == F.col("b.tempo_bucket_n"))
        ),
        "inner",
    )
    .filter(F.col("a.id") < F.col("b.id"))
    .filter(spark_abs(F.col("a.energy") - F.col("b.energy")) < 0.05)
    .filter(spark_abs(F.col("a.danceability") - F.col("b.danceability")) < 0.05)
    .filter(spark_abs(F.col("a.valence") - F.col("b.valence")) < 0.05)
    .filter(spark_abs(F.col("a.tempo") - F.col("b.tempo")) < 5)
    .select(
        F.col("a.id").alias("id_1"),
        F.col("a.name").alias("name_1"),
        F.col("a.artists").alias("artists_1"),
        F.col("a.popularity").alias("popularity_1"),
        F.col("b.id").alias("id_2"),
        F.col("b.name").alias("name_2"),
        F.col("b.artists").alias("artists_2"),
        F.col("b.popularity").alias("popularity_2"),
        spark_abs(F.col("a.popularity") - F.col("b.popularity")).alias(
            "popularity_diff"
        ),
    )
)

# --- resposta ---
print(f"Quantidade de pares similares: {pairs_q42.count()}")
pairs_q42.orderBy(F.desc("popularity_diff")).show(10, truncate=False)


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
# Interpretacao: enriquecer linha-a-linha com estatisticas agregadas melhora analise relativa de desempenho.
# AGENT: ajuste de debug - joins e metricas no nivel de artista individual; broadcast aplicado condicionalmente.
artists_clean_q43 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_base_q43 = (
    df.withColumn("artist_array", F.array_distinct(split(artists_clean_q43, r",\s*")))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

df_artist_stats_q43 = df_base_q43.groupBy("artist").agg(
    round(avg("popularity"), 6).alias("artist_avg_pop"),
    F.count_distinct("id").alias("artist_track_count"),
    round(stddev("popularity"), 6).alias("artist_std_pop"),
)

df_year_stats_q43 = df.groupBy("year").agg(
    round(avg("popularity"), 6).alias("year_avg_pop"),
    round(avg("energy"), 6).alias("year_avg_energy"),
    count("*").alias("year_total_tracks"),
)

df_key_mode_q43 = df.groupBy("key", "mode").agg(
    round(avg("popularity"), 6).alias("key_mode_avg_pop"),
    round(avg("danceability"), 6).alias("key_mode_avg_dance"),
)

artist_stats_small_q43 = df_artist_stats_q43.count() <= 10000
year_stats_small_q43 = df_year_stats_q43.count() <= 500
key_mode_small_q43 = df_key_mode_q43.count() <= 1000

artist_stats_join_q43 = (
    broadcast(df_artist_stats_q43) if artist_stats_small_q43 else df_artist_stats_q43
)
year_stats_join_q43 = (
    broadcast(df_year_stats_q43) if year_stats_small_q43 else df_year_stats_q43
)
key_mode_join_q43 = (
    broadcast(df_key_mode_q43) if key_mode_small_q43 else df_key_mode_q43
)

print(
    "Broadcast utilizado - artist_stats:",
    artist_stats_small_q43,
    "| year_stats:",
    year_stats_small_q43,
    "| key_mode:",
    key_mode_small_q43,
)

df_enriched_q43 = (
    df_base_q43.join(artist_stats_join_q43, "artist", "left")
    .join(year_stats_join_q43, "year", "left")
    .join(key_mode_join_q43, ["key", "mode"], "left")
    .withColumn("relative_pop", F.col("popularity") - F.col("artist_avg_pop"))
)

df_enriched_q43.select(
    "name",
    F.col("artist").alias("artists"),
    "year",
    "popularity",
    "artist_avg_pop",
    "relative_pop",
    "artist_track_count",
    "year_avg_pop",
    "key_mode_avg_pop",
).orderBy(F.desc("relative_pop")).show(10, truncate=False)


# %% [markdown]
# ## Q44 — Left Anti Join para Exclusões (2 pts)
# Crie um DataFrame `blacklist` com os 50 artistas mais populares.
# Use `left_anti` join para removê-los do dataset original.
# Compare as métricas médias (energy, danceability, valence, acousticness)
# do dataset completo vs. o "dataset sem mainstream".
# Qual a diferença? O underground soa diferente?

# %%
# Interpretacao: left_anti remove mainstream explicitamente e permite comparar assinatura media do restante.
# AGENT: ajuste de debug - blacklist robusta (minimo de faixas) e exclusao no nivel de track id.
artists_clean_q44 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_artists_q44 = (
    df.select("id", "artists", "popularity")
    .withColumn("artist_array", F.array_distinct(split(artists_clean_q44, r",\s*")))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

blacklist_q44 = (
    df_artists_q44.groupBy("artist")
    .agg(round(avg("popularity"), 6).alias("artist_avg_pop"))
    .join(
        df_artists_q44.groupBy("artist").agg(
            F.count_distinct("id").alias("artist_tracks")
        ),
        "artist",
        "inner",
    )
    .filter(F.col("artist_tracks") >= 5)
    .orderBy(F.desc("artist_avg_pop"))
    .limit(50)
    .select("artist")
)

blacklisted_track_ids_q44 = (
    df_artists_q44.join(blacklist_q44, "artist", "inner").select("id").distinct()
)
df_without_mainstream_q44 = df.join(blacklisted_track_ids_q44, "id", "left_anti")

metrics_q44 = ["energy", "danceability", "valence", "acousticness"]
full_metrics_q44 = df.agg(*[avg(c).alias(f"{c}_full") for c in metrics_q44])
underground_metrics_q44 = df_without_mainstream_q44.agg(
    *[avg(c).alias(f"{c}_underground") for c in metrics_q44]
)

comparison_q44 = full_metrics_q44.crossJoin(underground_metrics_q44)
for c in metrics_q44:
    comparison_q44 = comparison_q44.withColumn(
        f"{c}_diff_underground_minus_full",
        F.col(f"{c}_underground") - F.col(f"{c}_full"),
    )

comparison_q44.show(truncate=False)
print(
    "Leitura tecnica: diferencas positivas indicam que o dataset sem mainstream tem media maior naquele atributo."
)

# %% [markdown]
# ## Q45 — Otimização: Repartition e Coalesce (2 pts)
# Verifique o número atual de partições do DataFrame.
# Reparticione por `year` e meça (ou observe no plano) o efeito.
# Depois, use `coalesce` para reduzir a 2 partições.
# Salve o resultado em Parquet particionado por `decade` (floor(year/10)*10).
# Verifique a estrutura de pastas criada.
# (Use `df.rdd.getNumPartitions()` e `df.explain()`)

# %%
# Interpretacao: repartition por year melhora locality para operacoes por ano; coalesce reduz arquivos pequenos na escrita.
# AGENT: ajuste de debug - compatibilidade Databricks com fallback de path e inspecao via Spark/Hadoop FS.

# --- base ---
df_q45 = df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))


def get_num_partitions_q45(input_df):
    try:
        return input_df.rdd.getNumPartitions()
    except Exception:
        return input_df.select(F.spark_partition_id().alias("pid")).distinct().count()


print(f"Particoes iniciais: {get_num_partitions_q45(df_q45)}")

# --- repartition ---
df_repartitioned_q45 = df_q45.repartition("year")
print(
    f"Particoes apos repartition(year): {get_num_partitions_q45(df_repartitioned_q45)}"
)
print("Plano apos repartition(year):")
df_repartitioned_q45.explain()

# --- coalesce ---
df_coalesced_q45 = df_repartitioned_q45.coalesce(2)
print(f"Particoes apos coalesce(2): {get_num_partitions_q45(df_coalesced_q45)}")

# --- escrita como tabela (DELTA obrigatório) ---
table_name_q45 = "spotify_prova_50"

(
    df_coalesced_q45.write.mode("overwrite")
    .format("delta")  # 🔥 obrigatório em UC
    .partitionBy("decade")
    .saveAsTable(table_name_q45)
)

print(f"Tabela salva: {table_name_q45}")

# --- verificação ---
print("Particoes logicas gravadas (distinct decade):")
spark.read.table(table_name_q45).select("decade").distinct().orderBy("decade").show(
    200, truncate=False
)

print("Particoes fisicas registradas no metastore:")
spark.sql(f"SHOW PARTITIONS {table_name_q45}").show(truncate=False)


# %% [markdown]
# ## Q46 — Explain e Catalyst (2 pts)
# Compare os planos de execução (explain) de:
# 1. `df.filter(...).groupBy(...).agg(...)` (filter antes do group)
# 2. `df.groupBy(...).agg(...).filter(...)` (filter depois do group)
# O Catalyst otimiza? São iguais?
# Depois compare um join normal vs. broadcast join usando `explain(True)`.
# Documente as diferenças observadas nos planos.

# %%
# Interpretacao: comparar planos confirma pushdown/poda do Catalyst e efeito de hint de broadcast no join.
plan_filter_before_q46 = (
    df.filter(F.col("year") >= 2000)
    .groupBy("year")
    .agg(round(avg("popularity"), 6).alias("avg_popularity"))
)

plan_filter_after_q46 = (
    df.groupBy("year")
    .agg(round(avg("popularity"), 6).alias("avg_popularity"))
    .filter(F.col("year") >= 2000)
)

print("Plano 1 - filter antes do groupBy:")
plan_filter_before_q46.explain(True)

print("Plano 2 - filter depois do groupBy (mesma semantica no agrupador year):")
plan_filter_after_q46.explain(True)

small_df_q46 = df.groupBy("year").agg(round(avg("energy"), 6).alias("year_avg_energy"))
normal_join_q46 = df.join(small_df_q46, "year", "inner")
broadcast_join_q46 = df.join(broadcast(small_df_q46), "year", "inner")

print("Plano join normal:")
normal_join_q46.explain(True)

print("Plano broadcast join:")
broadcast_join_q46.explain(True)

print(
    "Observacao tecnica: Catalyst tende a empurrar filtro por chave de agrupamento para antes da agregacao; "
    "no join, broadcast substitui sort-merge/shuffle quando o lado pequeno cabe em memoria."
)


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
# Interpretacao: k-means simplificado manual sem MLlib, com 1 iteracao de reestimativa de centroides.
# AGENT: ajuste de debug - atribuicao via least+when com desempate deterministico (ordem C1..C4).
initial_centroids_q47 = {
    "C1": (0.2, 0.2, 0.2),
    "C2": (0.8, 0.8, 0.8),
    "C3": (0.8, 0.2, 0.5),
    "C4": (0.2, 0.8, 0.5),
}


def assign_cluster_q47(input_df, centroids):
    df_cluster = input_df
    distance_cols = []
    for cid, (c_energy, c_dance, c_valence) in centroids.items():
        dist_col = f"dist_{cid}"
        distance_cols.append(dist_col)
        df_cluster = df_cluster.withColumn(
            dist_col,
            sqrt(
                pow(F.col("energy") - F.lit(c_energy), 2)
                + pow(F.col("danceability") - F.lit(c_dance), 2)
                + pow(F.col("valence") - F.lit(c_valence), 2)
            ),
        )

    min_dist_expr = least(*[F.col(dc) for dc in distance_cols])
    df_cluster = df_cluster.withColumn("min_dist", min_dist_expr).withColumn(
        "cluster",
        when(F.col("dist_C1") == F.col("min_dist"), lit("C1"))
        .when(F.col("dist_C2") == F.col("min_dist"), lit("C2"))
        .when(F.col("dist_C3") == F.col("min_dist"), lit("C3"))
        .otherwise(lit("C4")),
    )
    return df_cluster


df_features_q47 = df.select(
    "id", "name", "artists", "popularity", "energy", "danceability", "valence"
)

assigned_iter1_q47 = assign_cluster_q47(df_features_q47, initial_centroids_q47)
centroids_iter1_rows_q47 = (
    assigned_iter1_q47.groupBy("cluster")
    .agg(
        avg("energy").alias("centroid_energy"),
        avg("danceability").alias("centroid_danceability"),
        avg("valence").alias("centroid_valence"),
    )
    .collect()
)

updated_centroids_q47 = dict(initial_centroids_q47)
for row in centroids_iter1_rows_q47:
    updated_centroids_q47[row["cluster"]] = (
        float(row["centroid_energy"]),
        float(row["centroid_danceability"]),
        float(row["centroid_valence"]),
    )

assigned_iter2_q47 = assign_cluster_q47(df_features_q47, updated_centroids_q47)
result_q47 = (
    assigned_iter2_q47.groupBy("cluster")
    .agg(
        count("*").alias("tracks"),
        round(avg("popularity"), 4).alias("avg_popularity"),
        round(avg("energy"), 4).alias("avg_energy"),
        round(avg("danceability"), 4).alias("avg_danceability"),
        round(avg("valence"), 4).alias("avg_valence"),
    )
    .orderBy("cluster")
)

result_q47.show(truncate=False)


# %% [markdown]
# ## Q48 — Análise de Séries Temporais Completa (2 pts)
# Para a métrica `energy` ao longo dos anos:
# - Calcule média anual, média móvel de 5 anos, tendência (regressão linear simples)
# - Tendência: `slope = corr(year, metric) * (std_metric / std_year)`
# - Calcule o resíduo: `residual = valor_real - tendência`
# - Identifique os anos "anômalos" (resíduo > 2 desvios padrão)
# Exiba: ano, energy_media, media_movel, tendencia, residuo.

# %%
# Interpretacao: tendencia linear via formula de slope com corr/std e residuos para anomalias de energia.
annual_energy_q48 = (
    df.groupBy("year")
    .agg(round(avg("energy"), 6).alias("energy_media"))
    .orderBy("year")
)

window_q48 = Window.orderBy("year").rowsBetween(-2, 2)
annual_energy_q48 = annual_energy_q48.withColumn(
    "media_movel", avg("energy_media").over(window_q48)
)

trend_stats_q48 = annual_energy_q48.agg(
    corr("year", "energy_media").alias("corr_year_energy"),
    stddev("energy_media").alias("std_energy_media"),
    stddev("year").alias("std_year"),
    avg("year").alias("avg_year"),
    avg("energy_media").alias("avg_energy_media"),
).first()
trend_stats_q48 = cast(
    Any,
    trend_stats_q48
    or {
        "corr_year_energy": 0.0,
        "std_energy_media": 0.0,
        "std_year": 0.0,
        "avg_year": 0.0,
        "avg_energy_media": 0.0,
    },
)

corr_q48 = trend_stats_q48["corr_year_energy"] or 0.0
std_energy_q48 = trend_stats_q48["std_energy_media"] or 0.0
std_year_q48 = trend_stats_q48["std_year"] or 0.0
avg_year_q48 = trend_stats_q48["avg_year"] or 0.0
avg_energy_q48 = trend_stats_q48["avg_energy_media"] or 0.0

slope_q48 = (
    float(corr_q48 * (std_energy_q48 / std_year_q48)) if std_year_q48 != 0 else 0.0
)
intercept_q48 = float(avg_energy_q48 - (slope_q48 * avg_year_q48))

result_q48 = (
    annual_energy_q48.withColumn(
        "tendencia", (F.col("year") * F.lit(slope_q48)) + F.lit(intercept_q48)
    )
    .withColumn("residuo", F.col("energy_media") - F.col("tendencia"))
    .select(
        "year",
        "energy_media",
        round("media_movel", 6).alias("media_movel"),
        round("tendencia", 6).alias("tendencia"),
        round("residuo", 6).alias("residuo"),
    )
    .orderBy("year")
)

residual_row_q48 = result_q48.agg(stddev("residuo").alias("residual_std")).first()
residual_std_q48 = (
    residual_row_q48.residual_std
    if residual_row_q48 and residual_row_q48.residual_std is not None
    else 0.0
)

anomalias_q48 = result_q48.filter(
    spark_abs(F.col("residuo")) > (F.lit(2.0) * F.lit(residual_std_q48))
)

print(f"Slope anual de energy_media: {slope_q48:.8f}")
result_q48.show(200, truncate=False)
print("Anos anomalos (|residuo| > 2*std):")
anomalias_q48.show(200, truncate=False)


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
# Interpretacao: pares de colaboracao sao arestas nao direcionadas entre artistas da mesma faixa.
artists_clean_q49 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
tracks_q49 = (
    df.select("id", "artists")
    .withColumn("artist_array_raw", split(artists_clean_q49, r",\s*"))
    .withColumn(
        "artist_array",
        F.expr("filter(transform(artist_array_raw, x -> trim(x)), x -> x <> '')"),
    )
    .withColumn("artist_array", F.array_distinct(F.col("artist_array")))
    .filter(F.size("artist_array") >= 2)
)

left_q49 = tracks_q49.select(
    "id", F.posexplode("artist_array").alias("idx1", "artist1")
)
right_q49 = tracks_q49.select(
    "id", F.posexplode("artist_array").alias("idx2", "artist2")
)

pair_counts_q49 = (
    left_q49.join(right_q49, "id", "inner")
    .filter(F.col("idx1") < F.col("idx2"))
    .select("artist1", "artist2")
    .groupBy("artist1", "artist2")
    .agg(count("*").alias("collaborations"))
    .orderBy(F.desc("collaborations"), F.asc("artist1"), F.asc("artist2"))
)

top10_pairs_q49 = pair_counts_q49.limit(10)
print("Top 10 pares de colaboracao:")
top10_pairs_q49.show(truncate=False)

top_artists_q49 = (
    top10_pairs_q49.select(F.col("artist1").alias("artist"))
    .union(top10_pairs_q49.select(F.col("artist2").alias("artist")))
    .distinct()
)

edges_q49 = pair_counts_q49.select(
    F.col("artist1").alias("artist"), F.col("artist2").alias("collaborator")
).union(
    pair_counts_q49.select(
        F.col("artist2").alias("artist"), F.col("artist1").alias("collaborator")
    )
)

degree_q49 = edges_q49.groupBy("artist").agg(
    F.count_distinct("collaborator").alias("degree")
)

result_degree_q49 = top_artists_q49.join(degree_q49, "artist", "left").orderBy(
    F.desc("degree"), F.asc("artist")
)

print("Grau dos artistas presentes no Top 10 de pares:")
result_degree_q49.show(truncate=False)
print("Artista mais conectado:")
result_degree_q49.show(1, truncate=False)


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
# Interpretacao: consolidar KPIs finais em DataFrames para leitura executiva e validacao analitica.
# AGENT: ajuste de debug - ranking estavel e serializacao ordenada para dashboard deterministico.
artists_clean_q50 = regexp_replace(
    regexp_replace(F.col("artists"), r"^\[|\]$", ""), r"'", ""
)
df_artists_q50 = (
    df.withColumn("artist_array", split(artists_clean_q50, r",\s*"))
    .withColumn("artist", explode("artist_array"))
    .withColumn("artist", trim(F.col("artist")))
    .filter(F.col("artist") != "")
)

# 1) Resumo geral
summary_q50 = df.agg(
    count("*").alias("total_musicas"),
    min("year").alias("ano_min"),
    max("year").alias("ano_max"),
).crossJoin(df_artists_q50.agg(F.count_distinct("artist").alias("total_artistas")))

print("1) Resumo geral:")
summary_q50.show(truncate=False)

# 2) Top 5 artistas por popularidade media (min 10 musicas)
top5_artists_q50 = (
    df_artists_q50.groupBy("artist")
    .agg(
        F.count_distinct("id").alias("tracks"),
        round(avg("popularity"), 6).alias("avg_popularity"),
    )
    .filter(F.col("tracks") >= 10)
    .orderBy(F.desc("avg_popularity"), F.desc("tracks"), F.asc("artist"))
    .limit(5)
)

print("2) Top 5 artistas por popularidade media (min 10 musicas):")
top5_artists_q50.show(truncate=False)

top5_ranked_q50 = top5_artists_q50.withColumn(
    "rank",
    row_number().over(
        Window.orderBy(F.desc("avg_popularity"), F.desc("tracks"), F.asc("artist"))
    ),
)

# 3) Decada de ouro: maior diversidade (artistas distintos / musica)
diversity_q50 = (
    df.withColumn("decade", (floor(F.col("year") / 10) * 10).cast("int"))
    .groupBy("decade")
    .agg(count("*").alias("total_musicas"))
    .join(
        df_artists_q50.withColumn(
            "decade", (floor(F.col("year") / 10) * 10).cast("int")
        )
        .groupBy("decade")
        .agg(F.count_distinct("artist").alias("artistas_distintos")),
        "decade",
        "inner",
    )
    .withColumn(
        "diversidade_ratio", F.col("artistas_distintos") / F.col("total_musicas")
    )
    .orderBy(F.desc("diversidade_ratio"))
)

golden_decade_q50 = diversity_q50.limit(1)
print("3) Decada de ouro (maior diversidade relativa):")
golden_decade_q50.show(truncate=False)

# 4) Evolucao de energia ao longo do tempo (slope)
annual_energy_q50 = df.groupBy("year").agg(avg("energy").alias("avg_energy"))
energy_stats_q50 = annual_energy_q50.agg(
    corr("year", "avg_energy").alias("corr_year_energy"),
    stddev("avg_energy").alias("std_energy"),
    stddev("year").alias("std_year"),
).first()
energy_stats_q50 = cast(
    Any,
    energy_stats_q50 or {"corr_year_energy": 0.0, "std_energy": 0.0, "std_year": 1.0},
)

energy_slope_q50 = float(energy_stats_q50["corr_year_energy"] or 0.0) * (
    float(energy_stats_q50["std_energy"] or 0.0)
    / float(energy_stats_q50["std_year"] or 1.0)
)
energy_trend_label_q50 = (
    "mais energetica" if energy_slope_q50 > 0 else "menos energetica"
)

energy_trend_q50 = spark.createDataFrame(
    [(energy_slope_q50, energy_trend_label_q50)],
    ["energy_slope", "interpretacao"],
)
print("4) Evolucao de energia:")
energy_trend_q50.show(truncate=False)

# 5) Predicao simples de popularidade para proximo ano
annual_pop_q50 = df.groupBy("year").agg(avg("popularity").alias("avg_popularity"))
pop_stats_q50 = annual_pop_q50.agg(
    corr("year", "avg_popularity").alias("corr_year_pop"),
    stddev("avg_popularity").alias("std_popularity"),
    stddev("year").alias("std_year"),
    avg("year").alias("avg_year"),
    avg("avg_popularity").alias("avg_pop"),
    max("year").alias("max_year"),
).first()
pop_stats_q50 = cast(
    Any,
    pop_stats_q50
    or {
        "corr_year_pop": 0.0,
        "std_popularity": 0.0,
        "std_year": 1.0,
        "avg_year": 0.0,
        "avg_pop": 0.0,
        "max_year": 0,
    },
)

pop_slope_q50 = float(pop_stats_q50["corr_year_pop"] or 0.0) * (
    float(pop_stats_q50["std_popularity"] or 0.0)
    / float(pop_stats_q50["std_year"] or 1.0)
)
pop_intercept_q50 = float(pop_stats_q50["avg_pop"] or 0.0) - (
    pop_slope_q50 * float(pop_stats_q50["avg_year"] or 0.0)
)
next_year_q50 = int(pop_stats_q50["max_year"] or 0) + 1
predicted_pop_next_year_q50 = (pop_slope_q50 * next_year_q50) + pop_intercept_q50

prediction_q50 = spark.createDataFrame(
    [(next_year_q50, pop_slope_q50, pop_intercept_q50, predicted_pop_next_year_q50)],
    ["next_year", "popularity_slope", "popularity_intercept", "predicted_popularity"],
)
print("5) Predicao simples de popularidade para proximo ano:")
prediction_q50.show(truncate=False)

# Dashboard consolidado (1 linha por insight)
dashboard_q50 = (
    summary_q50.select(
        lit("Resumo geral").alias("insight"),
        F.to_json(
            F.struct(
                "total_musicas",
                "total_artistas",
                "ano_min",
                "ano_max",
            )
        ).alias("valor"),
    )
    .unionByName(
        top5_ranked_q50.agg(
            F.to_json(
                F.sort_array(
                    collect_list(
                        F.struct(
                            F.col("rank"),
                            F.col("artist"),
                            F.col("tracks"),
                            F.round(F.col("avg_popularity"), 4).alias("avg_popularity"),
                        )
                    )
                )
            ).alias("valor")
        ).select(lit("Top 5 artistas").alias("insight"), "valor")
    )
    .unionByName(
        golden_decade_q50.select(
            lit("Decada de ouro").alias("insight"),
            F.to_json(
                F.struct(
                    F.col("decade"),
                    F.col("artistas_distintos"),
                    F.col("total_musicas"),
                    F.round(F.col("diversidade_ratio"), 6).alias("diversidade_ratio"),
                )
            ).alias("valor"),
        )
    )
    .unionByName(
        energy_trend_q50.select(
            lit("Evolucao de energia").alias("insight"),
            F.to_json(
                F.struct(
                    F.round(F.col("energy_slope"), 8).alias("energy_slope"),
                    F.col("interpretacao"),
                )
            ).alias("valor"),
        )
    )
    .unionByName(
        prediction_q50.select(
            lit("Predicao de popularidade").alias("insight"),
            F.to_json(
                F.struct(
                    F.col("next_year"),
                    F.round(F.col("predicted_popularity"), 6).alias(
                        "predicted_popularity"
                    ),
                    F.round(F.col("popularity_slope"), 8).alias("popularity_slope"),
                )
            ).alias("valor"),
        )
    )
)

print("Dashboard consolidado:")
dashboard_q50.show(truncate=False)


# %% [markdown]
# ---
# ## 🧹 Cleanup
# %%
spark.stop()
print("✅ Prova finalizada! Boa sorte, André!")
