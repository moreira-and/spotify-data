# %% [markdown]
# # 🎧 Exercícios Práticos de PySpark com Dataset do Spotify
# 
# Este notebook contém exercícios práticos para aplicar os conceitos de PySpark
# utilizando o dataset de músicas do Spotify.
# 
# **Dataset:** [Spotify Data from PySpark Course](https://www.kaggle.com/datasets/kapturovalexander/spotify-data-from-pyspark-course)
# 
# **Instruções:**
# 1. Baixe o dataset do Kaggle (spotify-data.csv)
# 2. Coloque o arquivo na pasta `data/` ou ajuste o caminho
# 3. Execute as células em ordem

# %% [markdown]
# ## 📦 Setup Inicial

# %%
# Instalação do PySpark (se necessário)
# !pip install pyspark

# %%
# Imports necessários
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)

from pyspark.sql.functions import (
    col, lit, when, coalesce,
    count, countDistinct, sum, avg, min, max,
    year, month, dayofweek,
    upper, lower, length,
    row_number, rank, dense_rank, lag, lead,
    floor, round, desc, asc, try_to_date,)

from pyspark.sql.window import Window
from pyspark.sql.types import StringType, IntegerType, FloatType

# %%
# Criar SparkSession
spark = SparkSession.builder \
    .appName("SpotifyAnalysis") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print(f"✅ Spark versão: {spark.version}")
# print(f"📱 App: {spark.sparkContext.appName}")

# %% [markdown]
# ## 📂 Carregamento dos Dados
# 
# Ajuste o caminho do arquivo conforme necessário.

# %%
# Caminho do arquivo CSV - AJUSTE AQUI
CAMINHO_ARQUIVO = "/Workspace/Repos/moreira-and@outlook.com/spotify-data/data/spotify-data.csv"

# Schema do arquivo
schema = StructType([
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
    StructField("explicit", IntegerType(), True)
])

# Carregar dados
df = spark.read \
    .schema(schema) \
    .option("header", "true") \
    .option("quote", '"') \
    .option("escape", '"') \
    .option("multiLine", "true") \
    .csv(CAMINHO_ARQUIVO)

print(f"✅ Dataset carregado com {df.count():,} músicas")


# %%
# Corrigir release_date


df_formats = df.withColumn(
    "date_format",
    when(col("release_date").rlike(r"^\d{4}-\d{2}-\d{2}$"), "yyyy-MM-dd")
    .when(col("release_date").rlike(r"^\d{4}-\d{2}$"), "yyyy-MM")
    .when(col("release_date").rlike(r"^\d{4}$"), "yyyy")
    .when(col("release_date").rlike(r"^\d{1,2}/\d{1,2}/\d{2}$"), "M/d/yy")
    .otherwise("unknown")
)

df_formats.groupBy("date_format").count().show()


df = df.withColumn(
    "release_date",
    coalesce(
        try_to_date("release_date","yyyy-MM-dd"),
        try_to_date("release_date","yyyy-MM"),
        try_to_date("release_date","yyyy"),
        try_to_date("release_date","M/d/yy"),
        try_to_date("release_date","MM/dd/yy")
    )
)

null_dates = df.filter(col("release_date").isNull()).count()

print(f"📅 Datas inválidas após parsing: {null_dates}")


# %%
# Visualizar schema
df.printSchema()


# %%
# Primeiros registros
df.show(10, truncate=False)

# %% [markdown]
# ---
# # 🔰 Nível Básico

# %% [markdown]
# ## Exercício 1: Exploração Inicial
# 
# **Objetivos:**
# - Contar registros e colunas
# - Identificar valores únicos
# - Verificar período dos dados

# %%
# 1.1 Dimensões do DataFrame
print(f"📊 Número de linhas: {df.count():,}")
print(f"📊 Número de colunas: {len(df.columns)}")

# %%
# 1.2 Colunas disponíveis
print("📋 Colunas:")
for i, col_name in enumerate(df.columns, 1):
    print(f"   {i:2}. {col_name}")

# %%
# 1.3 Músicas e artistas únicos
print(f"🎵 Músicas únicas: {df.select('id').distinct().count():,}")
print(f"🎤 Artistas únicos: {df.select('artists').distinct().count():,}")

# %%
# 1.4 Período coberto pelo dataset
df.select(
    min("year").alias("Ano_Inicial"),
    max("year").alias("Ano_Final")
).show()

# %%
# 1.5 Estatísticas descritivas
df.describe().show()

# %% [markdown]
# ## Exercício 2: Seleção e Filtragem
# 
# **Objetivos:**
# - Praticar select e filter
# - Aplicar diferentes tipos de condições

# %%
# 2.1 Selecionar colunas específicas
df.select("name", "artists", "year", "popularity").show(10)

# %%
# 2.2 Músicas muito populares (popularity > 80)
df_populares = df.filter(col("popularity") > 80)
print(f"🌟 Músicas com popularity > 80: {df_populares.count():,}")
df_populares.select("name", "artists", "popularity").orderBy(desc("popularity")).show(10)

# %%
# 2.3 Músicas recentes (após 2020)
df_recentes = df.filter(col("year") >= 2020)
print(f"📅 Músicas de 2020+: {df_recentes.count():,}")

# %%
# 2.4 Músicas com conteúdo explícito
df_explicit = df.filter(col("explicit") == 1)
print(f"🔞 Músicas explícitas: {df_explicit.count():,}")
print(f"📊 Porcentagem: {df_explicit.count() / df.count() * 100:.1f}%")

# %%
# 2.5 Músicas com "love" no nome (case insensitive)
df_love = df.filter(lower(col("name")).contains("love"))
print(f"❤️ Músicas com 'love' no nome: {df_love.count():,}")
df_love.select("name", "artists", "year").show(10)

# %% [markdown]
# ---
# # 🔵 Nível Intermediário

# %% [markdown]
# ## Exercício 3: Transformações de Colunas
# 
# **Objetivos:**
# - Criar novas colunas calculadas
# - Usar funções condicionais
# - Categorizar dados

# %%
# 3.1 Duração em minutos
df = df.withColumn("duration_min", round(col("duration_ms") / 60000, 2))

df.select("name", "duration_ms", "duration_min").show(5)

# %%
# 3.2 Década de lançamento
df = df.withColumn("decade", (floor(col("year") / 10) * 10).cast("int"))

df.select("name", "year", "decade").show(10)

# %%
# 3.3 Classificar nível de energia
df = df.withColumn(
    "energy_level",
    when(col("energy") < 0.4, "Low")
    .when(col("energy") < 0.7, "Medium")
    .otherwise("High")
)

df.groupBy("energy_level").count().orderBy("count").show()

# %%
# 3.4 Classificar humor/mood pela valence
df = df.withColumn(
    "mood",
    when(col("valence") < 0.3, "Sad")
    .when(col("valence") < 0.6, "Neutral")
    .otherwise("Happy")
)

df.groupBy("mood").count().orderBy(desc("count")).show()

# %%
# 3.5 Verificar transformações
df.select("name", "energy", "energy_level", "valence", "mood").show(10)

# %% [markdown]
# ## Exercício 4: Agregações e Agrupamentos
# 
# **Objetivos:**
# - Calcular métricas por grupo
# - Usar múltiplas funções de agregação
# - Identificar padrões nos dados

# %%
# 4.1 Popularidade média por década
df.groupBy("decade") \
    .agg(
        avg("popularity").alias("avg_popularity"),
        count("*").alias("num_musicas")
    ) \
    .orderBy("decade") \
    .show()

# %%
# 4.2 Top 10 artistas com mais músicas
df.groupBy("artists") \
    .count() \
    .orderBy(desc("count")) \
    .show(10)

# %%
# 4.3 Características médias por década
df.groupBy("decade") \
    .agg(
        round(avg("danceability"), 3).alias("avg_danceability"),
        round(avg("energy"), 3).alias("avg_energy"),
        round(avg("acousticness"), 3).alias("avg_acousticness"),
        round(avg("valence"), 3).alias("avg_valence"),
        round(avg("tempo"), 1).alias("avg_tempo")
    ) \
    .orderBy("decade") \
    .show()

# %%
# 4.4 Década com maior BPM médio
df.groupBy("decade") \
    .agg(round(avg("tempo"), 2).alias("avg_tempo")) \
    .orderBy(desc("avg_tempo")) \
    .show(1)

# %%
# 4.5 Estatísticas de valence por mode (maior/menor)
df.groupBy("mode") \
    .agg(
        count("*").alias("count"),
        round(avg("valence"), 3).alias("avg_valence"),
        round(min("valence"), 3).alias("min_valence"),
        round(max("valence"), 3).alias("max_valence")
    ) \
    .withColumn("mode_name", when(col("mode") == 1, "Major").otherwise("Minor")) \
    .show()

# %% [markdown]
# ## Exercício 5: SQL no Spark
# 
# **Objetivos:**
# - Registrar views temporárias
# - Executar queries SQL
# - Comparar com API DataFrame

# %%
# 5.1 Registrar como view temporária
df.createOrReplaceTempView("spotify")

# %%
# 5.2 Top 20 músicas mais populares
spark.sql("""
    SELECT name, artists, year, popularity
    FROM spotify
    ORDER BY popularity DESC
    LIMIT 20
""").show(truncate=False)

# %%
# 5.3 Músicas acima da média de popularidade
spark.sql("""
    WITH avg_pop AS (
        SELECT AVG(popularity) as media
        FROM spotify
    )
    SELECT name, artists, popularity
    FROM spotify, avg_pop
    WHERE popularity > media
    ORDER BY popularity DESC
    LIMIT 20
""").show()

# %%
# 5.4 Classificação por tempo (BPM)
spark.sql("""
    SELECT 
        name,
        artists,
        tempo,
        CASE 
            WHEN tempo < 90 THEN 'Lento'
            WHEN tempo < 120 THEN 'Moderado'
            WHEN tempo < 150 THEN 'Rápido'
            ELSE 'Muito Rápido'
        END as tempo_categoria
    FROM spotify
    WHERE popularity > 70
    ORDER BY tempo DESC
    LIMIT 20
""").show()

# %%
# 5.5 Análise por década usando SQL
spark.sql("""
    SELECT 
        FLOOR(year / 10) * 10 as decade,
        COUNT(*) as total_musicas,
        ROUND(AVG(popularity), 2) as avg_popularity,
        ROUND(AVG(danceability), 3) as avg_danceability,
        ROUND(AVG(energy), 3) as avg_energy
    FROM spotify
    GROUP BY FLOOR(year / 10) * 10
    ORDER BY decade
""").show()

# %% [markdown]
# ---
# # 🔴 Nível Avançado

# %% [markdown]
# ## Exercício 6: Window Functions
# 
# **Objetivos:**
# - Ranking por grupos
# - Cálculos com linhas vizinhas
# - Agregações em janela

# %%
# 6.1 Ranking de músicas por popularidade dentro de cada artista
window_artist = Window.partitionBy("artists").orderBy(desc("popularity"))

df_ranked = df.withColumn(
    "rank_by_artist",
    row_number().over(window_artist)
)

# Top 3 músicas de cada artista (artistas com mais músicas)
top_artists = df.groupBy("artists").count().orderBy(desc("count")).limit(10).collect()
top_artist_names = [row.artists for row in top_artists]

df_ranked.filter(
    (col("artists").isin(top_artist_names)) &
    (col("rank_by_artist") <= 3)
).select("artists", "name", "popularity", "rank_by_artist") \
.orderBy("artists", "rank_by_artist") \
.show(30, truncate=False)

# %%
# 6.2 Música mais popular de cada ano
window_year = Window.partitionBy("year").orderBy(desc("popularity"))

df_top_year = df.withColumn(
    "rank_year",
    row_number().over(window_year)
).filter(col("rank_year") == 1)

df_top_year.select("year", "name", "artists", "popularity") \
    .orderBy("year") \
    .show(50)

# %%
# 6.3 Diferença de cada música para a média do artista
window_avg = Window.partitionBy("artists")

df_diff = df.withColumn(
    "artist_avg_pop",
    round(avg("popularity").over(window_avg), 2)
).withColumn(
    "diff_from_avg",
    round(col("popularity") - col("artist_avg_pop"), 2)
)

# Músicas muito acima da média do artista
df_diff.filter(col("diff_from_avg") > 30) \
    .select("name", "artists", "popularity", "artist_avg_pop", "diff_from_avg") \
    .orderBy(desc("diff_from_avg")) \
    .show(20)

# %%
# 6.4 Média móvel de popularidade por ano
window_rolling = Window.orderBy("year").rowsBetween(-2, 0)

df_year_stats = df.groupBy("year") \
    .agg(round(avg("popularity"), 2).alias("avg_popularity"))

df_year_stats = df_year_stats.withColumn(
    "rolling_avg_3y",
    round(avg("avg_popularity").over(window_rolling), 2)
)

df_year_stats.orderBy("year").show(50)

# %% [markdown]
# ## Exercício 7: UDFs (User Defined Functions)
# 
# **Objetivos:**
# - Criar funções customizadas
# - Classificar músicas por características
# - Aplicar lógica complexa

# %%
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 7.1 UDF para classificar gênero estimado
@udf(returnType=StringType())
def classify_genre(danceability, energy, acousticness, instrumentalness, speechiness, loudness):
    """
    Classifica o gênero estimado baseado nas características da música.
    """
    if any(x is None for x in [danceability, energy, acousticness, instrumentalness, speechiness, loudness]):
        return "Unknown"
    
    if danceability > 0.7 and energy > 0.7:
        return "Dance/Electronic"
    elif acousticness > 0.7 and instrumentalness > 0.5:
        return "Acoustic/Folk"
    elif speechiness > 0.3:
        return "Hip-Hop/Rap"
    elif energy > 0.6 and loudness > -8:
        return "Rock/Pop"
    elif acousticness > 0.5 and energy < 0.4:
        return "Ballad/Soft"
    else:
        return "Other"

# %%
# 7.2 Aplicar UDF
df_genre = df.withColumn(
    "genre_estimate",
    classify_genre(
        col("danceability"),
        col("energy"),
        col("acousticness"),
        col("instrumentalness"),
        col("speechiness"),
        col("loudness")
    )
)

# %%
# 7.3 Distribuição de gêneros estimados
df_genre.groupBy("genre_estimate") \
    .agg(
        count("*").alias("count"),
        round(avg("popularity"), 2).alias("avg_popularity")
    ) \
    .orderBy(desc("count")) \
    .show()

# %%
# 7.4 Distribuição de gêneros por década
df_genre.groupBy("decade", "genre_estimate") \
    .count() \
    .orderBy("decade", desc("count")) \
    .show(50)

# %% [markdown]
# ## Exercício 8: Análise Final - Insights
# 
# **Objetivo:** Extrair insights acionáveis dos dados

# %%
# 8.1 DNA de uma música popular (top 10% de popularidade)
percentile_90 = df.approxQuantile("popularity", [0.9], 0.01)[0]
print(f"📊 Percentil 90 de popularidade: {percentile_90}")

df_top_music = df.filter(col("popularity") >= percentile_90)

print("\n🧬 DNA da Música Popular (Top 10%):")
df_top_music.select(
    round(avg("danceability"), 3).alias("danceability"),
    round(avg("energy"), 3).alias("energy"),
    round(avg("acousticness"), 3).alias("acousticness"),
    round(avg("valence"), 3).alias("valence"),
    round(avg("tempo"), 1).alias("tempo"),
    round(avg("loudness"), 2).alias("loudness"),
    round(avg("speechiness"), 3).alias("speechiness")
).show()

# %%
# 8.2 Evolução das características ao longo das décadas
print("\n📈 Evolução Musical por Década:")

evolucao = df.groupBy("decade") \
    .agg(
        round(avg("energy"), 3).alias("energy"),
        round(avg("danceability"), 3).alias("danceability"),
        round(avg("acousticness"), 3).alias("acousticness"),
        round(avg("valence"), 3).alias("valence"),
        round(avg("tempo"), 1).alias("tempo")
    ) \
    .orderBy("decade")

evolucao.show()

# %%
# 8.3 Artistas mais versáteis (maior variação nas características)
from pyspark.sql.functions import stddev

# Artistas com pelo menos 5 músicas
artistas_frequentes = df.groupBy("artists") \
    .count() \
    .filter(col("count") >= 5) \
    .select("artists")

df_versatil = df.join(artistas_frequentes, "artists") \
    .groupBy("artists") \
    .agg(
        count("*").alias("num_musicas"),
        round(stddev("energy"), 3).alias("std_energy"),
        round(stddev("danceability"), 3).alias("std_danceability"),
        round(stddev("valence"), 3).alias("std_valence"),
        round(stddev("tempo"), 2).alias("std_tempo")
    )

# Score de versatilidade (média dos desvios padrão normalizados)
df_versatil = df_versatil.withColumn(
    "versatility_score",
    round((col("std_energy") + col("std_danceability") + col("std_valence")) / 3, 3)
)

print("\n🎭 Top 10 Artistas Mais Versáteis:")
df_versatil.orderBy(desc("versatility_score")).show(10)

# %%
# 8.4 Correlação entre características e popularidade
print("\n📊 Correlação com Popularidade:")

caracteristicas = ["danceability", "energy", "acousticness", "valence", 
                   "tempo", "loudness", "speechiness", "instrumentalness"]

for caract in caracteristicas:
    corr = df.stat.corr("popularity", caract)
    sinal = "📈" if corr > 0 else "📉"
    print(f"   {sinal} {caract}: {corr:.4f}")

# %%
# 8.5 Melhor mês para lançar música
df_dates = df.withColumn("month", month(col("release_date")))

print("\n📅 Popularidade Média por Mês de Lançamento:")
df_dates.groupBy("month") \
    .agg(
        count("*").alias("lancamentos"),
        round(avg("popularity"), 2).alias("avg_popularity")
    ) \
    .orderBy("month") \
    .show(12)

# %% [markdown]
# ## Exercício 9: Salvar Resultados

# %%
# 9.1 Salvar dados processados em Parquet (particionado por década)
delta_name = "spotify_processed"

df_genre.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("decade") \
    .saveAsTable(delta_name)

print(f"✅ Dados salvos em: {delta_name}")

# %%
# 9.2 Verificar estrutura salva
import os
print("📁 Estrutura de partições:")
spark.table(delta_name).show(5)

spark.sql(f"DESCRIBE DETAIL {delta_name}").show()

# %% [markdown]
# ---
# ## 🧹 Cleanup

# %%
# Encerrar SparkSession
spark.stop()
print("✅ SparkSession encerrada")

# %% [markdown]
# ---
# # 📝 Resumo do Aprendizado
# 
# Neste notebook você praticou:
# 
# ✅ **Carregamento de dados** CSV no PySpark  
# ✅ **Exploração** com printSchema, describe, count  
# ✅ **Seleção e filtragem** com select, filter, where  
# ✅ **Transformações** com withColumn, when, otherwise  
# ✅ **Agregações** com groupBy, agg  
# ✅ **SQL** no Spark com createOrReplaceTempView e spark.sql  
# ✅ **Window Functions** para rankings e cálculos avançados  
# ✅ **UDFs** para lógica customizada  
# ✅ **Salvamento** em formato Parquet particionado  
# 
# ---
# 
# **Próximos passos sugeridos:**
# 1. Experimente com diferentes filtros e agregações
# 2. Crie suas próprias UDFs
# 3. Explore o Spark MLlib para Machine Learning
# 4. Pratique com datasets maiores