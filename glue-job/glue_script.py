import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

# Inicialização do Glue Context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(sys.argv[1], sys.argv)

# Caminhos S3
input_path = "s3://bucketfiapgrupo129/data/raw/"  # Dados brutos
output_path = "s3://bucketfiapgrupo129/data/refined/"  # Dados refinados

# Carregar os dados do S3
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="parquet"
)

# Conversão para DataFrame do Spark
df = datasource0.toDF()

# Transformação A: Agrupamento e sumarização
# Exemplo: Soma e contagem agrupados pelo nome da ação (coluna "acao")
df_grouped = df.groupBy("acao").agg({"volume": "sum", "preco": "count"}) \
    .withColumnRenamed("sum(volume)", "total_volume") \
    .withColumnRenamed("count(preco)", "transacoes")

# Transformação B: Renomear duas colunas
df_grouped = df_grouped.withColumnRenamed("acao", "nome_acao") \
                       .withColumnRenamed("transacoes", "total_transacoes")

# Transformação C: Cálculo com campos de data
# Exemplo: Criar uma nova coluna com a duração em dias (entre dois campos de data)
from pyspark.sql.functions import col, datediff
df_grouped = df_grouped.withColumn("dias_ativos", datediff(col("data_fim"), col("data_inicio")))

# Adicionar partições por data e nome/abreviação da ação
df_grouped = df_grouped.withColumn("data_particao", col("data_inicio"))

# Converter de volta para DynamicFrame
dynamic_frame_transformed = DynamicFrame.fromDF(df_grouped, glueContext, "transformed")

# Salvar os dados refinados no S3 particionados
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["data_particao", "nome_acao"]  # Partições: data e nome/abreviação da ação
    },
    format="parquet"
)

# Catalogar os dados no Glue Data Catalog
glueContext.create_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet",
    transformation_ctx="datasink"
)

job.commit()
