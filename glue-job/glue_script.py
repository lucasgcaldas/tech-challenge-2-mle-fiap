import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *

from pyspark.sql.functions import col, lit, datediff

# Inicialização do Glue Context
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init("Job-Glue-Bovespa", sys.argv)

# Caminhos S3
input_path = "s3://bucketfiapgrupo129-tech2/data/raw/"  # Dados brutos
output_path = "s3://bucketfiapgrupo129-tech2/data/refined/"  # Dados refinados

# Carregar os dados do S3
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="parquet"
)

# Conversão para DataFrame do Spark
df = datasource0.toDF()

# Transformação A: Agrupamento e sumarização
# Exemplo: Soma e contagem agrupados pelo nome da ação (coluna "Código")
df_grouped = df.groupBy("Código").agg(
    {"Qtde. Teórica": "sum", "Part. (%)": "avg"}
).withColumnRenamed("sum(Qtde. Teórica)", "total_qtde_teorica") \
 .withColumnRenamed("avg(Part. (%))", "media_participacao")

# Transformação B: Renomear duas colunas
df_grouped = df_grouped.withColumnRenamed("Código", "codigo_acao") \
                       .withColumnRenamed("total_qtde_teorica", "quantidade_teorica_total")

# Transformação C: Cálculo com campos de data
# Exemplo: Criar uma coluna fictícia "dias_ativos" para demonstração
df_grouped = df_grouped.withColumn("dias_ativos", lit(30))  # Coloque um valor fixo ou ajuste conforme necessário

# Adicionar partições por data e nome/abreviação da ação
# Assumimos que existe uma coluna de data "data_particao" ou usamos a data atual
from pyspark.sql.functions import current_date
df_grouped = df_grouped.withColumn("data_particao", current_date())  # Data atual como partição

# Converter de volta para DynamicFrame
dynamic_frame_transformed = DynamicFrame.fromDF(df_grouped, glueContext, "transformed")

# Salvar os dados refinados no S3 particionados
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["data_particao", "codigo_acao"]  # Partições: data e nome/abreviação da ação
    },
    format="parquet"
)

# Catalogar os dados no Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame_transformed,
    database="default",  # Substitua pelo nome do seu database no Glue Catalog
    table_name="refined_bovespa",  # Substitua pelo nome da tabela no Glue Catalog
    transformation_ctx="datasink"
)

job.commit()