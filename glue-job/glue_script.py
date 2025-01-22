import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from pyspark.sql.functions import col, lit, date_format, current_date

# Parse job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue Context and Job
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Caminhos S3
input_path = "s3://bucketfiapgrupo129-tech2/raw/data/"  # Dados brutos
output_path = "s3://bucketfiapgrupo129-tech2/refined/data/"  # Dados refinados

# Nome do database e tabela no Glue Catalog
database_name = "fiapgrupo129"  # Substitua pelo nome do seu banco de dados no Glue Catalog
table_name = "fiapgrupo129_table_ibov"  # Nome da tabela

# Carregar os dados do S3
datasource0 = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path]},
    format="parquet"
)

# Conversão para DataFrame do Spark
df = datasource0.toDF()

# Transformação A: Agrupamento e sumarização
df_grouped = df.groupBy("Código").agg(
    {"`Qtde. Teórica`": "sum", "`Part. (%)`": "avg"}
)

# Transformação B: Renomear colunas para padrões amigáveis
df_grouped = df_grouped.withColumnRenamed("Código", "codigo_acao") \
                       .withColumnRenamed("sum(Qtde. Teórica)", "quantidade_teorica_total") \
                       .withColumnRenamed("avg(Part. (%))", "media_participacao")

# Transformação C: Cálculo com campos de data
df_grouped = df_grouped.withColumn("dias_ativos", lit(30)) \
                       .withColumn("data_particao", date_format(current_date(), "yyyy-MM-dd"))

# Converter de volta para DynamicFrame
dynamic_frame_transformed = DynamicFrame.fromDF(df_grouped, glueContext, "transformed")

# Verificar as colunas antes de salvar
print("Esquema do DynamicFrame:", dynamic_frame_transformed.schema())

# Salvar os dados refinados no S3 particionados
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_transformed,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["data_particao"]  # Apenas a partição data_particao
    },
    format="parquet",
    format_options={"compression": "snappy"}
)

# Registrar os dados no Glue Data Catalog
glueContext.write_dynamic_frame.from_catalog(
    frame=dynamic_frame_transformed,
    database=database_name,
    table_name=table_name
)

print(f"Dados catalogados no Glue Data Catalog no banco de dados '{database_name}', tabela '{table_name}'")

job.commit()
