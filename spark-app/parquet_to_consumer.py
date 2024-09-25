from pyspark.sql import SparkSession
from kafka import KafkaConsumer
from pyspark.sql.functions import when, col, trim, regexp_replace
from pyspark.sql.types import IntegerType, DateType, StringType, DoubleType
import time
import os
import shutil

# Configuração do Spark
spark = SparkSession.builder \
    .appName("parquet_to_consumer") \
    .getOrCreate()

# Configuração do Kafka Consumer
consumer = KafkaConsumer(
    'parquet-files',
    bootstrap_servers='kafka:9093',  # KAFKA_ADVERTISED_LISTENERS: INSIDE://kafka:9093,OUTSIDE://localhost:9092
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group')

# Função para ajustar dados do arquivo .parquet
def process_parquet(spark, parquet_file, output_parquet):

    print("Entrou na function")
    # Lendo o arquivo Parquet com PySpark
    df = spark.read.parquet(parquet_file)

    df_ = df.dropDuplicates()

    # Corrige os tipos de acordo com o prefixo da coluna
    prefixes_to_types = {
        "Qtd": IntegerType(),
        "Dat": DateType(),
        "Mda": DoubleType(),
        "Nom": StringType(),
        "_id": StringType()
    }

    def change_column_types(df_, prefixes_to_types):
        for prefix, dtype in prefixes_to_types.items():
            # identifica a coluna com prefixo
            columns_with_prefix = [col_name for col_name in df_.columns if col_name.startswith(prefix)]
            
            for column in columns_with_prefix:

                if column.startswith("Mda"):
                    df_ = df_.withColumn(column, regexp_replace(col(column), ",", "."))
                                    
                df_ = df_.withColumn(column, col(column).cast(dtype))
        
        return df_

    df_ = change_column_types(df_, prefixes_to_types)

    #Corrigindo valores vazios por nulos

    df_.createOrReplaceTempView("temp")

    for col_ in df_.columns:
        null_check = spark.sql(f"SELECT count({col_}) FROM temp WHERE {col_} = ' ' GROUP BY {col_} ORDER BY {col_} asc")

        if not null_check.isEmpty():
            df_ = df_.withColumn(
                col_,
                when(
                    trim(col(col_)) == '',
                    None
                ).otherwise(col(col_))
            )
    print("Saiu da function")
    
############################################################ OLD CODE #############################################################
    # Uso do coalesce para garantir que seja gerado apenas um arquivo
    temp_output_path = output_parquet + "_temp"

    # Salvar como arquivo Parquet no diretório temporário
    df_.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

    # Mover arquivo .parquet para o diretório "output"
    for file_name in os.listdir(temp_output_path):
        if file_name.endswith(".parquet"):
            temp_file_path = os.path.join(temp_output_path, file_name)
            shutil.move(temp_file_path, output_parquet)
            break

    # Remover o diretório temporário
    shutil.rmtree(temp_output_path)

    print(f"Arquivo parquet gerado: {output_parquet}")

if __name__ == "__main__":
    output_folder = '/output/'  # Diretório mapeado no Docker
    
    while True:
        # Verificar novos arquivos no tópico Kafka
        for message in consumer:
            parquet_file = message.value.decode('utf-8')
            print(f"Novo arquivo Parquet detectado: {parquet_file}")

            # Definir o nome do parquet de saída
            output_parquet = os.path.join(output_folder, os.path.basename(parquet_file))
            
            # Processar o arquivo .parquet e gerar/sobrepor .parquet
            process_parquet(spark, parquet_file, output_parquet)

            print(f"Arquivo {output_parquet} criado com sucesso")
            
        # Esperar 2 segundos antes de checar novos arquivos
        time.sleep(2)
