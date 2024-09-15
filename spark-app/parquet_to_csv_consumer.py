from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import time
import os
import shutil

# Configuração do Spark
spark = SparkSession.builder \
    .appName("ParquetToCSV") \
    .getOrCreate()

# Consumer Kafka
consumer = KafkaConsumer(
    'parquet-files',
    bootstrap_servers='kafka:9093',  # "kafka" refere-se ao nome do serviço no docker-compose.yml
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group')

def process_parquet_to_csv(spark, parquet_file, output_csv):
    # Lendo o arquivo Parquet com PySpark
    df = spark.read.parquet(parquet_file)
    
    # Gerar apenas um arquivo CSV com coalesce(1)
    temp_output_path = output_csv + "_temp"
    
    # Escrevendo o CSV temporariamente
    df.coalesce(1).write.mode("overwrite").csv(temp_output_path, header=True)
    
    # Mover o arquivo CSV gerado do diretório temporário para o caminho final
    for file_name in os.listdir(temp_output_path):
        if file_name.endswith(".csv"):
            temp_file_path = os.path.join(temp_output_path, file_name)
            shutil.move(temp_file_path, output_csv)
            break

    # Remover o diretório temporário
    shutil.rmtree(temp_output_path)

    print(f"Arquivo CSV gerado: {output_csv}")

if __name__ == "__main__":
    output_folder = '/output/'  # Diretório mapeado no Docker
    
    while True:
        # Verificar novos arquivos no tópico Kafka
        for message in consumer:
            parquet_file = message.value.decode('utf-8')
            print(f"Novo arquivo Parquet detectado: {parquet_file}")
            
            # Definir o nome do CSV de saída
            output_csv = os.path.join(output_folder, os.path.basename(parquet_file).replace('.parquet', '.csv'))
            
            # Processar o arquivo Parquet e gerar CSV
            process_parquet_to_csv(spark, parquet_file, output_csv)
            
        # Esperar 2 segundos antes de checar novos arquivos
        time.sleep(2)
