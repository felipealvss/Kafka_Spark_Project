import requests as req
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import shutil
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configuração do Spark Streaming
spark = SparkSession.builder \
    .appName("APIToParquet") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Configuração do Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='kafka:9093',
    request_timeout_ms=120000)
topic = 'parquet-files'

# Informações da API e parâmetros
base_url = 'https://dadosabertos.aneel.gov.br/api/3/action/datastore_search'
resource_id = '49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a'
query = 'GD.DF'
limit = 100

# Estrutura inicial dos dados
schema = StructType([
    StructField("_id", IntegerType(), True),
    StructField("DatGeracaoConjuntoDados", StringType(), True),
    StructField("CodGeracaoDistribuida", StringType(), True),
    StructField("MdaAreaArranjo", StringType(), True),
    StructField("MdaPotenciaInstalada", StringType(), True),
    StructField("NomFabricanteModulo", StringType(), True),
    StructField("NomFabricanteInversor", StringType(), True),
    StructField("DatConexao", StringType(), True),
    StructField("MdaPotenciaModulos", StringType(), True),
    StructField("MdaPotenciaInversores", StringType(), True),
    StructField("QtdModulos", IntegerType(), True),
    StructField("NomModeloModulo", StringType(), True),
    StructField("NomModeloInversor", StringType(), True),
    StructField("rank", DoubleType(), True)
])

# Função para obter o total de registros
def get_total_records():
    url = f'{base_url}?resource_id={resource_id}&q={query}&limit=1'
    retries = 3
    delay = 2
    for attempt in range(retries):
        try:
            response = req.get(url)
            if response.status_code == 200:
                data = response.json()
                return data['result']['total']
            else:
                print(f'Erro na requisição: {response.status_code}. Tentativa {attempt + 1} de {retries}')
                time.sleep(delay)
        except Exception as e:
            print(f'Erro na requisição: {e}. Tentativa {attempt + 1} de {retries}')
            time.sleep(delay)
    raise Exception('Falha ao obter o total de registros.')

# Função para obter dados de uma URL
def fetch_data(offset):
    url = f'{base_url}?resource_id={resource_id}&q={query}&limit={limit}&offset={offset}'
    retries = 3
    delay = 5
    for attempt in range(retries):
        try:
            response = req.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                print(f'Erro na requisição: {response.status_code}. Tentativa {attempt + 1} de {retries}')
                time.sleep(delay)
        except Exception as e:
            print(f'Erro na requisição: {e}. Tentativa {attempt + 1} de {retries}')
            time.sleep(delay)
    return None

# Função para salvar dados em Parquet
def save_to_parquet(spark, records, batch_id):
    if records:
        rdd = spark.sparkContext.parallelize(records)
        df = spark.createDataFrame(rdd, schema=schema)

        # Diretório de saída
        output_path = f"/output/data_batch_{batch_id}.parquet"

        # Salvar como arquivo Parquet
        df.coalesce(1).write.mode("overwrite").parquet(output_path)

        print(f"Arquivo parquet salvo: {output_path}")

        # Enviar o caminho do arquivo .parquet para o Kafka
        try:
            producer.send(topic, output_path.encode('utf-8'))
            producer.flush()
            print(f'Arquivo {output_path} enviado para Kafka.')
        except Exception as e:
            print(f'Erro ao enviar o arquivo {output_path} para o Kafka: {e}')
    else:
        print(f"Lote {batch_id} vazio. Nenhum dado para processar.")

# Função para buscar e processar dados da API
def fetch_all_data(total_records):
    offsets = range(0, total_records, limit)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_data, offset): offset for offset in offsets}
        for batch_number, future in enumerate(as_completed(futures)):
            try:
                data = future.result()
                if data:
                    print(f'Processando dados para o offset: {futures[future]}')
                    records = data['result']['records']
                    save_to_parquet(spark, records, batch_number)
                else:
                    print(f'Nenhum dado retornado para o offset {futures[future]}')
            except Exception as e:
                print(f'Erro ao processar o futuro para o offset {futures[future]}: {e}')

# Configuração de streaming do Spark a partir do Kafka
df_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9093") \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .load()

# Transformar os dados de Kafka para o formato adequado
df_stream = df_stream.selectExpr("CAST(value AS STRING)")

# Conversão do DataFrame do stream em RDD e aplicação da função em cada micro-batch
def process_stream(batch_df, batch_id):
    records = [json.loads(record) for record in batch_df.collect()]
    save_to_parquet(spark, records, batch_id)

df_stream.writeStream.foreachBatch(process_stream).start()

# Função main para iniciar o processo
if __name__ == "__main__":
    # Obter o total de registros da API
    total_records = get_total_records()
    print(f'Total de registros disponíveis: {total_records}')

    # Obter todos os dados e processar
    fetch_all_data(total_records)

    # Fechar o producer do Kafka
    producer.close()
    
    print('Processamento completo!')

# Mantém o streaming ativo até ser interrompido
spark.streams.awaitAnyTermination()
