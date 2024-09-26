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
    bootstrap_servers='kafka:9093',  # KAFKA_ADVERTISED_LISTENERS: INSIDE://kafka:9093,OUTSIDE://localhost:9092
    request_timeout_ms=120000)  # Timeout de 120 seg
topic = 'parquet-files'

# Informações da API e parâmetros
base_url = 'https://dadosabertos.aneel.gov.br/api/3/action/datastore_search'
resource_id = '49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a'
query = 'GD.DF'  # Cenário menor de dados
limit = 100  # Número de registros por página

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
            response = req.get(url, timeout=10)  # Aumentar o timeout
            if response.status_code == 200:
                return response.json()
            else:
                print(f'Erro na requisição: {response.status_code}. Tentativa {attempt + 1} de {retries}')
                time.sleep(delay)
        except Exception as e:
            print(f'Erro na requisição: {e}. Tentativa {attempt + 1} de {retries}')
            time.sleep(delay)
    return None  # Retorna None se falhar após tentativas

# Função para buscar e processar dados da API
def fetch_all_data(total_records, limit):
    offsets = range(0, total_records, limit)
    batch_number = 0

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_data, offset): offset for offset in offsets}

        for future in as_completed(futures):
            try:
                data = future.result()
                if data:
                    print(f'Processando dados para o offset: {futures[future]}')  # Log adicional
                    records = data['result']['records']
                    rdd = spark.sparkContext.parallelize(records)
                    save_to_parquet_streaming(spark, rdd, batch_number, producer, topic)
                    batch_number += 1
                else:
                    print(f'Nenhum dado retornado para o offset {futures[future]}')
            except Exception as e:
                print(f'Erro ao processar o futuro para o offset {futures[future]}: {e}')

# Função para salvar dados em Parquet e enviar para Kafka no modo streaming
def save_to_parquet_streaming(spark, rdd, batch_id, producer, topic):
    # Verifica se o RDD tem dados
    if not rdd.isEmpty():
        # Converte o RDD para DataFrame com o esquema
        df = spark.createDataFrame(rdd, schema=schema)

        # Gera os caminhos para salvar os arquivos
        temp_output_path = f"/output/temp_data_batch_{batch_id}"
        final_output_path = f"/output/data_batch_{batch_id}.parquet"

        # Salvar como arquivo Parquet no diretório temporário
        df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

        # Mover arquivo .parquet para o diretório "output"
        for file_name in os.listdir(temp_output_path):
            if file_name.endswith(".parquet"):
                temp_file_path = os.path.join(temp_output_path, file_name)
                shutil.move(temp_file_path, final_output_path)
                break

        # Remover o diretório temporário
        shutil.rmtree(temp_output_path)

        print(f"Arquivo parquet salvo: {final_output_path}")

        # Enviar o caminho do arquivo .parquet para o Kafka
        try:
            print('Preparando send...')
            producer.send(topic, final_output_path.encode('utf-8'))
            print('Send realizado.')
            producer.flush()
            print(f'Arquivo {final_output_path} enviado para Kafka.')
        except Exception as e:
            print(f'Erro ao enviar o arquivo {final_output_path} para o Kafka: {e}')
    else:
        print(f"Lote {batch_id} vazio. Nenhum dado para processar.")

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
df_stream.writeStream.foreachBatch(lambda batch_df, batch_id: save_to_parquet_streaming(spark, batch_df.rdd, batch_id, producer, topic)).start()

# Função main para iniciar o processo
if __name__ == "__main__":
    # Obter o total de registros da API
    total_records = get_total_records()
    print(f'Total de registros disponíveis: {total_records}')

    # Obter todos os dados e processar
    fetch_all_data(total_records, limit)

    # Fechar o producer do Kafka
    producer.close()

    print('Processamento completo!')

# Mantém o streaming ativo até ser interrompido
spark.streams.awaitAnyTermination()
