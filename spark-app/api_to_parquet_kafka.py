import requests as req
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import shutil
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configurando o Spark
spark = SparkSession.builder \
    .appName("APIToParquet") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Configuração do Kafka Producer
producer = KafkaProducer(bootstrap_servers='kafka:9093', # "kafka" refere-se ao nome do serviço no docker-compose.yml 
                         request_timeout_ms=120000)  # Timeout de 120 segundos
topic = 'parquet-files'

# URL base da API e parâmetros
base_url = 'https://dadosabertos.aneel.gov.br/api/3/action/datastore_search'
resource_id = '49fa9ca0-f609-4ae3-a6f7-b97bd0945a3a'
#query = 'GD.CE' # Cenário menor de dados
query = 'GD.DF' # Cenário menor de dados
limit = 100  # Número de registros por página

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

# Função para salvar dados em Parquet e enviar para Kafka
def save_to_parquet(spark, data, batch_number, producer, topic):
    # Criar DataFrame do PySpark a partir dos dados
    records = data['result']['records']
    df = spark.createDataFrame(records, schema)

    # Coalesce para garantir que seja gerado apenas um arquivo
    temp_output_path = f"/output/temp_data_batch_{batch_number}"
    final_output_path = f"/output/data_batch_{batch_number}.parquet"

    # Salvar como arquivo Parquet no diretório temporário
    df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

    # Mover o arquivo .parquet gerado para o diretório /output
    # Isso considera que o nome do arquivo dentro do diretório temporário pode variar
    for file_name in os.listdir(temp_output_path):
        if file_name.endswith(".parquet"):
            temp_file_path = os.path.join(temp_output_path, file_name)
            shutil.move(temp_file_path, final_output_path)
            break

    # Remover o diretório temporário
    shutil.rmtree(temp_output_path)

    print(f"Arquivo parquet salvo: {final_output_path}")

    # Enviar o caminho do arquivo Parquet para o Kafka
    try:
        print('Preparando send...')
        producer.send(topic, final_output_path.encode('utf-8'))
        print('Send realizado.')
        producer.flush()
        print(f'Arquivo {final_output_path} enviado para Kafka.')
    except Exception as e:
        print(f'Erro ao enviar o arquivo {final_output_path} para o Kafka: {e}')

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
                    save_to_parquet(spark, data, batch_number, producer, topic)
                    batch_number += 1
                else:
                    print(f'Nenhum dado retornado para o offset {futures[future]}')
            except Exception as e:
                print(f'Erro ao processar o futuro para o offset {futures[future]}: {e}')

# Função principal para iniciar o processo
if __name__ == "__main__":
    # Obter o total de registros da API
    total_records = get_total_records()
    print(f'Total de registros disponíveis: {total_records}')

    # Obter todos os dados e processar
    fetch_all_data(total_records, limit)

    # Fechar o producer do Kafka
    producer.close()

    print('Processamento completo!')
