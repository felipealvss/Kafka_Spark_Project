import requests as req
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from kafka import KafkaProducer

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

# Função para enviar dados para o Kafka
def send_to_kafka(records):
    if records:
        for record in records:
            try:
                producer.send(topic, json.dumps(record).encode('utf-8'))
            except Exception as e:
                print(f'Erro ao enviar o registro para o Kafka: {e}')
    else:
        print("Nenhum dado para enviar.")

# Função para buscar e processar dados da API
def fetch_all_data(total_records):
    offsets = range(0, total_records, limit)
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fetch_data, offset): offset for offset in offsets}
        for future in as_completed(futures):
            try:
                data = future.result()
                if data:
                    records = data['result']['records']
                    send_to_kafka(records)
                else:
                    print(f'Nenhum dado retornado para o offset {futures[future]}')
            except Exception as e:
                print(f'Erro ao processar o futuro para o offset {futures[future]}: {e}')

# Função main para iniciar o processo
if __name__ == "__main__":
    # Obter o total de registros da API
    total_records = get_total_records()
    print(f'Total de registros disponíveis: {total_records}')

    # Obter todos os dados e enviar para o Kafka
    fetch_all_data(total_records)

    # Fechar o producer do Kafka
    producer.close()
    
    print('Processamento completo!')
