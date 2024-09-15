# Usar a imagem oficial do Spark com Hadoop
FROM bitnami/spark:latest

# Definir variáveis de ambiente
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

# Instalar dependências necessárias (Python e Java)
RUN apt-get update && apt-get install -y \
    python3-pip \
    openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Definir o diretório de trabalho
WORKDIR /app

# Copiar os arquivos do projeto para o contêiner
COPY . /app

# Instalar dependências do Python
COPY requirements.txt /app/requirements.txt
# RUN pip3 install --upgrade pip && pip3 install --no-cache-dir -r /app/requirements.txt
RUN pip3 install --upgrade pip && pip3 install requests, py4j, kafka-python-ng, pyspark, six

# Expor portas para o Spark UI e outros serviços (se necessário)
EXPOSE 4040 7077 8080 18080

# Comando padrão para iniciar o contêiner
CMD ["/bin/bash"]
