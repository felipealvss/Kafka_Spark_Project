import streamlit as st
import os
import pandas as pd
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import time

# Diretório dos arquivos .parquet
output_dir = './output'

# Medição de incício de execução
start_time = time.time()

# Título da página Streamlit
st.title('Monitoramento de Arquivos Parquet')

#Função para leitura de arquivos .parquet
def read_parquet_files(output_dir):
    total_rows = 0
    data_frames = []

    for file in os.listdir(output_dir):
        if file.endswith('.parquet'):
            try:
                parquet_file = pq.ParquetFile(os.path.join(output_dir, file))

                df = parquet_file.read().to_pandas()
                
                # Realizar ação: Conversão de tipos para garantir a compatibilidade
                df['_id'] = df['_id'].astype(str)
                df['CodGeracaoDistribuida'] = df['CodGeracaoDistribuida'].astype(str)

                # Ajuste de campos de data
                if df['DatConexao'].dtype == object:
                    df['DatConexao'] = pd.to_datetime(df['DatConexao'], errors='coerce')
                if df['DatGeracaoConjuntoDados'].dtype == object:
                    df['DatGeracaoConjuntoDados'] = pd.to_datetime(df['DatGeracaoConjuntoDados'], errors='coerce')

                # Verifica dados para ajuste de campos numéricos (ponto e virgula)
                def safe_numeric_conversion(column):
                    if column.dtype == object:
                        return pd.to_numeric(column.str.replace(',', '.'), errors='coerce')
                    return pd.to_numeric(column, errors='coerce')

                # Aplica conversão para numéricos
                df['MdaAreaArranjo'] = safe_numeric_conversion(df['MdaAreaArranjo'])
                df['MdaPotenciaInstalada'] = safe_numeric_conversion(df['MdaPotenciaInstalada'])
                df['MdaPotenciaModulos'] = safe_numeric_conversion(df['MdaPotenciaModulos'])
                df['MdaPotenciaInversores'] = safe_numeric_conversion(df['MdaPotenciaInversores'])
                df['QtdModulos'] = safe_numeric_conversion(df['QtdModulos'])

                data_frames.append(df)
            except Exception as e:
                st.error(f"Erro ao ler {file}: {e}")

    # Todos os dados
    all_data = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

    # todos os registros
    total_rows = len(all_data)

    return all_data, total_rows

# Cria o placeholder do Streamlit
placeholder = st.empty()

while True: # Sempre em execução

    # Contagem de arquivos
    current_files_count = len([f for f in os.listdir(output_dir) if f.endswith('.parquet')])
    
    all_data, total_rows = read_parquet_files(output_dir)  # Recalcula todos os dados a cada iteração

    with placeholder.container():

        # Cards de acompanhamento
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric(label="Total de Arquivos 📂", value=current_files_count)

        with col2:
            st.metric(label="Total de Linhas 📈", value=total_rows)

        with col3:
            execution_time = time.time() - start_time
            st.metric(label="Tempo de Execução ⏱️", value=f"{execution_time:.2f} seg.")

        # Gráfico de dados agrupados
        if not all_data.empty:
            st.subheader("TOP 5 Fabricantes de Módulos e Inversores")
            modulo_count = all_data['NomFabricanteModulo'].value_counts().nlargest(5)
            inversor_count = all_data['NomFabricanteInversor'].value_counts().nlargest(5)

            fig, ax = plt.subplots(1, 2, figsize=(12, 5))
            modulo_count.plot(kind='bar', ax=ax[0], color='skyblue')
            ax[0].set_title('Fabricantes de Módulos')
            ax[0].set_xlabel('Fabricante')
            ax[0].set_ylabel('Quantidade')

            inversor_count.plot(kind='bar', ax=ax[1], color='lightgreen')
            ax[1].set_title('Fabricantes de Inversores')
            ax[1].set_xlabel('Fabricante')
            ax[1].set_ylabel('Quantidade')

            st.pyplot(fig)

            # Dados totais em Dataframe
            st.subheader("informação total dos Arquivos")
            st.dataframe(all_data)
        else:
            st.write("Nenhum dado disponível para exibir.")

    # Tempo de espera antes da próxima atualização
    time.sleep(0.1)
