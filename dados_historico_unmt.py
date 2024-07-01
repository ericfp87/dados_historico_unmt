import pandas as pd
import os

# Caminho da pasta
pasta = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNMT\\Gerados"
pasta_dest = r"\\copanet04\\nfsobiee\\Exp_Leituras"


# Nome do arquivo de saída
arquivo_saida_csv = os.path.join(pasta_dest, "dados_unidos_UNMT.csv")
arquivo_saida_parquet = os.path.join(pasta_dest, "historico_UNMT.parquet")

# Contador para controlar o número de arquivos processados
contador = 0

# Lista para armazenar os dados
dados = []

# Percorre todos os arquivos na pasta
for arquivo in os.listdir(pasta):
    # Verifica se o arquivo é .csv
    if arquivo.endswith(".csv"):
        # Lê o arquivo .csv
        df = pd.read_csv(os.path.join(pasta, arquivo), delimiter=";")
        # Adiciona os dados à lista
        dados.append(df)
        
        # Incrementa o contador
        contador += 1
        
        # Se já processamos 50 arquivos, concatenamos os dataframes, salvamos como .csv e limpamos a lista de dados
        if contador % 50 == 0:
            df_final = pd.concat(dados)
            if os.path.exists(arquivo_saida_csv):
                df_final.to_csv(arquivo_saida_csv, mode='a', header=False, index=False, sep=';')
            else:
                df_final.to_csv(arquivo_saida_csv, mode='w', index=False, sep=';')
            dados = []

# Se ainda houver dados não salvos após o loop, salvamos eles agora
if dados:
    df_final = pd.concat(dados)
    if os.path.exists(arquivo_saida_csv):
        df_final.to_csv(arquivo_saida_csv, mode='a', header=False, index=False, sep=';')
    else:
        df_final.to_csv(arquivo_saida_csv, mode='w', index=False, sep=';')

# Converte o arquivo .csv unificado em um arquivo .parquet
df = pd.read_csv(arquivo_saida_csv, delimiter=";")
df.to_parquet(arquivo_saida_parquet, engine='pyarrow')
os.remove(arquivo_saida_csv)
print("Conluído!")

