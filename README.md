## Descrição do Projeto

Este projeto processa arquivos CSV de uma pasta específica, combina os dados em um único arquivo, e converte o resultado para o formato `.parquet`. O objetivo é unificar os dados de leituras em tempo real e armazená-los em um formato otimizado para consulta e análise.

## Funcionamento do Código

### Importação das Bibliotecas

```python
import pandas as pd
import os
```

O código começa importando as bibliotecas `pandas` para manipulação de dados e `os` para operações com o sistema de arquivos.

### Definição de Caminhos e Nomes de Arquivos

```python
pasta = "C:\\Tools\\pentaho\\Repositorio\\LeiturasTempoReal\\HISTORICO CONSORCIO\\LEITURAS\\UNLE\\Gerados"
pasta_dest = r"pasta\\da\\rede"
arquivo_saida_csv = os.path.join(pasta_dest, "dados_unidos_UNLE.csv")
arquivo_saida_parquet = os.path.join(pasta_dest, "historico_UNLE.parquet")
```

Os caminhos para a pasta de origem e destino, assim como os nomes dos arquivos de saída, são definidos.

### Inicialização do Contador e Lista de Dados

```python
contador = 0
dados = []
```

Um contador é inicializado para controlar o número de arquivos processados, e uma lista é criada para armazenar os dados temporariamente.

### Processamento dos Arquivos CSV

```python
for arquivo in os.listdir(pasta):
    if arquivo.endswith(".csv"):
        df = pd.read_csv(os.path.join(pasta, arquivo), delimiter=";")
        dados.append(df)
        contador += 1
        
        if contador % 50 == 0:
            df_final = pd.concat(dados)
            if os.path.exists(arquivo_saida_csv):
                df_final.to_csv(arquivo_saida_csv, mode='a', header=False, index=False, sep=';')
            else:
                df_final.to_csv(arquivo_saida_csv, mode='w', index=False, sep=';')
            dados = []
```

O código percorre todos os arquivos na pasta especificada e verifica se eles são arquivos CSV. Cada arquivo CSV é lido e seus dados são adicionados à lista. A cada 50 arquivos processados, os DataFrames são concatenados e salvos em um arquivo CSV. Se o arquivo de saída já existir, os dados são anexados; caso contrário, um novo arquivo é criado.

### Salvamento de Dados Restantes

```python
if dados:
    df_final = pd.concat(dados)
    if os.path.exists(arquivo_saida_csv):
        df_final.to_csv(arquivo_saida_csv, mode='a', header=False, index=False, sep=';')
    else:
        df_final.to_csv(arquivo_saida_csv, mode='w', index=False, sep=';')
```

Após o loop, qualquer dado restante na lista é concatenado e salvo no arquivo CSV.

### Conversão para Formato Parquet

```python
df = pd.read_csv(arquivo_saida_csv, delimiter=";")
df.to_parquet(arquivo_saida_parquet, engine='pyarrow')
os.remove(arquivo_saida_csv)
print("Concluído!")
```

O arquivo CSV unificado é lido novamente, convertido para o formato `.parquet`, e o arquivo CSV é removido para liberar espaço. Uma mensagem de conclusão é exibida.

## Como Executar

1. Certifique-se de que os arquivos CSV estão na pasta especificada.
2. Atualize os caminhos da pasta de origem e destino, se necessário.
3. Execute o script em um ambiente Python com Pandas instalado.
4. Verifique o arquivo `.parquet` gerado no diretório de destino.

## Requisitos

- Python 3.x
- Pandas
- PyArrow (para conversão para `.parquet`)

```sh
pip install pandas pyarrow
```

## Contribuições

Sinta-se à vontade para contribuir com melhorias para este projeto. Abra um pull request ou reporte um problema no repositório.

---

Espero que isso ajude a entender o funcionamento do código e como ele pode ser usado para combinar e converter dados de leituras em tempo real!