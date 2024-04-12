import pandas as pd
import os # Operation System - comunicar com o sistema operacional (consegue-se executar os comandos do terminal do windows)
import glob
from log import log_decorator

# função de extract que lê e consolida os json's
@log_decorator
def extrair_dados(pasta:str)-> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta,'*.json')) #lista tudo que está dentro da pasta 'data'
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list,ignore_index=True) #para ignorar os índices originais dos arquivos
    return df_total

# função que transforma
@log_decorator
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# função que carrega os dados
@log_decorator
def carregar_dados(df: pd.DataFrame,formato_saida: list):
    """
    parâmetro que vai ser "csv" ou "parquet" ou "os dois"
    """
    for formato in formato_saida:
        if formato == 'csv':
            df.to_csv("dados.csv")
        if formato == 'parquet':
            df.to_parquet("dados.parquet")
            
@log_decorator
def pipeline_calcular_kpi_de_vendas_consolidado(pasta: str,formato_saida: list):
    dados = extrair_dados(pasta)
    dados_transformados = calcular_kpi_de_total_de_vendas(dados)
    carregar_dados(dados_transformados, formato_saida)