import pandas as pd
import os
import glob
from utils_log import log_decorator


# Função que extrai e consolida no json
@log_decorator
def extrair_dado(pasta: str) -> pd.DataFrame:
    arquivo_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(file) for file in arquivo_json]
    df_final = pd.concat(df_list, ignore_index=True)
    return df_final
# Função que Realiza Transformação

@log_decorator
def transformar_dado(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# Uma função que da load em csv ou parquet

@log_decorator
def enviar_dados(df: pd.DataFrame,formato: list):
    """
    Parametro de entrada deve ser 'csv' ou 'parquet' ou 'ambos', 
    Args:
        df (pd.DataFrame): dataframe
        formato (list): formato do arquivo
    """
    if 'csv' in formato:
        df.to_csv('coleta.csv', index=False)
    if 'parquet' in formato:
        df.to_parquet('coleta.parquet', index=False)
        

@log_decorator
def pipeline(pasta: str, formato_saida: list):
    dataframe = transformar_dado(extrair_dado(pasta))
    enviar_dados(dataframe, formato_saida)



if __name__ == "__main__":
    pasta = 'data'
    dataframe = transformar_dado(extrair_dado(pasta))
    enviar_dados(dataframe, ['csv', 'parquet'])