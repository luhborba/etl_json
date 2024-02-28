import pandas as pd
import os
import glob


# Função que extrai e consolida no json
def extrair_dado(pasta: str) -> pd.DataFrame:
    arquivo_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(file) for file in arquivo_json]
    df_final = pd.concat(df_list, ignore_index=True)
    return df_final
# Função que Realiza Transformação
def transformar_dado(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# Uma função que da load em csv ou parquet
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

if __name__ == "__main__":
    pasta = 'data'
    dataframe = transformar_dado(extrair_dado(pasta))
    enviar_dados(dataframe, ['csv', 'parquet'])