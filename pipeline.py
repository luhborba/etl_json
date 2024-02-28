from etl import pipeline

pasta: str = 'data/'
formato: list = ['csv', 'parquet']

pipeline(pasta, formato)