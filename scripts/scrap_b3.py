import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

s3 = boto3.client('s3')
RAW_BUCKET_NAME = "raw-bovespa-bucket"

def scrap_b3(event, context):
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Parse dos dados (ajuste para a estrutura do site)
    data = []
    for row in soup.select('table tr'):
        columns = row.find_all('td')
        if columns:
            data.append([col.text.strip() for col in columns])

    # Convertendo para DataFrame
    df = pd.DataFrame(data, columns=['Coluna1', 'Coluna2', 'Coluna3'])
    file_name = f"bovespa_{datetime.now().strftime('%Y-%m-%d')}.parquet"
    df.to_parquet(file_name, index=False)

    # Upload para o bucket S3
    s3.upload_file(
        Filename=file_name,
        Bucket=RAW_BUCKET_NAME,
        Key=f"raw/{file_name}"
    )
    print(f"Arquivo {file_name} enviado para o bucket {RAW_BUCKET_NAME}.")
