import boto3
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from io import StringIO


s3 = boto3.client('s3')
RAW_BUCKET_NAME = "bucketfiapgrupo129-tech2"

def scrap_b3(event, context):
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Modo headless para pipelines
        page = browser.new_page()
        page.goto(url)

        # Selecionar 120 registros por página
        page.wait_for_selector("#selectPage")  # Esperar o seletor de página carregar
        page.select_option("#selectPage", "120")  # Selecionar o valor "120" no dropdown
        page.wait_for_timeout(3000)  # Esperar o carregamento da página (ajuste o tempo conforme necessário)


        # Esperar que a tabela carregue na página
        page.wait_for_selector("table.table.table-responsive-sm.table-responsive-md")

        # Obter o conteúdo da tabela renderizada
        content = page.content()

        # Fechar o navegador
        browser.close()

    soup = BeautifulSoup(content, 'html.parser')

    # Buscar a data no elemento <h2>
    date_element = soup.find("h2")
    extracted_date = None
    if date_element:
        extracted_date = date_element.get_text(strip=True).replace("Carteira do Dia - ", "").replace("/", "-")
        print("Data extraída:", extracted_date)


    table = soup.find('table')  # Localize a tabela no HTML

    if table:
        # Ler a tabela usando StringIO
        df = pd.read_html(StringIO(str(table)))[0]

        # Remover as duas últimas linhas
        df = df.iloc[:-2]  # Remove as últimas duas linhas do DataFrame

        # Exibir DataFrame ajustado
        print("data", df)
    else:
        print("No table found in the HTML.")
    
    file_name = f"bovespa_{extracted_date}.parquet"
    df.to_parquet(file_name, index=False)

    # Upload para o bucket S3
    s3.upload_file(
        Filename=file_name,
        Bucket=RAW_BUCKET_NAME,
        Key=f"raw/data/{file_name}"
    )
    print(f"Arquivo {file_name} enviado para o bucket {RAW_BUCKET_NAME}.")