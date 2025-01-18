import requests
import pandas as pd
from bs4 import BeautifulSoup

# URL do site da B3
url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"

# Scrap dos dados
response = requests.get(url)
soup = BeautifulSoup(response.content, 'html.parser')

# Transformar os dados em um DataFrame
# (Adapte para o formato específico do site)
data = []
# Exemplo de parse (substituir pelas classes corretas)
for row in soup.select('table tr'):
    columns = row.find_all('td')
    if columns:
        data.append([col.text.strip() for col in columns])

df = pd.DataFrame(data, columns=['Coluna1', 'Coluna2', 'Coluna3'])

# Salvar como parquet
df.to_parquet('bovespa_raw.parquet', index=False)
