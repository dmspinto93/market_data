import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import os
import re  # <--- Adicione esta linha aqui
import urllib.parse
from sqlalchemy import create_engine, text

# Configurações do Banco de Dados via variáveis de ambiente
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "varejo_db")


def extrair_dados_buscape():
    produtos_extraidos = []
    data_atual = datetime.today().strftime("%Y-%m-%d")

    # Lista de categorias e suas URLs base
    buscas = [
        ('Smartphone Apple', 'https://www.buscape.com.br/celular/iphone?page='),
        ('Smart TV', 'https://www.buscape.com.br/tv/smart-tv?page=')
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}

    for categoria, url_base in buscas:
        for i in range(1, 11):  # Varre até a página 10
            url_pag = f"{url_base}{i}"
            print(f"Buscando: {url_pag}")

            try:
                site = requests.get(url_pag, headers=headers)
                soup = BeautifulSoup(site.content, 'html.parser')
                # Atenção: As classes CSS do Buscapé mudam com frequência. Ajuste se necessário.
                produtos = soup.find_all('div', attrs={'class': re.compile(r'Hits_ProductCard__Bonl_')})

                for produto in produtos:
                    try:
                        apresentacao = produto.find('h2').get_text().strip()
                    except:
                        apresentacao = 'N/I'

                    try:
                        # Simplificação da busca pelas classes
                        loja = produto.find('div', attrs={'class': 'BestOfferMerchant_OrqProductCard_BestOfferMerchant__GByb1'}).get_text().strip()
                    except:
                        loja = 0.0

                    try:
                        # Simplificação da busca pelas classes
                        preco_texto = produto.find('div', attrs={'class': 'Price_OrqProductCard_Price__TNBZB'}).get_text().strip()
                        # Limpeza do preço (Ex: "R$ 1.200,50" -> 1200.50)
                        preco = float(preco_texto.replace('R$', '').replace('.', '').replace(',', '.').strip())
                    except:
                        preco = 0.0

                    try:
                        # Simplificação da busca pelas classes
                        condicoes = produto.find('div', attrs={'class': 'Installment_OrqProductCard_Installment__Hpa6k'}).get_text().strip()
                    except:
                        condicoes = 'N/I'

                    # Aqui você pode adicionar as lógicas de frete e cashback conforme o original
                    try:
                        # Simplificação da busca pelas classes
                        vantagem = produto.find('div', attrs={'class': 'Cashback_OrqProductCard_Cashback__SkQmD'}).get_text().strip()
                    except:
                        vantagem = 'N/I'

                    produtos_extraidos.append({
                        'APRESENTACAO': apresentacao,
                        'LOJA': loja,
                        'CATEGORIA': categoria,
                        'PRECO': preco,
                        'CONDICOES': condicoes,
                        'VANTAGEM': vantagem,
                        'DATA_REF': data_atual
                    })
            except Exception as e:
                print(f"Erro na página {url_pag}: {e}")

    df = pd.DataFrame(produtos_extraidos)
    df.drop_duplicates(inplace=True)

    # Salvar o CSV na pasta data localmente
    os.makedirs('./data', exist_ok=True)
    nome_arquivo = f"./data/lista_preco_varejo_{data_atual.replace('-', '')}.csv"
    df.to_csv(nome_arquivo, sep='|', index=False, encoding='utf-8')
    print(f"Arquivo salvo: {nome_arquivo}")

    return df, data_atual


def ingerir_postgresql(df, data_atual):
    if df.empty:
        print("Nenhum dado para inserir.")
        return

    # Codifica a senha para evitar conflitos com caracteres especiais como @, #, etc.
    senha_codificada = urllib.parse.quote_plus(DB_PASS)

    # Conexão com SQLAlchemy
    engine = create_engine(f"postgresql://{DB_USER}:{senha_codificada}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    nome_tabela_pai = "fato_lista_produtos"
    nome_particao = f"fato_lista_produtos_{data_atual.replace('-', '_')}"

    with engine.connect() as conn:
        # 1. Garantir que a tabela pai particionada existe (Partição por Lista usando a DATA_REF)
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {nome_tabela_pai} (
                "APRESENTACAO" VARCHAR,
                "LOJA" VARCHAR,
                "CATEGORIA" VARCHAR,
                "PRECO" FLOAT,
                "CONDICOES" VARCHAR,
                "VANTAGEM" VARCHAR,
                "DATA_REF" DATE
            ) PARTITION BY LIST ("DATA_REF");
        """))

        # 2. Criar a partição específica para a data de hoje, se não existir
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {nome_particao} 
            PARTITION OF {nome_tabela_pai} 
            FOR VALUES IN ('{data_atual}');
        """))
        conn.commit()

    # 3. Inserir os dados diretamente na tabela pai (o Postgres roteia para a partição correta)
    df.to_sql(nome_tabela_pai, engine, if_exists='append', index=False)
    print(f"Dados inseridos com sucesso na partição {nome_particao}.")


if __name__ == "__main__":
    df_raspagem, data_hoje = extrair_dados_buscape()
    ingerir_postgresql(df_raspagem, data_hoje)