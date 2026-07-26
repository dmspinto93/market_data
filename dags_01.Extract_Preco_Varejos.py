import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import pandas as pd
import os
from google.cloud import bigquery
from google.cloud import storage
from airflow import DAG
from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator
from airflow.utils.dates import days_ago
from time import perf_counter

arquivos = ['/lista_preco_varejo']
data = datetime.today()
data = format(data, "%Y") + format(data, "%m") + format(data, "%d")
# CONSTROI O OBJETO DE CLIENTE DO BIG QUERY.
client = bigquery.Client()
# SETAR DADOS DA TABELA A SER CRIADA E/OU CARREGADA
table_id = "peppy-breaker-412900.varejo_dataset.fato_lista_produtos"

# ACESSAR SITE PARA RASPAGEM DOS DADOS
def getPage():
    url = 'https://www.buscape.com.br/celular/iphone'
    ult_pagina = 10
    
    for i in range(1, int(ult_pagina)):
        url_pag = f'https://www.buscape.com.br/celular/iphone?page={i}'
        site = requests.get(url_pag)
        soup = BeautifulSoup(site.content, 'html.parser')
        produtos = soup.find_all('div', attrs = {'class' : 'Paper_Paper__4XALQ Paper_Paper__bordered__cl5Rh Card_Card__Zd8Ef Card_Card__clicable__ewI68 ProductCard_ProductCard__WWKKW'})
        
        with open('/home/airflow/gcs/data/lista_preco_varejo.csv' 
                  , 'a' 
                  , newline = '\n'
                  , encoding = 'cp1252') as f:
            
            for produto in produtos:
                apresentacao = produto.find('h2', attrs = {'class' : 'Text_Text__ARJdp Text_MobileLabelXs__dHwGG Text_DesktopLabelSAtLarge__wWsED ProductCard_ProductCard_Name__U_mUQ'}).get_text().strip()
                categoria = 'Smartphone Apple'
                try:
                    rede_melhor_preco = produto.find('h3', attrs = {'class' : 'Text_Text__ARJdp Text_MobileLabelXs__dHwGG Text_MobileLabelSAtLarge__m0whD ProductCard_ProductCard_BestMerchant__JQo_V'}).get_text().strip()
                    rede_melhor_preco = rede_melhor_preco[16:]
                except:
                    rede_melhor_preco = 'Rede nao localizada'
                try:
                    cash_back = produto.find('p', attrs = {'class' : 'Text_Text__ARJdp Text_MobileTagXs___PwYE ProductCard_ProductCard_CashbackInfoLabel__Td_EJ'}).get_text().strip()
                except:
                    cash_back = 'Sem cash back'
                try:
                    frete = produto.find('p', attrs = {'class' : 'Text_Text__ARJdp Text_MobileTagXs___PwYE ProductCard_ProductCard_FreeShippingInfo__quzvT'}).get_text().strip()
                except:
                    frete = 'Possui Frete'    
                preco = produto.find('p', attrs = {'class' : 'Text_Text__ARJdp Text_MobileHeadingS__HEz7L'}).get_text().strip()
                preco = str(preco[3:])
                try:
                    parcela = produto.find('span', attrs = {'class' : 'Text_Text__ARJdp Text_MobileLabelXs__dHwGG Text_MobileLabelSAtLarge__m0whD ProductCard_ProductCard_Installment__XZEnD'}).get_text().strip()
                except:
                    parcela = 'Somente a vista'
                data = datetime.today()
                data = format(data, "%Y-%m-%d")
                
                linha = apresentacao + '|' + categoria + '|' + rede_melhor_preco + '|' + cash_back + '|' + frete + '|' + preco + '|' + parcela + '|' + data + '\n'
                print(linha)
                
                #Confecçãoo do arquivo csv com a lista de produtos
                f.write(linha)
                
        print(url_pag)
    
    df = pd.read_csv('/home/airflow/gcs/data/lista_preco_varejo.csv'
                     , encoding = 'cp1252'
                     , delimiter = '|')
    
    df = df.drop_duplicates()
    df = df.drop_duplicates()
    
    
    df.to_csv('/home/airflow/gcs/data/lista_preco_varejo.csv'
              , header = ['APRESENTACAO', 'CATEGORIA', 'REDE_MELHOR_PRECO', 'CASH_BACK', 'FRETE', 'PRECO', 'PARCELA', 'DATA_REF']
              , sep = '|'
              , lineterminator = '\n'
              , encoding = 'cp1252'
              , index = False
              , na_rep = 'N/I'
              , quotechar = "'"
              )
    
    for i in range(1, int(ult_pagina)):
        url_pag = f'https://www.buscape.com.br/tv/smart-tv?page={i}'
        site = requests.get(url_pag)
        soup = BeautifulSoup(site.content, 'html.parser')
        produtos = soup.find_all('div', attrs = {'class' : 'Paper_Paper__4XALQ Paper_Paper__bordered__cl5Rh Card_Card__Zd8Ef Card_Card__clicable__ewI68 ProductCard_ProductCard__WWKKW'})
        
        with open('/home/airflow/gcs/data/lista_preco_varejo.csv' 
                  , 'a' 
                  , newline = '\n'
                  , encoding = 'cp1252') as f:
            
            for produto in produtos:
                apresentacao = produto.find('h2', attrs = {'class' : 'Text_Text__ARJdp Text_MobileLabelXs__dHwGG Text_DesktopLabelSAtLarge__wWsED ProductCard_ProductCard_Name__U_mUQ'}).get_text().strip()
                categoria = 'Smart TV'
                try:
                    rede_melhor_preco = produto.find('h3', attrs = {'class' : 'Text_Text__ARJdp Text_MobileLabelXs__dHwGG Text_MobileLabelSAtLarge__m0whD ProductCard_ProductCard_BestMerchant__JQo_V'}).get_text().strip()
                    rede_melhor_preco = rede_melhor_preco[16:]
                except:
                    rede_melhor_preco = 'Rede nao localizada'
                try:
                    cash_back = produto.find('p', attrs = {'class' : 'Text_Text__ARJdp Text_MobileTagXs___PwYE ProductCard_ProductCard_CashbackInfoLabel__Td_EJ'}).get_text().strip()
                except:
                    cash_back = 'Sem cash back'
                try:
                    frete = produto.find('p', attrs = {'class' : 'Text_Text__ARJdp Text_MobileTagXs___PwYE ProductCard_ProductCard_FreeShippingInfo__quzvT'}).get_text().strip()
                except:
                    frete = 'Possui Frete'    
                preco = produto.find('p', attrs = {'class' : 'Text_Text__ARJdp Text_MobileHeadingS__HEz7L'}).get_text().strip()
                preco = str(preco[3:])
                try:
                    parcela = produto.find('span', attrs = {'class' : 'Text_Text__ARJdp Text_MobileLabelXs__dHwGG Text_MobileLabelSAtLarge__m0whD ProductCard_ProductCard_Installment__XZEnD'}).get_text().strip()
                except:
                    parcela = 'Somente a vista'
                data = datetime.today()
                data = format(data, "%Y-%m-%d")
                
                linha = apresentacao + '|' + categoria + '|' + rede_melhor_preco + '|' + cash_back + '|' + frete + '|' + preco + '|' + parcela + '|' + data + '\n'
                print(linha)
                
                #Confecçãoo do arquivo csv com a lista de produtos
                f.write(linha)
                
        print(url_pag)
    
    df = pd.read_csv('/home/airflow/gcs/data/lista_preco_varejo.csv'
                     , encoding = 'cp1252'
                     , delimiter = '|')
    
    df = df.drop_duplicates()
    df = df.drop_duplicates()
    
    df.to_csv('/home/airflow/gcs/data/lista_preco_varejo.csv'
              , header = ['APRESENTACAO', 'CATEGORIA', 'REDE_MELHOR_PRECO', 'CASH_BACK', 'FRETE', 'PRECO', 'PARCELA', 'DATA_REF']
              , sep = '|'
              , lineterminator = '\n'
              , encoding = 'cp1252'
              , index = False
              , na_rep = 'N/I'
              , quotechar = "'"
              )
    return site

# FAZER UPLOAD DOS DADOS LOCAIS PARA O CLOUD STORAGE
def uploadCloudStorage():
    for arquivo in arquivos:
        #def upload_blob(bucket_name, source_file_name, destination_blob_name):
        """Upload do arquivo no bucket"""
        #Id do bucket
        bucket_name = 'dimas_teste-1'
        #Atalho da origem do arquivo
        source_file_name = '/home/airflow/gcs/data' + arquivo + '.csv'
        #Id do objeto GCS
        destination_blob_name = 'dados_varejo/' + arquivo[1:] + '_' + data + '.csv'
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        
        blob.upload_from_filename(source_file_name)
        
        print(
            'Arquivo {} carregado em {}.'.format(
                source_file_name, destination_blob_name
                )
            )
    return blob

# REMOVER ARQUIVOS DA PASTA LOCAL
def removeLocalFile():
    for delete in arquivos:
        try:
            print('Removendo arquivo: {}'.format(delete[1:]))
            os.remove('/home/airflow/gcs/data' + delete + '.csv')
            print('Arquivo {} removido do diretorio local'.format(delete[1:]))
        except:
            print('Arquivo {} não existe no diretorio'.format(delete[1:]))
    return os

# REALIZAR INGESTAO DOS DADOS DO CLOUD STORAGE PARA O BIG QUERY
def ingestBigQuery():
    # CONFIGURAR DADOS DO CLOUD STORAGE A SER GRAVADO NO BIG QUERY
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("APRESENTACAO", "STRING"),
            bigquery.SchemaField("CATEGORIA", "STRING"),
            bigquery.SchemaField("REDE_MELHOR_PRECO", "STRING"),
            bigquery.SchemaField("CASH_BACK", "STRING"),
            bigquery.SchemaField("FRETE", "STRING"),
            bigquery.SchemaField("PRECO", "FLOAT"),
            bigquery.SchemaField("PARCELA", "STRING"),
            bigquery.SchemaField("DATA_REF", "DATE"),
        ],
        #skip_leading_rows = 1,
        autodetect = True,
        #time_partitioning = bigquery.TimePartitioning(
        #    field = "DATA_REF",  # nome da coluna a ser particionada
        #),
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND, # seleciona se será adicional ou substituida a base
        source_format = bigquery.SourceFormat.CSV,
    )
    
    # ATALHO DO CLOUD STORAGE A SER LIDO
    uri = "gs://dimas_teste-1/dados_varejo/lista_preco_varejo" + "_" + data + ".csv"
    
    # GRAVACAO DOS DADOS NO BIG QUERY
    load_job = client.load_table_from_uri(
        uri, table_id, job_config = job_config
    )  # requisicao da API
    
    load_job.result()  # aguarda fim da atividade
    
    table = client.get_table(table_id)
    print("Carregados {} linhas a tabela {}".format(table.num_rows, table_id))
    return load_job


# DEFINICAO DAS DAGS NO AIRFLOW
default_args = {
    'owner' : 'airflow'
    ,'depends_on_past' : False
    #Set da data de inicio da execucao
    ,'start_date' : datetime(2024,2,24)
    ,'email' : ['dmspinto93@gmail.com']
    ,'email_on_failure' : False
    ,'email_on_retry' : False
    #Tentativas caso apresente falhas
    ,'retries' : 1
    #Tempo para tentar novamente, caso dê falha no proceso
    ,'retry_delay' : timedelta(minutes = 10)
    #Agendador, para iniciar a tarefa automaticamente
    ,'schedule_interval' : '0 9 * * *'}

with DAG(dag_id = 'Update_Market_Data'
         ,default_args = default_args
         ,tags = ['']
         ) as dag:
    
    t1 = python_operator.PythonOperator(
        task_id = 'download_data'
        ,python_callable = getPage
        ,do_xcom_push = False
        ,dag = dag)
    
    t2 = python_operator.PythonOperator(
        task_id = 'upload_google_cloud_storage'
        ,python_callable = uploadCloudStorage
        ,do_xcom_push = False
        ,dag = dag)
    
    t3 = python_operator.PythonOperator(
        task_id = 'remove_local_files'
        ,python_callable = removeLocalFile
        ,do_xcom_push = False
        ,dag = dag)
    
    t4 = python_operator.PythonOperator(
        task_id = 'big_query_ingestion'
        ,python_callable = ingestBigQuery
        ,do_xcom_push = False
        ,dag = dag)
    
    t1 >> t2 >> t3 >> t4