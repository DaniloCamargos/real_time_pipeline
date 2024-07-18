import functions_framework
import pandas as pd
import requests
import json
from io import StringIO
from datetime import datetime, timedelta
import pytz
from google.cloud import bigquery

def extract(url, tokenEstrutura, tokenUsuario, painelId, formato):
    try:
        fuso_horario = pytz.timezone('America/Sao_Paulo')
        atual = datetime.now(fuso_horario)
        anterior = atual - timedelta(hours=1, minutes=30)
        data_atual = atual.strftime("%Y-%m-%d %H:%M:%S")
        data_anterior = anterior.strftime("%Y-%m-%d %H:%M:%S")

        payload = {
            "tokenEstrutura": tokenEstrutura,
            "tokenUsuario": tokenUsuario,
            "dataHoraInicioCarga": data_anterior,
            "dataHoraFimCarga": data_atual,
            "painelId": painelId,
            "outputFormat": formato
        }
        headers = {
            'Content-Type': 'text/plain',
            'Cookie': '__cflb=02DiuHcRebXBbQZs3gX28EM2MeLsdaT3jC2MMTm36LJzp'
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload, ensure_ascii=False))
        response.raise_for_status()  # Levanta uma exceção para códigos de status de erro

        csv_data = response.text
        df = pd.read_csv(StringIO(csv_data), delimiter=';')
        return df

    except requests.RequestException as e:
        print(f"Erro na requisição: {e}")
        return None


def transform(data):
    
    data.rename(columns={
        "Nome Linha":"GRUPO",
        "Nome Coluna":"FILA",
        "Número Pedido": "NUMERO_ATIVIDADE",
        "Pedido Vinculado": "PEDIDO_VINCULO",
        "Pedido Origem":"ATIVIDADE_ORIGEM",
        "Login Operadora":"LOGIN_OPERADORA",
        "Nome Cliente":"NOME_CLIENTE",
        "Cpf Cnpj": "CPF_CNPJ",
        "Cidade":"CIDADE_CLIENTE",
        "Estado":"ESTADO",
        "Usuário Proprietário":"PROPRIETARIO_DO_PEDIDO",
        "Usuário Tags":"TAGS_USUARIO_PEDIDO",
        "Usuário ADM":"ADM_DO_PEDIDO",
        "Consultor na Operadora":"CONSULTOR_NA_OPERADORA",
        "Nome Equipe":"EQUIPE",
        "Nome Etapa":"ETAPA_PEDIDO",
        "Data Cadastro":"CADASTRO",
        "Data Atualizacao": "ATUALIZACAO",
        "Solicitação":"SOLICITACAO",
        "Tipo Negociacao":"TIPO_NEGOCIACAO",
        "Notas Fiscais":"NOTAS_FISCAIS",
        "Revisão":"REVISAO",
        "Item":"ITEM",
        "Numero":"NUMERO",
        "Etapa Item":"ETAPA_ITEM",
        "Portabilidade":"PORTABILIDADE",
        "PRoduto":"PRODUTO",
        "Valor":"VALOR_UNIT",
        "Quantidade":"QUANTIDADE",
        "Data Referencia":"DATA_REF",
        "Origem":"ORIGEM",
        "Data Instalação":"DATA_INSTALACAO",
        "Período":"PERIODO",
        "Cidade Instalação":"CIDADE_INSTALACAO",
        "Estado Instalação":"UF",
        "Rpon":"RPON",
        "Instância":"INSTANCIA",
        "Tags":"TAGS"
          }, inplace=True) #Renomeando colunas
    data['CPF_CNPJ'] = data['CPF_CNPJ'].astype(int)
    data['NUMERO_ATIVIDADE'] = data['NUMERO_ATIVIDADE'].astype(int)
    data['VALOR_UNIT'] = data['VALOR_UNIT'].str.replace(",",".")
    data['VALOR_UNIT'] = data['VALOR_UNIT'].astype(float)
    data['QUANTIDADE'] = data['QUANTIDADE'].astype(int)
    data['ATUALIZACAO'] = pd.to_datetime(data['ATUALIZACAO'], format='%d/%m/%Y %H:%M:%S')
    data['CADASTRO'] = pd.to_datetime(data['CADASTRO'], format='%d/%m/%Y')
    data['DATA_REF'] = pd.to_datetime(data['DATA_REF'], format='%d/%m/%Y')
    paraString = ["PEDIDO_VINCULO", "ATIVIDADE_ORIGEM","CONSULTOR_NA_OPERADORA","NOTAS_FISCAIS","TAGS_USUARIO_PEDIDO", "NUMERO", "ADM_DO_PEDIDO", "REVISAO", "ITEM", "PORTABILIDADE", "DATA_INSTALACAO", "PERIODO", "RPON", "INSTANCIA", "TAGS"]
    data[paraString] = data[paraString].astype(str) 
        
    return data

def loadBigquery(df, table_id):
    if df is None:
        return "Sem dados carregados"
    
    try:
        client = bigquery.Client()
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        return f"Loaded {job.output_rows} rows into {table_id}"

    except Exception as e:
        print(f"Erro ao carregar dados no BigQuery: {e}")
        return None

@functions_framework.http
def main(request):
    url = "https://app.neosales.com.br/producao-painel-integration-v2"
    tokenEstrutura = "02f3e71e-0e35-4ccb-93ad-ec4922cde320"
    tokenUsuario = "af0e4506-b4ac-472e-9e65-8de19351df14"
    painelId = "15005"
    formato = "csv"
    table_id = 'airy-machine-426413-i0.TesteBI.comercial'

    # Extracao de dados
    data = (extract(url, tokenEstrutura, tokenUsuario, painelId, formato))
    if data is None:
        print("Data extraction failed.")
        return {
            "status": "falha",
            "messagem": "Extracao de dados falhou."
        }, 500
    # Tratamento de dados
    df_tratado = transform(data)
    if df_tratado is None:
        return {
            "status": "falha",
            "message": "Extracao de dados falhou."
        }, 500
    
    # Carregamento de dados
    load_result = loadBigquery(df_tratado, table_id)
    if load_result is None:
        return {
            "status": "falha",
            "message": "Carregamento de dados para o BigQuery falhou."
        }, 500

    return {
        "status": "success",
        "message": load_result
    }, 200