# Pipeline de dados Vivo Fibra


## Descrição

Este projeto desenvolve um pipeline de dados "near-real time". O pipeline realiza a extração de dados via API utilizando a biblioteca `requests`, trata os dados com `pandas` através de Google Cloud Functions e os carrega para o BigQuery. O agendamento é realizado através do Google Cloud Scheduler, e os dados são extraídos do BigQuery para o Power BI com atualização automática.
<img src="/docs/Pipeline_Dados.png">


## Estrutura do Projeto
│

├── README.md

├── LICENSE
├── .gitignore

├── requirements.txt

├── docs/

  │├── architecture.md

  │├── api_documentation.md

  │└── powerbi_setup.md

├── cloud_functions/

  │├── main.py

├── bigquery/

  │├── schema.sql

  │├── create_tables.sql

  │└── queries/

       │├── query1.sql

│── scheduler/

  │├── schedule_config.yaml
  │└── scheduler_setup.md



## Pré-requisitos


- Python 3.7 ou superior

- Conta no Google Cloud Platform (GCP)

- Projeto configurado no GCP com BigQuery
- Cloud Functions habilitados

- Power BI Service


### BigQuery
Crie o dataset e a tabela no BigQuery usando os scripts em `bigquery/`:

```sh
    
bq mk --dataset projeto_id:dataset_id

bq query --use_legacy_sql=false < bigquery/schema.sql

```

## Configuração


### Google Cloud Functions


1. Navegue até o diretório da função:
    
```sh
    
cd cloud_functions
    
```


2. Implante a Cloud Function:
    
```sh
    
gcloud functions deploy main.py --runtime python39 --trigger-http --allow-unauthenticated --entry-point main

```

### Google Cloud Scheduler


Configure o agendamento conforme o arquivo `scheduler/schedule_config.yaml`:

```sh

gcloud scheduler jobs create http JOB_NAME --schedule "*/5 * * * *" --uri FUNCTION_URL --http-method POST

```

## Uso


A Cloud Function será acionada conforme agendado pelo Cloud Scheduler, extraindo dados da API, transformando-os e carregando-os no BigQuery.
No Power BI, configure a atualização automática para carregar os dados do BigQuery.

