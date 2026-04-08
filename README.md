# Data Engineering Lab

Projeto prático de engenharia de dados usando Airflow + Docker + Postgres.

## arquitetura

Airflow orquestra pipeline ETL:

CSV -> Pandas -> PostgreSQL

## stack

- Apache Airflow
- Docker
- PostgreSQL
- Python (pandas, sqlalchemy)
- Git / GitHub

## pipeline

1 extração de dados CSV
2 transformação de dados
3 carga no banco postgres

## execução

docker compose up -d

abrir:

http://localhost:8080

usuario:
airflow

senha:
airflow

## estrutura

airflow_lab/

dags/ → orquestração
scripts/ → lógica ETL
data/ → dados de entrada
logs/ → logs airflow

## objetivo

construir base sólida em engenharia de dados moderna
