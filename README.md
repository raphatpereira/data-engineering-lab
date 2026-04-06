# Data Engineering Lab

Laboratório prático de Engenharia de Dados com foco em construção de pipelines ETL reproduzíveis.

## Stack

- Apache Airflow
- Docker
- PostgreSQL
- Python
- Git

## Projeto 1 — Pipeline ETL local

Pipeline orquestrado pelo Airflow que:

1. Extrai dados
2. Transforma dados
3. Carrega no PostgreSQL

## Como executar

subir containers:

docker compose up -d

acessar airflow:

http://localhost:8080

## Objetivo

construir base sólida para evolução em:

- pipelines escaláveis
- modelagem de dados
- orquestração de workflows
