import pandas as pd
import logging
from sqlalchemy import create_engine
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

FILE_PATH = "/opt/airflow/data/big_dataset.csv"

def extract():

    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"{FILE_PATH} não encontrado")

    logging.info("extraindo dados")

    df = pd.read_csv(FILE_PATH)

    return df.to_json()


def transform(df_json):

    logging.info("transformando dados")

    df = pd.read_json(df_json)

    if df.empty:
        raise ValueError("dataset vazio")

    if (df["valor"] < 0).any():
        raise ValueError("valor negativo encontrado")

    # converter coluna de data
    df["data"] = pd.to_datetime(df["data"])

    # filtro incremental
    data_limite = "2024-01-03"

    df = df[df["data"] > data_limite]

    # transformação principal
    df["valor_ajustado"] = df["valor"] * 1.1

    return df.to_json()


def load(df_json):

    logging.info("carregando dados")

    df = pd.read_json(df_json)

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )

    df.to_sql(
        "big_table",
        engine,
        if_exists="replace",
        index=False
    )

    logging.info("carga concluída")
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
)
