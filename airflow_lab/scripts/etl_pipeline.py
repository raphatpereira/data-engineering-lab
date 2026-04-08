import pandas as pd
import logging
from sqlalchemy import create_engine
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_data(df):

    logging.info("validando qualidade dos dados")

    if df.empty:
        raise ValueError("dataset vazio")

    if df["valor"].isnull().any():
        raise ValueError("coluna valor contém nulos")

    if (df["valor"] < 0).any():
        raise ValueError("valores negativos encontrados")

    logging.info("dados validados com sucesso")


def run_pipeline():

    file_path = "/opt/airflow/data/big_dataset.csv"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} não encontrado")

    logging.info("iniciando extração")

    df = pd.read_csv(file_path)

    logging.info(f"{len(df)} registros encontrados")

    validate_data(df)

    logging.info("iniciando transformação")

    df["valor_ajustado"] = df["valor"] * 1.1

    logging.info("carregando dados no postgres")

    engine = create_engine(
        "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    )

    df.to_sql(
        "big_table",
        engine,
        if_exists="replace",
        index=False
    )

    logging.info("pipeline finalizado com sucesso")
