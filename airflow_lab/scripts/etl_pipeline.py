import pandas as pd
import logging
from sqlalchemy import create_engine, types
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

FILE_PATH = "/opt/airflow/data/big_dataset.csv"


def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )


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

    # conversão de data
    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    mask_invalid = df["data"].isna()

    if mask_invalid.any():
        df.loc[mask_invalid, "data"] = pd.to_datetime(
            df.loc[mask_invalid, "data"], unit="ms", errors="coerce"
        )

    if df["data"].isna().any():
        raise ValueError("Existem valores inválidos na coluna data após conversão")

    logging.info(f"linhas antes do filtro: {len(df)}")

    engine = get_engine()

    try:
        query = "SELECT MAX(data) as ultima_data FROM big_table"
        ultima_data = pd.read_sql(query, engine)["ultima_data"].iloc[0]

        if pd.notna(ultima_data):
            logging.info(f"ultima data encontrada no banco: {ultima_data}")
            df = df[df["data"] > ultima_data]

    except Exception:
        logging.info("tabela ainda não existe — carga inicial completa")

    logging.info(f"linhas após filtro incremental: {len(df)}")

    df["valor_ajustado"] = df["valor"] * 1.1

    # tipagem
    df = df.astype({
        "id": "int64",
        "nome": "string",
        "valor": "float",
        "valor_ajustado": "float"
    })

    return df.to_json(date_format="iso")


def load(df_json):
    logging.info("carregando dados")

    df = pd.read_json(df_json)

    if df.empty:
        logging.info("nenhum dado novo para carregar")
        return

    logging.info(f"inserindo {len(df)} linhas no banco")

    engine = get_engine()

    df.to_sql(
        "big_table",
        engine,
        if_exists="append",
        index=False,
        dtype={
            "id": types.Integer(),
            "nome": types.String(),
            "valor": types.Float(),
            "data": types.DateTime(),
            "valor_ajustado": types.Float()
        }
    )

    logging.info("carga concluída")
