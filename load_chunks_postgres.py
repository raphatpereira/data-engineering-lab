import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5432/labdata"
)

chunk_size = 50000

for chunk in pd.read_csv("big_dataset.csv", chunksize=chunk_size):

    chunk["valor_ajustado"] = chunk["valor"] * 1.1

    chunk.to_sql(
        "big_table",
        engine,
        if_exists="append",
        index=False
    )

    print("chunk inserido:", len(chunk))

print("carga finalizada")
