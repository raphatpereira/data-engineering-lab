import pandas as pd

chunk_size = 50000

contador = 0

for chunk in pd.read_csv("big_dataset.csv", chunksize=chunk_size):

    contador += len(chunk)

    print("chunk processado:", len(chunk))

    # exemplo de transformação
    chunk["valor_ajustado"] = chunk["valor"] * 1.1

print("total de linhas:", contador)
