import pandas as pd

# simular dataset grande
n = 200000  # 200k linhas

df = pd.DataFrame({
    "id": range(n),
    "nome": ["user_" + str(i) for i in range(n)],
    "valor": [i * 0.5 for i in range(n)]
})

df.to_csv("big_dataset.csv", index=False)

print("arquivo grande criado")
print("linhas:", len(df))
