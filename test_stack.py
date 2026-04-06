import pandas as pd

dados = {
    "nome": ["ana", "bruno", "carla"],
    "idade": [23, 35, 29]
}

df = pd.DataFrame(dados)

print(df)
print(df.describe())
