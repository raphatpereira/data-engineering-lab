import requests
import pandas as pd

# fonte pública de teste
url = "https://jsonplaceholder.typicode.com/users"

response = requests.get(url)

dados = response.json()

df = pd.DataFrame(dados)

# selecionar colunas úteis
df = df[["id", "name", "email", "username"]]

# criar coluna derivada
df["domain"] = df["email"].apply(lambda x: x.split("@")[1])

# salvar arquivo
df.to_csv("usuarios.csv", index=False)

print("arquivo criado: usuarios.csv")
print(df.head())
