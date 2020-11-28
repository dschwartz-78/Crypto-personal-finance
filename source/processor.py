import pandas as pd

data = pd.read_csv("../intermediate/test.csv")

df = pd.DataFrame(data)

df[df['Origin funds'] == "CoinBase"]

# Frais totaux
df['Fee amount EUR'].sum()
# Frais par plateforme
df.loc[df['Destination funds'] == 'CoinBase', 'Fee amount EUR'].sum()
# Frais moyens par plateforme
df.loc[df['Destination funds'] == 'CoinBase', 'Fee amount EUR'].mean()
