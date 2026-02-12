import pandas as pd
import os
import ast

files = [f for f in os.listdir('dataset') if f.endswith('.csv')]
for file in files:
    df = pd.read_csv(f'dataset/{file}')
    sets = df["properties"].apply(lambda x: ast.literal_eval(x)).tolist()
    df1 = pd.DataFrame(sets)
    df1["id"] = df["id"]
    if file.startswith("_"):
        file = "desinventar" + file
    print(f"Saving dataset_clean/{file}")
    df1.to_csv(f'dataset_clean/{file}', index=False)

