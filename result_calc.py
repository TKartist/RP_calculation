import pandas as pd
import os

files = os.listdir('iteration_two')
files = [f for f in files if f.endswith('.csv')]

dfs = []
for file in files:
    file = os.path.join('iteration_two', file)
    df = pd.read_csv(file)
    dfs.append(df)

concat_df = pd.concat(dfs, ignore_index=True)
hazards = concat_df['hazard'].unique().tolist()
print(hazards)

impact_types = concat_df['impact_type'].unique().tolist()
print(impact_types)