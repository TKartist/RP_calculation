import os
import pandas as pd

files = os.listdir('iteration_one')
files = [f for f in files if f.endswith('.csv')]
s = set()
for file in files:
    file = os.path.join('iteration_one', file)
    df = pd.read_csv(file)
    s.update(df['impact_type'].unique().tolist())

print(s)