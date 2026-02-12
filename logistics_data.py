import os
import pandas as pd
import ast
from disaster_mapping import hazard_codes
import numpy as np

def target_events():
    files = [f for f in os.listdir('events') if f.endswith('.csv')]

    df_all = pd.DataFrame()
    for file in files:
        df = pd.read_csv(f'events/{file}')
        df_all = pd.concat([df_all, df], ignore_index=True)

    df_props = df_all["properties"].apply(lambda x: ast.literal_eval(x)).tolist()
    df_props = pd.DataFrame(df_props)
    df_props["id"] = df_all["id"]
    df_props["collection"] = df_all["collection"]
    df_drc = df_props[df_props["monty:country_codes"].apply(lambda x: isinstance(x, list) and len(x) > 0 and ("COG" in x or "COD" in x or "RWA" in x or "BDI" in x))]
    df_moz = df_props[df_props["monty:country_codes"].apply(lambda x: isinstance(x, list) and len(x) > 0 and ("MOZ" in x or "AGO" in x))]
    df_drc.to_csv("logistics_events/drc_events.csv", index=False)
    df_moz.to_csv("logistics_events/moz_events.csv", index=False)

df_drc = pd.read_csv("logistics_events/drc_events_with_hazards.csv")
print(f"Length of DRC events: {len(df_drc)}")
print(df_drc["collection"].value_counts())
print("=====================================")
df_moz = pd.read_csv("logistics_events/moz_events_with_hazards.csv")
print(f"Length of Mozambique events: {len(df_moz)}")
print(df_moz["collection"].value_counts())

def uniq_codes(series):
    return sorted({code for lst in series.dropna() for code in lst})

def clean_events(df):
    for c in ["datetime", "start_datetime"]:
        df[c] = df[c].replace(["", " ", "NA", "N/A", "null", "None"], np.nan)
    df["monty:country_codes"] = df["monty:country_codes"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else [])
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    df["start_datetime"] = pd.to_datetime(df["start_datetime"], errors="coerce", utc=True)
    df["end_datetime"] = pd.to_datetime(df["end_datetime"], errors="coerce", utc=True)

    df["datetime"] = df["datetime"].fillna(df["start_datetime"].fillna(df["end_datetime"]))

    df = df.dropna(subset=["datetime"])

    hazard_dfs = []
    for hazard, sub_df in df.groupby("hazard", dropna=False):
        sub_df = sub_df.sort_values("datetime").copy()
        sub_df["delta_sd"] = sub_df["datetime"].diff().dt.days
        sub_df["delta_sd"] = sub_df["delta_sd"].fillna(0)
        sub_df["event_group"] = (sub_df["delta_sd"] > 6).cumsum()
        hazard_dfs.append(sub_df)

    out = pd.concat(hazard_dfs, ignore_index=True)
    out.to_csv("logistics_events/cleaned_moz_events.csv", index=False)
    return out

out = clean_events(df_moz)
out = (
    out.groupby(["hazard", "event_group"], as_index=False)
      .agg(
          start_datetime=("datetime", "min"),
          end_datetime=("end_datetime", "max"),
          country_codes=("monty:country_codes", uniq_codes),
          id_list=("id", lambda x: list(x)),
          sub_event_count=("id", "count"),
      )
)
out.to_csv("logistics_events/cleaned_moz_events_grouped.csv", index=False)

        



# def extract_hazard(x):
#     cur = 0
#     index = -1
#     for i in range(len(x)):
#         if len(x[i]) == 6:
#             return hazard_codes.get(x[i], "unknown")
#         else:
#             if len(x[i]) > cur:
#                 cur = len(x[i])
#                 index = i
#     return hazard_codes.get(x[index], "unknown") if index > -1 else "unknown"

# cols = ["id", "hazard", "datetime", "title", "description", "start_datetime", "end_datetime", "monty:country_codes", "collection"]


# df_drc["monty:hazard_codes"] = df_drc["monty:hazard_codes"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else [])
# df_drc["hazard"] = df_drc["monty:hazard_codes"].apply(lambda codes: extract_hazard(codes) if isinstance(codes, list) and codes else "unknown")
# df_drc = df_drc[cols]
# df_drc.to_csv("logistics_events/drc_events_with_hazards.csv", index=False)
# df_moz["monty:hazard_codes"] = df_moz["monty:hazard_codes"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("[") else [])
# df_moz["hazard"] = df_moz["monty:hazard_codes"].apply(lambda codes: extract_hazard(codes) if isinstance(codes, list) and codes else "unknown")
# df_moz = df_moz[cols]
# df_moz.to_csv("logistics_events/moz_events_with_hazards.csv", index=False)