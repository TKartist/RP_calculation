import pandas as pd
import requests
import ast


def collect_ifrc_appeals():
    url = "https://goadmin.ifrc.org/api/v2/appeal/"
    dfs = []
    while url:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data["results"])
            dfs.append(df)
            url = data.get("next")
        else:
            print(f"Failed to fetch data: {response.status_code}")
            break

    if dfs:
        appeals_df = pd.concat(dfs, ignore_index=True)
        appeals_df.to_csv("ifrc_appeals.csv", index=False)

def fit_datastruct():
    df = pd.read_csv("ifrc_appeals.csv")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce", utc=True)
    df = df[df["start_date"] > pd.Timestamp("2021-01-01", tz="UTC")]
    df["country"] = df["country"].apply(lambda x: ast.literal_eval(x)["iso3"] if isinstance(x, str) and x.strip().startswith("{") else {})
    df = df[df["country"].isin(["COD", "COG", "RWA", "BDI", "MOZ", "AGO"])]
    df.to_csv("logistics_events/ifrc_appeals_filtered.csv", index=False)
    out_df = pd.DataFrame()
    df["dtype"] = df["dtype"].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) and x.strip().startswith("{") else {})
    out_df["hazard"] = df["dtype"].apply(lambda x: x.get("name", None) if isinstance(x, dict) else None)
    out_df = out_df[out_df["hazard"] != "Population Movement"]
    out_df["event_group"] = None
    out_df["start_datetime"] = df["start_date"]
    out_df["end_datetime"] = pd.to_datetime(df["end_date"], errors="coerce", utc=True)
    out_df["country_codes"] = df["country"]
    out_df["id_list"] = df["code"].apply(lambda x: [x] if pd.notnull(x) else [])
    out_df["sub_event_count"] = 1
    drc_df = out_df[out_df["country_codes"].isin(["COD", "COG", "RWA", "BDI"])]
    drc_df["country_codes"] = drc_df["country_codes"].apply(lambda x: [x])
    moz_df = out_df[out_df["country_codes"].isin(["MOZ", "AGO"])]
    moz_df["country_codes"] = moz_df["country_codes"].apply(lambda x: [x])
    drc_df.to_csv("logistics_events/drc_appeals_cleaned.csv", index=False)
    moz_df.to_csv("logistics_events/moz_appeals_cleaned.csv", index=False)

fit_datastruct()