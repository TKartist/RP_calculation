import pandas as pd
import os
import ast


def file_list():
    files = [f for f in os.listdir('dataset') if f.endswith('.csv')]
    return files


def get_sources(files):
    sources = set()
    for file in files:
        source = file.split('_')[0]
        sources.add(source)
    return list(sources)

def calc_min(a, b):
    return 1 if int(a / b) == 0 else int(a / b)

def return_period_calculation(df):
    if df is None or df.empty:
        print("Insufficient data for return period calculation.")
        return None
    
    if len(df) == 1:
        return {
            "impact_count" : len(df),
            "timeframe" : 1,
            "1-in-1": df["impact_quantity"].mean(),
            "1-in-2": df["impact_quantity"].mean(),
            "1-in-5": df["impact_quantity"].mean(),
            "1-in-10": df["impact_quantity"].mean(),
            "1-in-20": df["impact_quantity"].mean()
        }
    
    df = df.sort_values(by="impact_quantity", ascending=False).reset_index(drop=True)
    timeframe = df["datetime"].dt.year.max() - df["datetime"].dt.year.min() + 1
    return {
        "impact_count" : len(df),
        "timeframe" : timeframe,
        "1-in-1": int(df["impact_quantity"].mean()),
        "1-in-2": int(df.iloc[:min(calc_min(timeframe, 2), len(df))]["impact_quantity"].mean()),
        "1-in-5": int(df.iloc[:min(calc_min(timeframe, 5), len(df))]["impact_quantity"].mean()),
        "1-in-10": int(df.iloc[:min(calc_min(timeframe, 10), len(df))]["impact_quantity"].mean()),
        "1-in-20": int(df.iloc[:min(calc_min(timeframe, 20), len(df))]["impact_quantity"].mean())
    }


def concat_by_source():
    files = file_list()
    sources = get_sources(files)
    if not files:
        print("No files found in the dataset directory.")
        return None

    for source in sources:
        source_files = [f for f in files if f.startswith(source)]
        df_list = []
        for file in source_files:
            file_path = os.path.join('dataset', file)
            try:
                df = pd.read_csv(file_path)
                df_list.append(df)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue
        if df_list:
            combined_df = pd.concat(df_list, ignore_index=True)
            # combined_df = data_cleaning(combined_df, source)
        else:
            print(f"No valid dataframes to concatenate for source: {source}")
    


def find_keys(combined_df, source):
    print("x")



def data_cleaning(df, source):
    if df is None or df.empty:
        print("No data provided for cleaning.")
        return None

    date_str = (
        df["monty:corr_id"].astype(str)
        .str.extract(r'(\d{8})', expand=False)
    )
    df["datetime"] = pd.to_datetime(date_str, format="%Y%m%d", errors="coerce")

    return_periods = pd.DataFrame(columns=["country", "source", "hazard", "impact_type", "impact_count", "timeframe", "1-in-1", "1-in-2", "1-in-5", "1-in-10", "1-in-20"])

    df["monty:country_code_list"] = df["monty:country_codes"].apply(
        lambda x: (ast.literal_eval(x)[-1] if isinstance(x, str) and x.strip().startswith("[") else
                   (x[-1] if isinstance(x, list) and x else "unknown")) if pd.notnull(x) else "unknown"
    )

    def get_hazard(x):
        parts = str(x).split("-")
        return parts[4] if len(parts) > 4 else "unknown"
    df["hazard_code"] = df["monty:corr_id"].apply(get_hazard)

    def parse_detail(x):
        if pd.isna(x):
            return {}
        if isinstance(x, dict):
            return x
        if isinstance(x, str):
            try:
                return ast.literal_eval(x)
            except Exception:
                return {}
        return {}

    detail = df["monty:impact_detail"].apply(parse_detail)
    df["impact_type"] = detail.apply(lambda d: d.get("type", "unknown"))
    df["impact_quantity"] = pd.to_numeric(detail.apply(lambda d: d.get("value", None)), errors="coerce")

    df["tag"] = df["monty:hazard_codes"].apply(
        lambda x: (ast.literal_eval(x)[-1] if isinstance(x, str) and x.strip().startswith("[") else
                   (x[-1] if isinstance(x, list) and x else "unknown")) if pd.notnull(x) else "unknown"
    )

    dfs = {
        f"{c1}_{c2}_{c3}": g
        for (c1, c2, c3), g in df.groupby(
            ["monty:country_code_list", "hazard_code", "impact_type"],
            dropna=False
        )
    }

    for name, table in dfs.items():
        print(f"Processing group: {source}_{name}")

        table = table.loc[table["datetime"].notna()].copy()
        if table.empty:
            print(f"  Skipping {source}_{name}: no valid datetime after parsing.")
            continue

        table = table[[
            "monty:country_code_list", "datetime", "hazard_code",
            "tag", "impact_type", "impact_quantity", "monty:corr_id"
        ]]


        if table.empty:
            print(f"  Skipping {name}: aggregation produced empty table.")
            continue
        table.to_csv(f"transformed_dataset/{source}_{name}.csv", index=False)
        rp = return_period_calculation(table)
        rp["country"] = name.split('_')[0]
        rp["source"] = source
        rp["hazard"] = name.split('_')[1]
        rp["impact_type"] = " ".join(name.split('_')[2:])
        return_periods = pd.concat([return_periods, pd.DataFrame([rp])], ignore_index=True)
    
    return_periods.to_csv(f"{source}_return_periods.csv", index=False)
        



def period_restriction(partially_cleaned_df):
    partially_cleaned_df["year"] = partially_cleaned_df["datetime"].dt.year
    years = partially_cleaned_df["year"].nunique()


def disaster_concatenation(table):
    sum_col = "impact_quantity"
    agg_dict = {col: "first" for col in table.columns if col != sum_col}
    agg_dict[sum_col] = "sum"

    out = table.groupby("datetime", as_index=False).agg(agg_dict)

    return out




concat_by_source()