import os
import pandas as pd


countries = pd.read_csv("iso_codes.csv", usecols=["ISO3", "country"])


def read_folder(folder_name):
    filenames = os.listdir(folder_name)
    df = pd.DataFrame()

    for filename in filenames:
        temp_df = pd.read_csv(os.path.join(folder_name, filename))
        temp_df = temp_df[["country", "source", "hazard", "impact_type", "impact_count", "1-in-5"]]
        temp_df["country"] = temp_df["country"].map(countries.set_index("ISO3")["country"])
        df = pd.concat([df, temp_df], ignore_index=True)
    
    return df


def process_data(df):
    dfs = {val : df[df["impact_type"] == val] for val in df["impact_type"].unique()}
    for key, val in dfs.items():
        df_top20 = (
            val.sort_values(["hazard", "1-in-5"], ascending=[True, False])
            .groupby("hazard")
            .head(20)
        )
        df_top20.to_csv(f"eap_data/top_20_{key}_impacts.csv", index=False)
    
    print("Top 20 impacts organized")

imps = process_data(read_folder("iteration_one"))