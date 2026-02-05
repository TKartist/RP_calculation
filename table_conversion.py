import os
import pandas as pd


datasets = ["desinventar", "emdat", "gdacs", "idmc", "gfd", "pdc"]
mapping_type = {
    "affected direct people": "Affected",
    "affected indirect people": "Affected Indirect",
    "evacuated people": "Evacuated",
    "relocated people": "Displaced",
    "cost usd uncertain": "Cost USD",
    "cost local currency": "Cost Local Currency",
    "affected total people": "Affected",
    "death people" : "Deaths",
    "displaced internal people": "Displaced",
}

exchange_rates = pd.read_csv("exchange_rate.csv")
exchange_dict = dict(zip(exchange_rates["iso3"], exchange_rates["usd_to_local"]))

isos = pd.read_csv("iso_codes.csv")
iso_dict = dict(zip(isos["ISO3"], isos["country"]))




def currency_conversion(x):
    x["mapped_impact_type"] = "Cost USD"
    x["1-in-1"] = x["1-in-1"] / exchange_dict.get(x["country"], 1)  # Use exchange rate if available, otherwise default to 1
    x["1-in-5"] = x["1-in-5"] / exchange_dict.get(x["country"], 1)  # Use exchange rate if available, otherwise default to 1
    x["1-in-10"] = x["1-in-10"] / exchange_dict.get(x["country"], 1)  # Use exchange rate if available, otherwise default to 1
    x["1-in-20"] = x["1-in-20"] / exchange_dict.get(x["country"], 1)  # Use exchange rate if available, otherwise default to 1
    x["1-in-2"] = x["1-in-2"] / exchange_dict.get(x["country"], 1)  # Use exchange rate if available, otherwise default to 1
    return x

def clean_rp():
    for dataset in datasets:
        df = pd.read_csv(f'{dataset}_return_periods.csv')
        df = df[df["timeframe"] > 4]
        print(f"{dataset}: {df["hazard"].nunique()} hazards with valid RP calculations.")
        print(f"{dataset}: {df['hazard'].unique().tolist()}")
        print("\n-------------------------------------\n")
        print(f"{dataset}: {df['impact_type'].nunique()} impact types with valid RP calculations.")
        print(f"{dataset}: {df['impact_type'].unique().tolist()}")
        print("\n-------------------------------------\n")
        df["mapped_impact_type"] = df["impact_type"].apply(lambda x: mapping_type.get(x, "Other"))
        df = df[df["mapped_impact_type"] != "Other"]
        df = df.apply(lambda row: currency_conversion(row) if row["mapped_impact_type"] == "Cost Local Currency" else row, axis=1)
        df = df[["country", "hazard", "source", "mapped_impact_type", "impact_count", "timeframe", "1-in-5"]]
        df.to_csv(f'{dataset}_return_periods_cleaned.csv', index=False)

def eap_rp_top20():
    df = pd.DataFrame()
    for dataset in datasets:
        df = pd.concat([df, pd.read_csv(f'{dataset}_return_periods_cleaned.csv')], ignore_index=True)
    
    df = df[df["mapped_impact_type"].isin(["Affected", "Deaths", "Displaced", "Cost USD"])]
    df["country"] = df["country"].apply(lambda x: iso_dict.get(x, x))
    df_affected = df[df["mapped_impact_type"] == "Affected"]
    df_affected_top20 = (
        df_affected
        .sort_values(["hazard", "1-in-5"], ascending=[True, False])
        .assign(rank=lambda d: d.groupby("hazard").cumcount() + 1)
        .query("rank <= 20")
    )
    df_affected_top20.to_csv("eap_rp_affected_top20.csv", index=False)
    df_deaths = df[df["mapped_impact_type"] == "Deaths"]
    df_deaths_top20 = (
        df_deaths
        .sort_values(["hazard", "1-in-5"], ascending=[True, False])
        .assign(rank=lambda d: d.groupby("hazard").cumcount() + 1)
        .query("rank <= 20")
    )
    df_deaths_top20.to_csv("eap_rp_deaths_top20.csv", index=False)
    df_displaced = df[df["mapped_impact_type"] == "Displaced"]

    df_displaced_top20 = (
        df_displaced
        .sort_values(["hazard", "1-in-5"], ascending=[True, False])
        .assign(rank=lambda d: d.groupby("hazard").cumcount() + 1)
        .query("rank <= 20")
    )
    df_displaced_top20.to_csv("eap_rp_displaced_top20.csv", index=False)
    df_cost = df[df["mapped_impact_type"] == "Cost USD"]
    df_cost_top20 = (
        df_cost
        .sort_values(["hazard", "1-in-5"], ascending=[True, False])
        .assign(rank=lambda d: d.groupby("hazard").cumcount() + 1)
        .query("rank <= 20")
    )
    df_cost_top20.to_csv("eap_rp_cost_top20.csv", index=False)

eap_rp_top20()



