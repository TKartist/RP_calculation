import pandas as pd
import requests
from dotenv import load_dotenv
import os
from pystac_client import Client



load_dotenv()
api_token = os.getenv("API_TOKEN")
if api_token is None:
    raise ValueError("API_TOKEN not found in environment variables.")
auth_headers = {"Authorization": f"Bearer {api_token}"}


def load_isos():
    iso_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/5854516465eb565b689bf8b643d0ed5401e53ccd/docs/model/Montandon_JSON-Example.json"
    iso_response = requests.get(url=iso_url)
    iso_data = iso_response.json()['taxonomies']['ISO_info']

    iso_df = pd.DataFrame(iso_data)
    iso_df.to_csv('iso_codes.csv', index=False)
    return iso_data


def correct_link(link):
    pathway = link.split('/')
    if 'stac' in pathway:
        return link
    else:
        pathway.insert(3, 'stac')
        return '/'.join(pathway)


def __establish_client():
    try:
        client = Client.open("https://montandon-eoapi-stage.ifrc.org/stac", headers=auth_headers)
        print("Connected to STAC API successfully.")
    except Exception as e:
        print(f"Failed to connect to STAC API: {e}")
        return None
    return client


def export_collections(client, collection_id: str, page_size: int = 1000):
    try:
        search = client.search(collections=[collection_id], limit=page_size)
    except Exception as e:
        print(f"Error searching collection {collection_id}: {e}")
        return
    iteration = 1
    try:
        for page in search.pages():
            print("Reading collection:", collection_id, " page:", iteration)
            rows = []
            for item in page.items:
                rows.append(item.to_dict())
            df = pd.DataFrame(rows)
            df.to_csv(f"dataset/{collection_id.replace('-', '_')}_p{iteration}.csv", index=False)
            iteration += 1
        print(f"{collection_id} DONE")
    except Exception as e:
        print(f"Error processing collection {collection_id} page {iteration}: {e}")

        
client = __establish_client()
impacts = list(client.get_collections())


for c in impacts:
    if c.id.endswith('-impacts'):
        if c.id.startswith("desinventar") or c.id.startswith("emdat") or c.id.startswith("gdacs") or c.id.startswith("idmc"):
            print(f"Skipping collection: {c.id}")
            continue
        export_collections(client, c.id)
