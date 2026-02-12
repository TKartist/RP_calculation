import pandas as pd
import gc
from dotenv import load_dotenv
import os
from pystac_client import Client
from datetime import datetime, timezone


# Reading the API token from environment variables
load_dotenv()
api_token = os.getenv("API_TOKEN")
if api_token is None:
    raise ValueError("API_TOKEN not found in environment variables.")
auth_headers = {"Authorization": f"Bearer {api_token}"}


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


collection_id = []
c = __establish_client()

# Getting all collections which contain "events" information
if c is not None:
    collections = c.get_collections()
    for collection in collections:
        if collection.id.endswith("events"):
            collection_id.append(collection.id)

# Let's fit some filters for the search query. We want 5 years of data upto 1st of Jan 2026
start_dt = datetime(2021, 1, 1, tzinfo=timezone.utc)
end_dt   = datetime(2026, 1, 1, tzinfo=timezone.utc)
datetime_range = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"

# Now let's get the events by event source and save them as csv files
for collection in collection_id:
    try:
        search = c.search(collections=[collection], datetime=datetime_range, limit=1000)
    except Exception as e:
        print(f"Error searching collection {collection}: {e}")
        continue
    iteration = 1
    try:
        for page in search.pages():
            print("Reading collection:", collection_id, " page:", iteration)
            rows = []
            for item in page.items:
                rows.append(item.to_dict())
            df = pd.DataFrame(rows)
            df.to_csv(f"events/{collection.replace('-', '_')}_p{iteration}.csv", index=False)
            iteration += 1
    except Exception as e:
        print(f"Error processing collection {collection}: {e}")
        continue