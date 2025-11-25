import pandas as pd
import requests
import gc

def load_isos():
    iso_url = "https://raw.githubusercontent.com/IFRCGo/monty-stac-extension/5854516465eb565b689bf8b643d0ed5401e53ccd/docs/model/Montandon_JSON-Example.json"
    iso_response = requests.get(url=iso_url)
    iso_data = iso_response.json()['taxonomies']['ISO_info']

    iso_df = pd.DataFrame(iso_data)
    iso_df.to_csv('iso_codes.csv', index=False)
    return iso_data

# iso_data = load_isos()

# for source_event in collection_list:
#     start_date = ".."
#     end_date = "2026-01-01T00:00:00Z"
#     datetime_range = f"{start_date}/{end_date}"

#     url = (
#         f"https://montandon-eoapi-stage.ifrc.org/stac/collections/"
#         f"{source_event}/items?limit=200&datetime={datetime_range}"
#     )

#     while url:
#         response = requests.get(url)
#         if response.status_code != 200:
#             print(f"Failed to fetch data for {source_event}: {response.status_code}")
#             break

#         data = response.json()
#         for item in data.get('features', []):
#             codes = item['properties'].get('monty:country_codes', [])
#             country_list.extend(codes)

#         next_link = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)
#         url = next_link


# country_event_counts = dict(Counter(country_list))

# df = pd.DataFrame([
#     {"iso3": iso, "country": iso3_to_country.get(iso, iso), "event_count": count}
#     for iso, count in country_event_counts.items()
# ])

# fig = px.choropleth(
#     df,
#     locations="iso3",              # ISO3 codes
#     color="event_count",           # Color by event count
#     hover_name="country",          # Hover label
#     color_continuous_scale="OrRd",
#     title="Disaster Event Counts by Country (2020–2025)",
#     labels={"event_count": "Event Count"},
# )

# fig.update_geos(showcountries=True, showcoastlines=True, showland=True, fitbounds="locations")
# fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})

# fig.write_html("disaster_event_map.html")


def correct_link(link):
    pathway = link.split('/')
    if 'stac' in pathway:
        return link
    else:
        pathway.insert(3, 'stac')
        return '/'.join(pathway)


def get_collections():
    collections_list = []

    url = f"https://montandon-eoapi-stage.ifrc.org/stac/collections"
    while url:
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch collections: {response.status_code}")
            break
        else:
            data = response.json()
            collections_list.extend(data.get('collections', []))

            next_link = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)
            print(next_link)
            url = correct_link(next_link) if next_link else None
    
    collections_df = pd.DataFrame(collections_list)
    collections_df.to_csv('stac_collections.csv', index=False)
    return collections_df


def get_items(collection_list):
    if collection_list is None or len(collection_list) == 0:
        print("No collections provided.")
        return None
    
    collection = []
    events = []

    for source_event in collection_list:
        if "impacts" not in source_event or "desinventar" in source_event:
            continue
        start_date = ".."
        end_date = "2026-01-01T00:00:00Z"
        datetime_range = f"{start_date}/{end_date}"

        url = (
            f"https://montandon-eoapi-stage.ifrc.org/stac/collections/"
            f"{source_event}/items?limit=100&datetime={datetime_range}"
        )
        counter = 0
        while url:
            response = requests.get(url)
            if response.status_code != 200:
                print(f"Failed to fetch data for {source_event}: {response.status_code}")
                print(response.text)
                print(f"Link : {url}")
                break

            data = response.json()
            for item in data.get('features', []):
                collection.append(item.get("collection", "unknown"))
                events.append(item.get("properties", {}))

            if len(events) > 3000:
                events_df = pd.DataFrame(events)
                events_df['collection'] = collection
                events_df.to_csv(f"dataset/{source_event}_{counter}.csv", index=False)
                counter += 1
                gc.collect()
                events = []
                collection = []
            
            next_link = next((l.get("href") for l in data.get("links", []) if l.get("rel") == "next"), None)
            print(next_link)
            url = correct_link(next_link) if next_link else None
    
        events_df = pd.DataFrame(events)
        events_df['collection'] = collection
        events = []
        collection = []
        gc.collect()
        events_df.to_csv(f"dataset/{source_event}_{counter}.csv", index=False)

    return events_df
        
# collections = get_collections()
# items = get_items(collections['id'].tolist())
load_isos()