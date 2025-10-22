# RP_calculation
Calculating impact of disasters based on return periods

# Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Methodology](#methodology)

## Overview
This pipeline ingests disaster impact data from the Montandon API, the world’s largest disaster database. 
It then estimates the quantified impact across different impact types, using factors such as the event’s return period, disaster type, and country of origin.

## Architecture
### Data Collector
1. Call 'collections' endpoint to collect all the data sources available on the Montandon API. (datacollector.py)
2. Identify all the 'impact' collection tables and access them individually to download the disaster impact data.
3. Clean the links given. (account for potentially invalid links provided by the endpoint) 
3. When downloading, limit the batch to maximum 200 entries per call to avoid exceeding the server RAM capacity.
4. To avoid slowing down of device, the rows per file is capped at 3000.

### Return Period Calculation
1. Group CSV files based on source. Source is identifiable through the filename. (naming convention: {SOURCE}_{INDEX}.csv)
2. Clean the date field and remove all fields except 'country', 'date', 'disaster_type', 'impact_type', 'impact_quantity', 'data_source'
3. Group the impacts by country, disaster, and type of impact.
4. Merge some rows by mapping synonymous impact_type categorizations.
5. Calculate the impact_quantity of impact_type for particular disaster type in certain country based on return period.
6. Merge all transformation into one singular table.

## Return Period Calculation Methodology
