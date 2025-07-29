import requests
import pandas as pd
import io

base_url = "https://data.delaware.gov/resource/5zy2-grhr.csv"
limit = 1000
offset = 0
all_data = []

while True:
    params = {"$limit": limit, "$offset": offset}
    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        print(f"Failed at offset {offset}, status code: {response.status_code}")
        break
    df = pd.read_csv(io.StringIO(response.text))
    if df.empty:
        break
    all_data.append(df)
    print(f"Downloaded {len(df)} records (offset {offset})")
    offset += limit

if all_data:
    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv("delaware_business_licenses_full.csv", index=False)
    print(f"Saved {len(final_df)} records to delaware_business_licenses_full.csv")
else:
    print("No data downloaded.") 