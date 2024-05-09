import pandas as pd
import json
from tqdm import tqdm
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut

geolocator = Nominatim(user_agent="data-sci-project")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

locationMapperPath = "/opt/locationsMapper_scraped.json"
with open(locationMapperPath, 'r') as file:
    locationMapper = json.load(file) 

def findLocation(query, retries=3):
    query = query.lower()
    searchRes = locationMapper.get(query)
    if searchRes == "not found":
        return None, None
    if searchRes is None:
        for _ in range(retries):
            try:
                location = geolocator.geocode(query, timeout=10)  # Increase timeout as needed
                if location is None:
                    locationMapper[query] = "not found"
                    return None, None
                locationMapper[query] = {"latitude": location.latitude, "longitude": location.longitude}
                return location.latitude, location.longitude
            except GeocoderTimedOut:
                print(f"Geocoding request timed out for query: {query}. Retrying...")
                continue
        print(f"Geocoding request failed for query: {query}. Skipping...")
        locationMapper[query] = "not found"
        return None, None
    location = locationMapper.get(query)
    return location.get("latitude"), location.get("longitude")

def clean(file, year):
    new_df = pd.DataFrame(columns=["eid", "references", "affiliations"])
    new_df["eid"] = file["eid"]

    # references gathering
    new_df["references"] = file["ref_docs"].apply(lambda x: eval(x))
    def filter_dicts(lst):
        return [{"title": d.get("title"), "year": d.get("publicationyear")}
                for d in lst if d.get("title") is not None and d.get("publicationyear") is not None]
    new_df["references"] = new_df["references"].apply(filter_dicts)

    # Affiliation gathering
    affiliations_outer = []
    for idx, row in tqdm(file.iterrows()):
        try:
            affiliations_inner = []
            affi_name = row["affilname"].split(";")
            affi_city = row["affiliation_city"].split(";")
            affi_country = row["affiliation_country"].split(";")
            if (len(affi_name) != len(affi_city)) or (len(affi_name) != len(affi_country)) or (len(affi_city) != len(affi_country)):
                affiliations_outer.append(None)
                continue
            for j in range(len(affi_name)):
                if (affi_name[j] == None) or (affi_city[j] == None) or (affi_country[j] == None):
                    continue
                query = affi_city[j] + ", " + affi_country[j]
                lat, long = findLocation(query)
                if ((lat == None) or (long == None)):
                    continue
                affiliations_inner.append({"name": affi_name[j], "city": affi_city[j], "country": affi_country[j], "latitude" : lat, "longitude" : long}) 
            affiliations_outer.append(affiliations_inner)
        except:
            affiliations_outer.append(None)

    
    print("Length of affiliations_outer:", len(affiliations_outer))
    new_df["affiliations"] = affiliations_outer

    new_df.dropna(subset=["references", "affiliations"], inplace=True)   
    print(new_df.isnull().sum())

    new_df.to_json(f"/opt/cleaned_data/datafromscrapping{year}.json", orient="records")

def clean_caller():
    df_2018 = pd.read_csv("/opt/raw_data/raw_scraped_data/df2018.csv")
    df_2018 = df_2018.head(1000)

    df_2019 = pd.read_csv("/opt/raw_data/raw_scraped_data/df2019.csv")
    df_2019 = df_2019.head(1000)

    df_2020 = pd.read_csv("/opt/raw_data/raw_scraped_data/df2020.csv")
    df_2020 = df_2020.head(1000)

    df_2021 = pd.read_csv("/opt/raw_data/raw_scraped_data/df2021.csv")
    df_2021 = df_2021.head(1000)


    df_2022 = pd.read_csv("/opt/raw_data/raw_scraped_data/df2022.csv")
    df_2022 = df_2022.head(1000)
    

    df_2023 = pd.read_csv("/opt/raw_data/raw_scraped_data/df2023.csv")
    df_2023 = df_2023.head(1000)

    clean(df_2018,2018)
    clean(df_2019,2019)
    clean(df_2020,2020)
    clean(df_2021,2021)
    clean(df_2022,2022)
    clean(df_2023,2023)