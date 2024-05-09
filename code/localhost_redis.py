import redis
import json
import pandas as pd
import time
from geopy.geocoders import Nominatim

r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def get_paper(year="all"):
    if year == "all":
        return r.smembers("papereids")
    else:
        return r.smembers(f"papereids:{year}")


def get_paper_references(eid):
    return r.lrange(f"paper:{eid}:references", 0, -1)


def get_paper_affiliations(eid):
    return r.lrange(f"paper:{eid}:affiliations", 0, -1)


def references_dataframe(year="all"):
    papers = get_paper(year)
    all_references = []
    for paper in papers:
        references = get_paper_references(paper)
        for reference in references:
            reference = json.loads(reference)
            reference["eid"] = paper
            all_references.append(reference)
    return pd.DataFrame(all_references)


def affiliations_dataframe(year="all"):
    papers = get_paper(year)
    all_affiliations = []
    for paper in papers:
        affiliations = get_paper_affiliations(paper)
        for affiliation in affiliations:
            affiliation = json.loads(affiliation)
            affiliation["eid"] = paper
            all_affiliations.append(affiliation)
    return pd.DataFrame(all_affiliations)


def get_country_aff_count(year="all"):
    aff_df = affiliations_dataframe(year)
    country_counts = aff_df.groupby(["country"]).size().iloc[1:]
    country_counts_df = country_counts.to_frame().reset_index()
    country_counts_df.columns = ["country", "count"]

    country_counts_df = country_counts_df.iloc[:].sort_values(
        by="count", ascending=False
    )

    geolocator = Nominatim(user_agent="my-geoapi-application")

    for index, row in country_counts_df.iterrows():
        # some country name may not be recognized by geopy
        try:
            country = row["country"]
            location = geolocator.geocode(country)
            if location:
                country_counts_df.at[index, "latitude"] = location.latitude
                country_counts_df.at[index, "longitude"] = location.longitude
            else:
                country_counts_df.at[index, "latitude"] = None
                country_counts_df.at[index, "longitude"] = None
        except:
            country_counts_df.at[index, "latitude"] = None
            country_counts_df.at[index, "longitude"] = None
            continue

    return country_counts_df.dropna()
