import redis
import json
import pandas as pd

# Connect to Redis server
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


# retrieve paper IDs for a given year or all years
def get_paper(year="all"):
    if year == "all":
        return r.smembers("papereids")
    else:
        return r.smembers(f"papereids:{year}")


# Function to retrieve references for a given paper ID
def get_paper_references(eid):
    return r.lrange(f"paper:{eid}:references", 0, -1)


# Function to retrieve affiliations for a given paper ID
def get_paper_affiliations(eid):
    return r.lrange(f"paper:{eid}:affiliations", 0, -1)


# Function to construct DataFrame containing references for papers for a given year or all years
def references_dataframe(year="all"):
    papers = get_paper(year)
    all_references = []
    for paper in papers:
        references = get_paper_references(paper)
        for reference in references:
            reference = json.loads(reference)
            reference["eid"] = paper  # Add paper ID to reference data
            all_references.append(reference)
    return pd.DataFrame(all_references)


# Function to construct DataFrame containing affiliations for papers for a given year or all years
def affiliations_dataframe(year="all"):
    papers = get_paper(year)
    all_affiliations = []
    for paper in papers:
        affiliations = get_paper_affiliations(paper)
        for affiliation in affiliations:
            affiliation = json.loads(affiliation)
            affiliation["eid"] = paper  # Add paper ID to affiliation data
            all_affiliations.append(affiliation)
    return pd.DataFrame(all_affiliations)


# Function to group affiliations by city, counting the number of affiliations per city
def get_city_aff_count(year="all"):
    aff_df = affiliations_dataframe(year)
    grouped_df = (
        aff_df.groupby("city")
        .agg({"eid": "count", "latitude": "first", "longitude": "first"})
        .reset_index()
    )
    grouped_df.columns = ["city", "count", "latitude", "longitude"]
    grouped_df = grouped_df[grouped_df["city"] != ""]  # Remove empty city entries

    return grouped_df


# Function to remove outliers from a given Series using the IQR method
def removeOutlier(serie):
    # Convert Series index to int
    indexes = pd.Series(serie.index).astype(int)

    # Calculate quartiles and IQR
    q1 = indexes.quantile(0.25)
    q3 = indexes.quantile(0.75)
    iqr = q3 - q1

    # lower and upper bounds
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # Filter out outliers based on index
    if serie.index.dtype == "object":
        indexes = indexes.loc[
            (indexes >= lower_bound) & (indexes <= upper_bound)
        ].astype(str)
    else:
        indexes = indexes.loc[
            (indexes >= lower_bound) & (indexes <= upper_bound)
        ].astype(serie.index.dtype)

    # Return only the values that are in the filtered indexes
    return serie.loc[indexes]
