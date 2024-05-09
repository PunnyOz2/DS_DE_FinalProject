import numpy as np  # linear algebra
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)

import os
import json

from tqdm import tqdm

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

geolocator = Nominatim(user_agent="data-sci-project")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)


def getFilesPath(folderPath):
    filesPath = []
    for dirname, _, filenames in os.walk(folderPath):
        for filename in sorted(filenames):
            filesPath.append(os.path.join(dirname, filename))
    return filesPath


def readJsonFile(filePath):
    with open(filePath, "r") as f:
        data = json.load(f)
    return data


locationMapper = {}
with open("/opt/locationsMapper.json", "r") as f:
    locationMapper = json.load(f)


def findLocation(query):
    query = query.lower()
    searchRes = locationMapper.get(query)
    if searchRes == "not found":
        raise Exception("Location not found for" + query)
    if searchRes == None:
        location = geolocator.geocode(query)
        locationMapper[query] = "not found"
        locationMapper[query] = {
            "latitude": location.latitude,
            "longitude": location.longitude,
        }
    location = locationMapper.get(query)
    return location["latitude"], location["longitude"]


def getSimplifiedData(filesPath, field):
    if field != "REF" and field != "AFF" and field != "ALL":
        raise Exception("argument field must be 'REF' or 'AFF' or 'ALL' only.")
    refCount = refInfoCount = noRefInfoCount = noRefCount = 0
    affCount = affInfoCount = noAffInfoCount = noAffCount = 0
    locationNotFound = 0

    res = []

    for filePath in tqdm(filesPath):
        data = readJsonFile(filePath)
        data = data["abstracts-retrieval-response"]
        # print(data.keys())

        resData = {}
        resData["eid"] = data["coredata"]["eid"]

        if field == "ALL" or field == "REF":
            # get references
            refCount += 1
            try:
                itemData = data["item"]
                itemData = itemData["bibrecord"]
                itemData = itemData["tail"]
                itemData = itemData["bibliography"]
                refcount = itemData["@refcount"]
                refList = itemData["reference"]

                refRes = []
                # handle the case that refList is only single element
                if type(refList) != list:
                    refList = [refList]
                for ref in refList:
                    ref = ref["ref-info"]
                    refData = {}
                    refInfoCount += 1
                    try:
                        refData["year"] = ref["ref-publicationyear"]["@first"]
                        try:
                            refData["title"] = ref["ref-title"]["ref-titletext"]
                        except:
                            refData["title"] = ref["ref-sourcetitle"]
                        refRes.append(refData)
                    except:
                        noRefInfoCount += 1

            except:
                noRefCount += 1

            resData["references"] = refRes

        if field == "ALL" or field == "AFF":
            # get affiliations
            affCount += 1
            try:
                affList = data["affiliation"]
                affRes = []
                # handle single element case
                if type(affList) != list:
                    affList = [affList]
                for aff in affList:
                    affData = {}
                    affInfoCount += 1
                    try:
                        affData["name"] = aff["affilname"]
                        affData["city"] = aff["affiliation-city"]
                        affData["country"] = aff["affiliation-country"]
                        latitude, longitude = findLocation(
                            affData["city"] + ", " + affData["country"]
                        )
                        affData["latitude"] = latitude
                        affData["longitude"] = longitude
                        affRes.append(affData)
                    except:
                        noAffInfoCount += 1
            except:
                noAffCount += 1

            resData["affiliations"] = affRes

        res.append(resData)

    print("Reference error:", noRefCount, "from", refCount)
    print("Reference info error:", noRefInfoCount, "from", refInfoCount)

    print("Affiliation error", noAffCount, "from", affCount)
    print("Affiliation info error:", noAffInfoCount, "from", affInfoCount)

    return res


def createSimplifiedJSON(filesPath, outputFilePath, field):
    simplifiedData = getSimplifiedData(filesPath, field)
    with open(outputFilePath, "w") as f:
        json.dump(simplifiedData, f)


def cleaningProcess(
    basePath="/kaggle/input/scopus-engineering-2018-2023/Project",
    outputPath="/kaggle/working/output",
):
    try:
        os.mkdir(outputPath)
    except:
        pass

    filesPath2018 = getFilesPath(basePath + "/2018")
    print("number of files in 2018:", len(filesPath2018))

    filesPath2019 = getFilesPath(basePath + "/2019")
    print("number of files in 2019:", len(filesPath2019))

    filesPath2020 = getFilesPath(basePath + "/2020")
    print("number of files in 2020:", len(filesPath2020))

    filesPath2021 = getFilesPath(basePath + "/2021")
    print("number of files in 2021:", len(filesPath2021))

    filesPath2022 = getFilesPath(basePath + "/2022")
    print("number of files in 2022:", len(filesPath2022))

    filesPath2023 = getFilesPath(basePath + "/2023")
    print("number of files in 2023:", len(filesPath2023))

    createSimplifiedJSON(
        filesPath2018, outputPath + "/simplified2018.json", field="ALL"
    )
    createSimplifiedJSON(
        filesPath2019, outputPath + "/simplified2019.json", field="ALL"
    )
    createSimplifiedJSON(
        filesPath2020, outputPath + "/simplified2020.json", field="ALL"
    )
    createSimplifiedJSON(
        filesPath2021, outputPath + "/simplified2021.json", field="ALL"
    )
    createSimplifiedJSON(
        filesPath2022, outputPath + "/simplified2022.json", field="ALL"
    )
    createSimplifiedJSON(
        filesPath2023, outputPath + "/simplified2023.json", field="ALL"
    )