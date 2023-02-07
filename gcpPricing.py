import requests
import json
import csv
import pandas as pd
import os
from pathlib import Path
import time
from datetime import datetime, timedelta
import random

def main():
    startTime = datetime.today()
    print(f"Started at: {startTime}")

    rootDirectory = str(Path(__file__).parent)
    servicesDirectory = f"{rootDirectory}/services/"
    jsonDirectory = f"{rootDirectory}/json/"
    pricingDirectory = f"{rootDirectory}/pricing/"

    token = getToken()
    servicesList = callServicesURL(token, servicesDirectory)
    
    #servicesList = readServicesFile()

    total_services = len(servicesList)
    service_count = 0
    merged_df = pd.DataFrame()
    
    for i in servicesList:
        service_count += 1
        print(f"({service_count}/{total_services}) {i['displayName']}  sku: {i['serviceId']}")
        downloadSKUFile(token, i['serviceId'], i['displayName'], jsonDirectory)
        sku_df = processSKUFile(i['serviceId'], i['displayName'], jsonDirectory, pricingDirectory)
        df_list = [merged_df, sku_df]
        merged_df = pd.concat(df_list)
        time.sleep(random.randint(12,15))

    writeMergedSKUFile(merged_df, rootDirectory)

    print(f"Number of services: {service_count}")
    print(f"Number of SKUs: {len(merged_df.index)}")
    
    stopTime = datetime.today()

    print(f"Finished at: {stopTime}")
    print(f"Processing time: {stopTime - startTime}")

def getToken():
    filelocation = str(Path(__file__).parent)
    fileinputname = "token.txt"
    fileinputpath = filelocation + "/" + fileinputname

    f = open(fileinputpath)
    data = f.read()
    f.close()

    return data


def callServicesURL(token, servicesDirectory):
    # Call the GCP Cloud Billing API
    # https://cloud.google.com/billing/v1/how-tos/catalog-api
    request_url = f"https://cloudbilling.googleapis.com/v1/services?key={token}"

    response = requests.get(request_url)

    # Create an array to store services list
    servicesList= []

    # Add the services returned in the API response to a list
    for i in response.json()['services']:
        servicesList.append(i)

    # Retrieve services from all available pages until there is a 'NextPageLink' available to retrieve prices
    while response.json()["nextPageToken"] != "":
        print(f"nextPageToken: {len(response.json()['nextPageToken'])}")
        for i in response.json()['services']:
            servicesList.append(i)
        response = requests.get(response.json()["nextPageToken"])
        #time.sleep(random.randint(2,7))

    # Retrieve price list from the last page when there is no "NextPageLink" available to retrieve prices
    if response.json()["nextPageToken"] == "":
        for i in response.json()['services']:
            servicesList.append(i)

    saveServicesFile(servicesList, servicesDirectory)
    return servicesList


def saveServicesFile(services, servicesDirectory):
    # Set the file location and filename to save files
    filelocation = servicesDirectory
    filename = "gcpServices"

    with open(os.path.join(filelocation,filename) + '.json', 'w') as f:
        json.dump(services, f)

    #Read the JSON into a dataframe
    df = pd.read_json(json.dumps(services))

    # Save the data frame as a CSV file
    df.to_csv(os.path.join(filelocation,filename) + '.csv', index=False)



def downloadSKUFile(token, service_id, display_name, jsonDirectory):
    # Call the GCP Cloud Billing API
    # https://cloud.google.com/billing/v1/how-tos/catalog-api
    request_url = f"https://cloudbilling.googleapis.com/v1/services/{service_id}/skus?key={token}"
    response = requests.get(request_url)

    fileoutputpath = f"{jsonDirectory}gcpPricing-{service_id}.json"

    with open(fileoutputpath, "w") as f:
        f.write(response.text)

    return fileoutputpath


def processSKUFile(service_id, display_name, inputLocation, outputLocation):
    fileinputpath = f"{inputLocation}gcpPricing-{service_id}.json"
    fileoutputlocation = outputLocation
    fileoutputname = f"gcpPricing-{service_id}"

    # Create arrays to store products and terms
    skuitems = []

    f = open(fileinputpath)
    data = json.load(f)
    skus = data['skus']

    number_of_skus = 0

    for i in skus:
        number_of_skus += 1
        data = i
        keys = get_simple_keys(data)
        ## Avoid getting duplicates - https://stackoverflow.com/questions/23724136/appending-a-dictionary-to-a-list-in-a-loop
        data = keys.copy()
        data['serviceId'] = service_id
        data['displayName'] = display_name
        skuitems.append(data)

    f.close()

    #Read the JSON into a dataframe
    #df = pd.read_json(json.dumps(skuitems))
    df = pd.json_normalize(skuitems)

    #shorten the header names
    for col in df.columns:
        colName = col
        dotPosition = colName.rfind(".")
        if dotPosition > -1:
            newName = colName[dotPosition+1:]
            df.rename({colName: newName}, axis=1, inplace=True)

    # Save the data frame as a CSV file
    df.to_csv(os.path.join(fileoutputlocation,fileoutputname) + '.csv', index=False)

    return df

def get_simple_keys(data, result={}):
    #approach from https://stackoverflow.com/questions/34327719/get-keys-from-json-in-python#34327880
    for key in data.keys():
        if type(data[key]) not in [dict, list]:
            result[key] = data[key]
        elif type(data[key]) == list:
            if key in ("tieredRates","serviceRegions","regions"):
                result[key] = data[key]
            else:
                for i in data[key]:
                    if type(i) not in [dict]:
                        continue
                    else:
                        get_simple_keys(i)
        else:
            get_simple_keys(data[key])
    return result

def readServicesFile():
    # Set the file location and filename to read files
    filelocation = str(Path(__file__).parent) + "/services/"
    filename = "gcpServices"
    filepath = filelocation + filename + '.json'

    with open(filepath, 'r') as f:
        data = json.load(f)
    return data

def writeMergedSKUFile(dataframe, fileoutputlocation):
    # Set the file location and filename to read files
    fileoutputname = f"gcpPricingMerged"
    dataframe.to_csv(os.path.join(fileoutputlocation,fileoutputname) + '.csv', index=False)

if __name__ == "__main__":
    main()
