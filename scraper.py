import pymongo
import json
import time
import requests
import datetime
import itertools

f = open('uuids.json')
uuids = json.load(f)

my_client = pymongo.MongoClient("mongodb://localhost:27017/OpenNames")
database = my_client["opennames"]
collection = database["profiles"]


def process(uuids):
    past_time = time.time()
    chunks = [uuids[i:i + 25] for i in range(0, len(uuids), 25)]
    for accountChunk in chunks:
        account_details = fetch_account_info(accountChunk)
        if account_details:
            try:
                collection.insert_many(account_details)
            except:
                print("Duplicate found in mongo")
        else:
            print("STOP THE SCRIPT THE API IS DOWN")
            quit()
        # Save the progress into the file
        chunks.remove(accountChunk)
        updated_chunks = list(itertools.chain(*chunks))
        print(f"[{len(updated_chunks)}] Accounts left to check ({time.time() - past_time}ms)")
        past_time = time.time()


def fetch_account_info(data):
    formatted_data = list(set(data[:25]))
    response = requests.post("https://opensourced.danktrain.workers.dev/data", headers={'Content-Type': 'application/json'}, json=formatted_data)
    if response.status_code == 200:
        profiles = list(response.json()["response"])
        print(profiles)
        return format_profiles(profiles)
    else:
        return bool(False)


def format_profiles(profiles):
    new_profiles = []
    for profile in profiles:
        if profile["name"] and profile["uuid"]:
            profile["lastUpdated"] = datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S")
            profile["lowercaseName"] = profile["name"].lower()
            new_profiles.append(profile)
    return new_profiles


print("{:,}".format(len(uuids)) + " Names have been loaded!")

process(uuids)
