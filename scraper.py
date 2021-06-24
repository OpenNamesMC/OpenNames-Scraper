import pymongo
import json
import time
import requests
import datetime
import itertools
import threading
import sys
from typing import List

configFile = open("config.json")
configData = json.load(configFile)
configFile.close()

uuidsFile = open("uuids.json")
uuids = json.load(uuidsFile)
uuidsFile.close()

my_client = pymongo.MongoClient(configData["database"]["url"])
database = my_client[configData["database"]["databaseName"]]
collection = database[configData["database"]["databaseCollection"]]

live_chunks = [uuids[i:i + 50] for i in range(0, len(uuids), 50)]


def process():
    print("{:,}".format(len(uuids)) + " Names have been loaded!")
    chunks = [uuids[i:i + 50] for i in range(0, len(uuids), 50)]

    threading.Thread(target=save_uuids).start()

    for chunk in chunks:
        while len(live_chunks) > 0:
            time.sleep(0.01)
            if threading.active_count() < configData["threadCount"]:
                threading.Thread(target=fetch_chunks_thread, args=[chunk]).start()

                live_chunks.remove(chunk)
                live_chunks_len = len(list(itertools.chain(*live_chunks)))
                print("[" + "{:,}".format(live_chunks_len) + "] Accounts left to check")
                break
        else:
            print(f"All {len(list(itertools.chain(*chunks)))} UUIDS has been loaded into the database")


def save_uuids():
    while 1:
        time.sleep(10)
        with open("uuids.json", "w+") as f:
            data = json.dumps(list(itertools.chain(*live_chunks)))
            f.write(data)


def fetch_chunks_thread(chunk: List):
    account_details = fetch_account_info(chunk)
    if account_details:
        try:
            collection.insert_many(account_details)
        except pymongo.errors.DuplicateKeyError:
            print("Duplicate found in Mongo")
    else:
        print("STOP THE SCRIPT THE API IS DOWN")
        sys.exit()


def fetch_account_info(data):
    formatted_data = list(set(data[:50]))
    headers = {'Content-Type': 'application/json'}
    response = requests.post("https://opennames.opennames.workers.dev/aschonn_data", headers=headers, json=formatted_data)
    if response.status_code == 200:
        profiles = list(response.json()["response"])
        return format_profiles(profiles)
    else:
        return bool(False)


def format_profiles(profiles):
    new_profiles = []
    for profile in profiles:
        if profile["name"] and profile["uuid"]:
            new_profiles.append({
                "name": profile["name"],
                "uuid": profile["uuid"],
                "name_history": profile["name_history"],
                "lastUpdated": datetime.datetime.now().strftime("%Y-%d-%m %H:%M:%S"),
                "lowercaseName": profile["name"].lower(),
            })
    if len(new_profiles) == 0:
        print(profiles)
    return new_profiles


process()
