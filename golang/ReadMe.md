# Scraper (golang)

This is the GoLang rewrite of the python scraper. Its meant to put new names/uuids into the MongoDB database.

Its also planned that it can update existing profiles every few days.

## Config

* Threads
How many threads the script will use

* chunkSize
How big each chunk is that is being send to the worker

* dbUrl
The URL of your MongoDB database

* workerUrl
The URL of your Cloudflare worker

* file 
The path to a file with UUIDs/names (seperated with a newline)