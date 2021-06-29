// https://www.mongodb.com/languages/golang

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var config Config

func init() {
	data, _ := ioutil.ReadFile("config.json")
	json.Unmarshal(data, &config)
}

func main() {
	// Connect to MongoDB
	client, err := mongo.NewClient(options.Client().ApplyURI(config.Dburl))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*10*10*time.Hour) // why is this even a thing lmao
	defer cancel()
	// TODO
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)
	fmt.Println("\nSuccessfully connected to MongoDB")
	profiles := client.Database("OpenNames").Collection("profiles")

	fmt.Println("\nGetting UUIDs already in the DB")
	already := GetUUIDsInDB(profiles, ctx)
	fmt.Printf("%d UUIDs are already in the DB\n", len(already))

	fmt.Println("\nGetting UUIDs from file...")
	IDs := GetUUIDs(config.File)
	fmt.Println("\nRemoving duplicates and those already in the DB...")
	IDs = removeDuplicates(IDs)
	IDs = removeAlreadyInDB(IDs, already)
	fmt.Printf("\nGot %d new UUIDs. Dividing them into chunks...\n", len(IDs))
	chunks := DivideIntoChunks(IDs, config.Chunksize)
	fmt.Printf("\nDivided UUIDs into %d chunks of %d\nStarting to fetch data...\n", len(chunks), config.Chunksize)

	for _, chunk := range chunks {
		for {
			if runtime.NumGoroutine() < config.Threads {
				go func() {
					d := GetData(chunk)

					res, err := ResponseToProfiles(d)
					if err != nil {
						fmt.Println(err)
					}
					AddToDB(res, profiles, ctx)
					fmt.Println("Added 50 to db", time.Now().UTC())
				}()
				time.Sleep(time.Millisecond * 50)
				break
			}
			time.Sleep(time.Millisecond * 50)
		}
	}
}

func AddToDB(docs []Profile, profiles *mongo.Collection, ctx context.Context) {
	tmp := []interface{}{}

	for _, doc := range docs {
		tmp = append(tmp, doc)
	}

	_, insertErr := profiles.InsertMany(ctx, tmp)
	if insertErr != nil {
		fmt.Println(insertErr)
	}
}

func GetData(data []string) Response {
	var res Response

	json_data, err := json.Marshal(data)
	if err != nil {
		fmt.Println(err)
		return res
	}

	resp, err := http.Post(config.Workerurl, "application/json", bytes.NewBuffer(json_data))

	if err != nil {
		fmt.Println(err)
		return res
	}

	json.NewDecoder(resp.Body).Decode(&res)
	return res
}

func GetUUIDs(file string) []string {
	// read from file
	data, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("File reading error", err)
		var r []string
		return r
	}
	return strings.Split(string(data), "\n")
}

func GetUUIDsInDB(profiles *mongo.Collection, ctx context.Context) []string {
	var already []string
	cursor, err := profiles.Find(context.TODO(), bson.D{})
	if err != nil {
		log.Fatal(err)
	}
	for cursor.Next(ctx) {
		var result bson.M
		err := cursor.Decode(&result)
		if err != nil {
			fmt.Println(err)
		}
		uuid := result["uuid"].(string)
		already = append(already, uuid)
	}
	return already
}
