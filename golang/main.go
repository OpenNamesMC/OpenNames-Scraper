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
	profiles := client.Database("OpenNames").Collection("profiles")

	chunks := GetChunks(config.File)
	fmt.Printf("Divided UUIDs into %d chunks of %d\n", len(chunks), config.Chunksize)

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
				break
			}
			time.Sleep(time.Millisecond * 5)
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

func GetChunks(file string) [][]string {

	// read from file
	data, err := ioutil.ReadFile(file)
	if err != nil {
		fmt.Println("File reading error", err)
		var r [][]string
		return r
	}

	IDs := strings.Split(string(data), "\n")

	// turn into chunks of 50
	return DivideIntoChunks(IDs, config.Chunksize)
}

// https://stackoverflow.com/a/67011816
func DivideIntoChunks(xs []string, chunkSize int) [][]string {
	if len(xs) == 0 {
		return nil
	}
	divided := make([][]string, (len(xs)+chunkSize-1)/chunkSize)
	prev := 0
	i := 0
	till := len(xs) - chunkSize
	for prev < till {
		next := prev + chunkSize
		divided[i] = xs[prev:next]
		prev = next
		i++
	}
	divided[i] = xs[prev:]
	return divided
}
