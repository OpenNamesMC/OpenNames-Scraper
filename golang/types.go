package main

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// import "go.mongodb.org/mongo-driver/bson"

type NameHistory struct {
	Name        string `json:"name" bson:"name"`
	Changedtoat int64  `json:"changedToAt,omitempty" bson:"changedToAt,omitempty"`
}

type Profile struct {
	Hypixel     interface{}   `json:"hypixel" bson:"hypixel"`
	Lastupdated int64         `json:"lastUpdated" bson:"lastUpdated"`
	Name        string        `json:"name" bson:"name"`
	UUID        string        `json:"uuid" bson:"uuid"`
	NameHistory []NameHistory `json:"name_history" bson:"name_history"`
}

type Response struct {
	Error    interface{} `json:"error"`
	Response []struct {
		Code        int    `json:"code"`
		Name        string `json:"name"`
		UUID        string `json:"uuid"`
		NameHistory []struct {
			Username  string `json:"username"`
			Changedat string `json:"changed_at"` // 2021-06-13T20:49:27.000Z
		} `json:"name_history"`
	} `json:"response"`
}

// func (r *Profile) FromResponse(r Response) {
// 	if r.Error
// }

func ResponseToProfiles(r Response) ([]Profile, error) {
	var p []Profile
	if r.Error != nil {
		fmt.Printf("Non 200 response (%d)", r.Error)
		return p, errors.New("non 200 response")
	}

	for _, profile := range r.Response {
		if profile.Code != 200 {
			continue
		}

		var Hist []NameHistory

		for _, elem := range profile.NameHistory {
			if elem.Changedat != "" {
				t, err := time.Parse("2006-01-02T15:04:05.000Z", elem.Changedat)
				if err != nil {
					fmt.Println(err)
					continue
				}
				Hist = append(Hist, NameHistory{Name: elem.Username, Changedtoat: t.Unix()})
			} else {
				Hist = append(Hist, NameHistory{Name: elem.Username})
			}
		}

		p = append(p, Profile{
			Lastupdated: time.Now().Unix(),
			Name:        profile.Name,
			UUID:        strings.ReplaceAll(profile.UUID, "-", ""),
			NameHistory: reverseNCs(Hist),
		})
	}
	return p, nil
}

func reverseNCs(input []NameHistory) []NameHistory {
	if len(input) == 0 {
		return input
	}
	return append(reverseNCs(input[1:]), input[0])
}

type Config struct {
	Threads   int    `json:"threads"`
	Chunksize int    `json:"chunkSize"`
	Dburl     string `json:"dbUrl"`
	Workerurl string `json:"workerUrl"`
	File      string `json:"file"`
}
