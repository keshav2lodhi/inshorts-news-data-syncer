package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	// "time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"inshorts.com/inshorts-news-data-syncer/utils"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
)

type Article struct {
	ID              string   `json:"id"`
	Title           string   `json:"title"`
	Description     string   `json:"description"`
	URL             string   `json:"url"`
	PublicationDate string   `json:"publication_date"`
	SourceName      string   `json:"source_name"`
	Category        []string `json:"category"`
	RelevanceScore  float64  `json:"relevance_score"`
	Latitude        float64  `json:"latitude"`
	Longitude       float64  `json:"longitude"`
	LLMSummary      string   `json:"llm_summary,omitempty"`
}

func main() {
	// Set loggers
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	// Set the global time format for zerolog
	zerolog.TimeFieldFormat = "2006-01-02T15:04:05.000Z"
	// Optional: force UTC to ensure 'Z' (Zulu time) is used instead of a numeric offset
	zerolog.TimestampFieldName = "@timestamp" // example for compatibility with some log processors
	logger := log.Logger

	// I hardcoded locally, but production reads from env/secret manager.
	username := os.Getenv("ES_USERNAME")
	if username == "" {
		username = "elastic"
	}
	password := os.Getenv("ES_PASSWORD")
	if password == "" {
		password = "UMEFncAL6JL_kBNauzej"
	}

	// Elasticsearch config
	cfg := elasticsearch.Config{
		Addresses: []string{
			"https://localhost:9200",
		},
		Username: username,
		Password: password,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	// Elasticsearch client initialisation
	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create elasticsearch client")
	}

	index := "inshorts-news-new"

	err = createMappingsSettings(index, es)
	if err != nil {
		fmt.Printf("error while creating mappings in es. Reason: (%v)", err)
	}

	startTime := time.Now()
	err = loadNewsFromFile(index, "/Users/keshavlodhi/GoProjects/inshorts-news-data-syncer/resources/news_data.json", es)
	if err != nil {
		fmt.Println("the error while inserting doc in es: ", err)
	}
	elapsed := time.Since(startTime)
	tookMs := elapsed.Milliseconds()
	fmt.Println("time taken to insert documents in ES: ", tookMs)
}

func createMappingsSettings(index string, es *elasticsearch.Client) error {
	exists, _ := es.Indices.Exists([]string{index})
	if exists.StatusCode == 200 {
		return nil
	}

	// 1. Define the mapping and settings as a JSON string
	// Note: The "type" mapping has been removed in modern ES versions (v7+)
	// and is replaced by "properties" within the "mappings" object.
	mapping := `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		},
		"mappings": {
			"properties": {
				"title": {
					"type": "text"
				},
				"description": {
					"type": "text"
				},
				"source_name": {
					"type": "keyword"
				},
				"category": {
					"type": "keyword"
				},
				"publication_date": {
					"type": "date"
				},
				"relevance_score": {
					"type": "float"
				},
				"latitude": {
					"type": "float"
				},
				"longitude": {
					"type": "float"
				},
				"location": {
					"type": "geo_point"
				}
			}
		}
	}`

	// 2. Create the index creation request
	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(mapping),
	}

	// 3. Execute the request
	res, err := req.Do(context.Background(), es)
	if err != nil {
		// Log the error. If the index already exists, ES will return an error.
		log.Printf("Error creating index: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		fmt.Printf("Error response: %s\n", res.String())
	} else {
		fmt.Printf("Index %s created successfully! Status: %s\n", index, res.Status())
	}
	return nil
}

func loadNewsFromFile(index string, path string, es *elasticsearch.Client) error {
	if _, err := os.Stat(path); err != nil {
		return fmt.Errorf("file not found at path %s: %w", path, err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var articles []Article
	if err := json.Unmarshal(data, &articles); err != nil {
		return err
	}

	for _, a := range articles {
		formattedDate, err := utils.NormalizeToESDate(a.PublicationDate)
		if err != nil {
			return err
		}
		doc := map[string]interface{}{
			"id":               a.ID,
			"title":            a.Title,
			"description":      a.Description,
			"url":              a.URL,
			"publication_date": formattedDate,
			"source_name":      a.SourceName,
			"category":         a.Category,
			"relevance_score":  a.RelevanceScore,
			"latitude":         a.Latitude,
			"longitude":        a.Longitude,
			"location": map[string]float64{
				"lat": a.Latitude,
				"lon": a.Longitude,
			},
		}

		// a := Article{
		// 	ID: a.ID,
		// 	Title: a.Title,
		// 	Description: a.Description,
		// 	URL: a.URL,
		// 	PublicationDate: a.PublicationDate,
		// 	SourceName: a.SourceName,
		// 	Category: a.Category,
		// 	RelevanceScore: a.RelevanceScore,
		// 	Latitude: a.Latitude,
		// 	Longitude: a.Longitude,

		// }

		body, _ := json.Marshal(doc)
		res, err := es.Index(index,
			bytes.NewReader(body),
			es.Index.WithDocumentID(a.ID),
			es.Index.WithRefresh("true"),
		)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.IsError() {
			return fmt.Errorf("insertion failed: %s", res.String())
		}
	}
	return nil
}
