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

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"inshorts.com/inshorts-news-data-syncer/utils"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/elastic/go-elasticsearch/v9/esapi"
)

const (
	indexName = "inshorts-news"
	bulkSize  = 500
	path      = "resources/news_data.json"
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
		log.Fatal().Caller().Err(err).Msg("failed to create elasticsearch client")
	}

	// Create index mapping before inserting data
	err = createMappingsSettings(indexName, es)
	if err != nil {
		log.Error().Caller().Err(err).Msg("error while creating mappings in es")
	}

	// Load articles from json file
	startTime := time.Now()
	articles, err := loadArticles(path)
	if err != nil {
		log.Fatal().Caller().Err(err).Msg("error while loading articles from json file")
	}

	// Insert articles into elastic by using bulk api
	if err := bulkIndex(es, articles); err != nil {
		log.Fatal().Caller().Err(err).Msg("error while inserting articles in es using bulk api")
	}
	log.Info().Caller().Msgf("indexed %d articles in %v milliseconds\n", len(articles), time.Since(startTime).Milliseconds())
}

func createMappingsSettings(index string, es *elasticsearch.Client) error {
	// Check if index already exists
	exists, _ := es.Indices.Exists([]string{index})
	if exists.StatusCode == 200 {
		return nil
	}

	// 1. Define the mapping and settings as a JSON string
	var settingsAndmappings = `
{
  "settings": {
    "analysis": {
      "analyzer": {
        "news_text": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "stop",
            "english_stemmer"
          ]
        }
      },
      "filter": {
        "english_stemmer": {
          "type": "stemmer",
          "language": "english"
        }
      },
      "normalizer": {
        "keyword_lowercase": {
          "type": "custom",
          "filter": ["lowercase"]
        }
      }
    }
  },
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {
        "type": "keyword"
      },
	  "url": {
  		"type": "keyword",
  		"ignore_above": 2048
	  },
      "title": {
        "type": "text",
        "analyzer": "news_text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "description": {
        "type": "text",
        "analyzer": "news_text"
      },
      "llm_summary": {
        "type": "text",
        "analyzer": "news_text"
      },
      "source_name": {
        "type": "text",
        "analyzer": "news_text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "keyword_lowercase"
          }
        }
      },
      "category": {
        "type": "text",
        "analyzer": "news_text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "normalizer": "keyword_lowercase"
          }
        }
      },
      "publication_date": {
        "type": "date"
      },
      "location": {
        "type": "geo_point"
      },
      "relevance_score": {
        "type": "float"
      },
	  "latitude": {
  		"type": "float"
	  },
	  "longitude": {
  		"type": "float"
	  }
    }
  }
}
`
	// 2. Create the index creation request
	req := esapi.IndicesCreateRequest{
		Index: index,
		Body:  strings.NewReader(settingsAndmappings),
	}

	// 3. Execute the request
	res, err := req.Do(context.Background(), es)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Error().Caller().Err(err).Msgf("error response: %s\n", res.String())
	} else {
		log.Info().Caller().Err(err).Msgf("index: (%s) created successfully. Status: %s\n", index, res.Status())
	}
	return nil
}

func loadArticles(path string) ([]Article, error) {
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("file not found at path %s: %w", path, err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var articles []Article
	if err := json.Unmarshal(data, &articles); err != nil {
		return nil, err
	}
	return articles, nil
}

func bulkIndex(es *elasticsearch.Client, articles []Article) error {
	var buf bytes.Buffer
	ctx := context.Background()

	for i, a := range articles {
		formattedDate, err := utils.NormalizeToESDate(a.PublicationDate)
		if err != nil {
			return err
		}
		meta := fmt.Sprintf(
			`{ "index": { "_index": "%s", "_id": "%s" } }%s`,
			indexName, a.ID, "\n",
		)
		buf.WriteString(meta)

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

		body, err := json.Marshal(doc)
		if err != nil {
			return err
		}
		buf.Write(body)
		buf.WriteByte('\n')

		if (i+1)%bulkSize == 0 {
			if err := flushBulk(ctx, es, &buf); err != nil {
				return err
			}
		}
	}

	return flushBulk(ctx, es, &buf)
}

func flushBulk(ctx context.Context, es *elasticsearch.Client, buf *bytes.Buffer) error {
	if buf.Len() == 0 {
		return nil
	}

	res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithContext(ctx))
	if err != nil {
		return err
	}
	defer res.Body.Close()

	var bulkResp struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int                    `json:"status"`
			Error  map[string]interface{} `json:"error,omitempty"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&bulkResp); err != nil {
		return err
	}

	if bulkResp.Errors {
		for _, item := range bulkResp.Items {
			for _, action := range item {
				if action.Error != nil {
					return fmt.Errorf("bulk item failed: %+v", action.Error)
				}
			}
		}
	}

	buf.Reset()
	return nil
}
