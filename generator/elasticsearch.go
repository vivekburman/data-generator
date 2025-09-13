package generator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	mathrand "math/rand"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

type UserActivityLog struct {
	UserID           int64     `json:"user_id"`
	SessionID        string    `json:"session_id"`
	EventType        int       `json:"event_type"`
	TimestampUTC     time.Time `json:"timestamp_utc"`
	PartitionDate    string    `json:"partition_date"`
	IPAddress        string    `json:"ip_address"`
	UserAgentHash    int64     `json:"user_agent_hash"`
	PageURLHash      int64     `json:"page_url_hash"`
	ReferrerHash     int64     `json:"referrer_hash,omitempty"`
	CountryCode      string    `json:"country_code"`
	DeviceType       int       `json:"device_type"`
	ResponseTimeMs   int       `json:"response_time_ms"`
	StatusCode       int       `json:"status_code"`
	BytesTransferred int       `json:"bytes_transferred"`
}

func Elasticsearch() {
	// Initialize Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
		// Uncomment if you have authentication
		// Username: "elastic",
		// Password: "your-password",
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// Test connection
	res, err := es.Info()
	if err != nil {
		log.Fatalf("Error getting Elasticsearch info: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Elasticsearch error: %s", res.String())
	}

	fmt.Println("Connected to Elasticsearch successfully")

	// Create index with proper mapping and settings
	createIndex(es)

	// Insert test data
	insertElasticsearch(es)
}

func createIndex(es *elasticsearch.Client) {
	indexName := "user-activity-log"

	// Index mapping optimized for billion-scale records
	mapping := `{
		"settings": {
			"number_of_shards": 5,
			"number_of_replicas": 1,
			"refresh_interval": "30s",
			"index.codec": "best_compression",
			"index.mapping.total_fields.limit": 2000,
			"index.max_result_window": 50000
		},
		"mappings": {
			"properties": {
				"user_id": {
					"type": "long"
				},
				"session_id": {
					"type": "keyword",
					"index": false
				},
				"event_type": {
					"type": "byte"
				},
				"timestamp_utc": {
					"type": "date",
					"format": "strict_date_optional_time||epoch_millis"
				},
				"partition_date": {
					"type": "date",
					"format": "yyyy-MM-dd"
				},
				"ip_address": {
					"type": "ip"
				},
				"user_agent_hash": {
					"type": "long"
				},
				"page_url_hash": {
					"type": "long"
				},
				"referrer_hash": {
					"type": "long"
				},
				"country_code": {
					"type": "keyword"
				},
				"device_type": {
					"type": "byte"
				},
				"response_time_ms": {
					"type": "short"
				},
				"status_code": {
					"type": "short"
				},
				"bytes_transferred": {
					"type": "integer"
				}
			}
		}
	}`

	// Check if index exists
	existsReq := esapi.IndicesExistsRequest{
		Index: []string{indexName},
	}

	existsRes, err := existsReq.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error checking if index exists: %s", err)
	}
	defer existsRes.Body.Close()

	// If index exists (status 200), delete it first
	if existsRes.StatusCode == 200 {
		fmt.Printf("Index %s exists, deleting it...\n", indexName)
		deleteReq := esapi.IndicesDeleteRequest{
			Index: []string{indexName},
		}
		deleteRes, err := deleteReq.Do(context.Background(), es)
		if err != nil {
			log.Fatalf("Error deleting index: %s", err)
		}
		defer deleteRes.Body.Close()

		if deleteRes.IsError() {
			log.Fatalf("Error deleting index: %s", deleteRes.String())
		}
		fmt.Printf("Index %s deleted successfully\n", indexName)
	}

	// Create the index
	createReq := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(mapping),
	}

	res, err := createReq.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error creating index: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Fatalf("Error creating index: %s", res.String())
	}

	fmt.Printf("Index %s created successfully\n", indexName)
}

func insertElasticsearch(es *elasticsearch.Client) {
	indexName := "user-activity-log"
	totalRecords := 1000000 // Insert 1 million for testing
	batchSize := 1000       // Bulk insert batch size

	fmt.Printf("Starting to insert %d records in batches of %d\n", totalRecords, batchSize)

	var batch []UserActivityLog
	insertedCount := 0

	for i := 0; i < totalRecords; i++ {
		timestamp := time.Now().Add(-time.Duration(mathrand.Intn(86400)) * time.Second)

		record := UserActivityLog{
			UserID:           mathrand.Int63n(100000),
			SessionID:        generateSessionID(),
			EventType:        mathrand.Intn(5) + 1,
			TimestampUTC:     timestamp,
			PartitionDate:    timestamp.Format("2006-01-02"),
			IPAddress:        generateRandomIP(),
			UserAgentHash:    mathrand.Int63(),
			PageURLHash:      mathrand.Int63(),
			ReferrerHash:     mathrand.Int63(),
			CountryCode:      getRandomCountryCode(),
			DeviceType:       mathrand.Intn(3) + 1,
			ResponseTimeMs:   mathrand.Intn(5000),
			StatusCode:       getRandomStatusCode(),
			BytesTransferred: mathrand.Intn(10000),
		}

		batch = append(batch, record)

		// When batch is full or it's the last record, perform bulk insert
		if len(batch) >= batchSize || i == totalRecords-1 {
			err := bulkInsert(es, indexName, batch)
			if err != nil {
				log.Fatalf("Error during bulk insert: %s", err)
			}

			insertedCount += len(batch)
			batch = batch[:0] // Clear the batch

			if insertedCount%50000 == 0 || i == totalRecords-1 {
				fmt.Printf("Inserted %d records\n", insertedCount)
			}
		}
	}

	fmt.Println("Completed: 1 million records inserted into Elasticsearch")

	// Refresh index to make data searchable immediately
	refreshReq := esapi.IndicesRefreshRequest{
		Index: []string{indexName},
	}

	res, err := refreshReq.Do(context.Background(), es)
	if err != nil {
		log.Printf("Warning: Error refreshing index: %s", err)
	} else {
		defer res.Body.Close()
		if !res.IsError() {
			fmt.Println("Index refreshed successfully")
		}
	}
}

func bulkInsert(es *elasticsearch.Client, indexName string, records []UserActivityLog) error {
	var buf bytes.Buffer

	for _, record := range records {
		// Add the index action
		meta := fmt.Sprintf(`{"index":{"_index":"%s"}}`, indexName)
		buf.WriteString(meta + "\n")

		// Add the document
		docBytes, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("error marshaling document: %w", err)
		}
		buf.Write(docBytes)
		buf.WriteString("\n")
	}

	// Perform the bulk request
	req := esapi.BulkRequest{
		Index: indexName,
		Body:  &buf,
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("error performing bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk request failed: %s", res.String())
	}

	// Parse response to check for individual document errors
	var bulkResponse map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("error parsing bulk response: %w", err)
	}

	if bulkResponse["errors"].(bool) {
		// There were errors in the bulk request
		items := bulkResponse["items"].([]interface{})
		errorCount := 0
		for _, item := range items {
			itemMap := item.(map[string]interface{})
			if indexResult, ok := itemMap["index"].(map[string]interface{}); ok {
				if _, hasError := indexResult["error"]; hasError {
					errorCount++
					if errorCount <= 5 { // Log only first 5 errors
						fmt.Printf("Document error: %v\n", indexResult["error"])
					}
				}
			}
		}
		if errorCount > 0 {
			return fmt.Errorf("bulk request had %d errors", errorCount)
		}
	}

	return nil
}

// Helper functions

func generateSessionID() string {
	bytes := make([]byte, 16)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

func generateRandomIP() string {
	return fmt.Sprintf("192.168.%d.%d", mathrand.Intn(255), mathrand.Intn(255))
}

func getRandomCountryCode() string {
	countries := []string{"US", "CA", "GB", "DE", "FR", "JP", "AU", "BR", "IN", "CN"}
	return countries[mathrand.Intn(len(countries))]
}

func getRandomStatusCode() int {
	statusCodes := []int{200, 201, 204, 301, 302, 304, 400, 401, 403, 404, 500, 502, 503}
	weights := []int{70, 5, 3, 3, 2, 2, 5, 2, 2, 4, 1, 1, 1} // Weighted distribution

	totalWeight := 0
	for _, weight := range weights {
		totalWeight += weight
	}

	random := mathrand.Intn(totalWeight)
	currentWeight := 0

	for i, weight := range weights {
		currentWeight += weight
		if random < currentWeight {
			return statusCodes[i]
		}
	}

	return 200 // Default fallback
}
