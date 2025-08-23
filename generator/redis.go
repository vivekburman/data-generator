package generator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	mathRand "math/rand"
	"time"

	"github.com/redis/go-redis/v9"
)

var globalClient *redis.Client

func Redis() {
	username := "default"
	password := "redisdb"
	host := "localhost"
	port := 6379
	databaseIndex := 0

	// Create Redis client
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Username: username,
		Password: password,
		DB:       databaseIndex,
	})

	// Test the connection
	ctx := context.Background()
	_, err := client.Ping(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	globalClient = client
	insertRedis()
}

func insertRedis() {
	ctx := context.Background()

	// Get current record count
	currentCount, err := globalClient.Get(ctx, "user_activity:counter").Int64()
	if err != nil && err.Error() != "redis: nil" {
		currentCount = 0
	}

	totalRecords := 1000000 // Insert 1 million records
	batchSize := 1000

	for i := 0; i < totalRecords; i += batchSize {
		pipe := globalClient.Pipeline()

		// Insert batch of records
		for j := 0; j < batchSize && i+j < totalRecords; j++ {
			recordID := currentCount + int64(i+j+1)
			userActivity := generateUserActivity(recordID)

			hashKey := fmt.Sprintf("user_activity:%d", recordID)
			pipe.HMSet(ctx, hashKey, userActivity)
		}

		_, err := pipe.Exec(ctx)
		if err != nil {
			log.Fatalf("Pipeline execution failed: %v", err)
		}

		if (i+batchSize)%50000 == 0 || i+batchSize >= totalRecords {
			fmt.Printf("Inserted %d records\n", i+batchSize)
		}
	}

	// Update counter
	newCount := currentCount + int64(totalRecords)
	globalClient.Set(ctx, "user_activity:counter", newCount, 0)

	fmt.Printf("Completed: 1 million records inserted (Total: %d)\n", newCount)
}

func generateUserActivity(recordID int64) map[string]interface{} {
	// Generate random session ID
	sessionBytes := make([]byte, 16)
	rand.Read(sessionBytes)
	sessionID := hex.EncodeToString(sessionBytes)

	// Generate timestamp within last 24 hours
	timestamp := time.Now().Add(-time.Duration(mathRand.Intn(86400)) * time.Second)

	return map[string]interface{}{
		"id":                recordID,
		"user_id":           mathRand.Int63n(100000),
		"session_id":        sessionID,
		"event_type":        mathRand.Intn(5) + 1,
		"timestamp_utc":     timestamp.Unix(),
		"partition_date":    timestamp.Format("2006-01-02"),
		"ip_address":        fmt.Sprintf("192.168.%d.%d", mathRand.Intn(255), mathRand.Intn(255)),
		"user_agent_hash":   mathRand.Int63(),
		"page_url_hash":     mathRand.Int63(),
		"referrer_hash":     mathRand.Int63(),
		"country_code":      "US",
		"device_type":       mathRand.Intn(3) + 1,
		"response_time_ms":  mathRand.Intn(5000),
		"status_code":       200,
		"bytes_transferred": mathRand.Intn(10000),
	}
}
