package generator

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type mongoDBUserActivityLog struct {
	ID               primitive.ObjectID `bson:"_id,omitempty"`
	UserID           int64              `bson:"user_id"`
	SessionID        primitive.Binary   `bson:"session_id"`
	EventType        int                `bson:"event_type"`
	TimestampUTC     time.Time          `bson:"timestamp_utc"`
	PartitionDate    time.Time          `bson:"partition_date"`
	IPAddress        string             `bson:"ip_address,omitempty"`
	UserAgentHash    int64              `bson:"user_agent_hash,omitempty"`
	PageURLHash      int64              `bson:"page_url_hash,omitempty"`
	ReferrerHash     int64              `bson:"referrer_hash,omitempty"`
	CountryCode      string             `bson:"country_code,omitempty"`
	DeviceType       int                `bson:"device_type,omitempty"`
	ResponseTimeMs   int                `bson:"response_time_ms,omitempty"`
	StatusCode       int                `bson:"status_code,omitempty"`
	BytesTransferred int                `bson:"bytes_transferred,omitempty"`
}

func MongoDB() {
	// MongoDB connection
	ctx := context.Background()
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Test connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatalf("Failed to ping MongoDB: %v", err)
	}

	db := client.Database("source_data_db")
	collection := db.Collection("user_activity_log")

	// Setup collection for billion-record scale
	setupCollection(ctx, collection)

	// Insert records
	insertMongoDB(ctx, collection)
}

func setupCollection(ctx context.Context, collection *mongo.Collection) {
	fmt.Println("Setting up collection with indexes for billion-record scale...")

	// Drop existing indexes (except _id) to recreate them
	cursor, err := collection.Indexes().List(ctx)
	if err == nil {
		var indexes []bson.M
		cursor.All(ctx, &indexes)
		for _, index := range indexes {
			if name, ok := index["name"].(string); ok && name != "_id_" {
				collection.Indexes().DropOne(ctx, name)
			}
		}
	}

	// Create compound indexes optimized for billion records
	indexes := []mongo.IndexModel{
		// Compound index for user queries with time range
		{
			Keys: bson.D{
				{"user_id", 1},
				{"timestamp_utc", 1},
			},
			Options: options.Index().SetName("idx_user_time").SetBackground(true),
		},
		// Compound index for event type with time
		{
			Keys: bson.D{
				{"event_type", 1},
				{"timestamp_utc", 1},
			},
			Options: options.Index().SetName("idx_event_time").SetBackground(true),
		},
		// Session ID index
		{
			Keys:    bson.D{{"session_id", 1}},
			Options: options.Index().SetName("idx_session").SetBackground(true),
		},
		// Partition date index for time-based queries
		{
			Keys:    bson.D{{"partition_date", 1}},
			Options: options.Index().SetName("idx_partition_date").SetBackground(true),
		},
		// Compound index for analytics queries
		{
			Keys: bson.D{
				{"country_code", 1},
				{"device_type", 1},
				{"timestamp_utc", 1},
			},
			Options: options.Index().SetName("idx_analytics").SetBackground(true),
		},
		// Sparse index for IP addresses (only when present)
		{
			Keys:    bson.D{{"ip_address", 1}},
			Options: options.Index().SetName("idx_ip").SetBackground(true).SetSparse(true),
		},
	}

	// Create indexes
	_, err = collection.Indexes().CreateMany(ctx, indexes)
	if err != nil {
		log.Printf("Warning: Failed to create some indexes: %v", err)
	}

	// Enable sharding preparation (for billion-record scale)
	// Note: This requires MongoDB to be configured as a replica set or sharded cluster
	fmt.Println("Collection setup completed with indexes optimized for billion records")
	fmt.Println("For billion-record scale, consider:")
	fmt.Println("1. Sharding on {user_id: 1, timestamp_utc: 1}")
	fmt.Println("2. Using MongoDB time series collections (MongoDB 5.0+)")
	fmt.Println("3. Implementing data retention policies")
}

func insertMongoDB(ctx context.Context, collection *mongo.Collection) {
	totalRecords := 1000000 // Insert 1 million for testing
	batchSize := 1000       // Optimal batch size for MongoDB

	fmt.Printf("Starting insertion of %d records in batches of %d...\n", totalRecords, batchSize)

	// Generate country codes for variety
	countryCodes := []string{"US", "GB", "DE", "FR", "JP", "IN", "BR", "CA", "AU", "CN"}

	var batch []interface{}
	insertedCount := 0

	for i := 0; i < totalRecords; i++ {
		// Generate random timestamp within last 24 hours
		timestamp := time.Now().Add(-time.Duration(rand.Intn(86400)) * time.Second)
		partitionDate := time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), 0, 0, 0, 0, timestamp.Location())

		// Generate session ID as 16-byte binary
		sessionIDBytes := make([]byte, 16)
		rand.Read(sessionIDBytes)

		record := mongoDBUserActivityLog{
			UserID:           rand.Int63n(100000),
			SessionID:        primitive.Binary{Data: sessionIDBytes, Subtype: 0x00},
			EventType:        rand.Intn(5) + 1,
			TimestampUTC:     timestamp,
			PartitionDate:    partitionDate,
			IPAddress:        fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)),
			UserAgentHash:    rand.Int63(),
			PageURLHash:      rand.Int63(),
			ReferrerHash:     rand.Int63(),
			CountryCode:      countryCodes[rand.Intn(len(countryCodes))],
			DeviceType:       rand.Intn(3) + 1,
			ResponseTimeMs:   rand.Intn(5000),
			StatusCode:       200 + rand.Intn(100), // 200-299 range
			BytesTransferred: rand.Intn(10000),
		}

		batch = append(batch, record)

		// Insert batch when it reaches batchSize or it's the last record
		if len(batch) == batchSize || i == totalRecords-1 {
			opts := options.InsertMany().SetOrdered(false) // Unordered for better performance
			_, err := collection.InsertMany(ctx, batch, opts)
			if err != nil {
				log.Printf("Batch insert error: %v", err)
				continue
			}

			insertedCount += len(batch)
			batch = batch[:0] // Clear batch

			if insertedCount%50000 == 0 {
				fmt.Printf("Inserted %d records\n", insertedCount)
			}
		}
	}

	// Get total count in collection
	totalCount, err := collection.CountDocuments(ctx, bson.M{})
	if err != nil {
		log.Printf("Failed to count documents: %v", err)
	} else {
		fmt.Printf("Completed: %d records inserted. Total records in collection: %d\n", insertedCount, totalCount)
	}
}

// Helper function to create time series collection (MongoDB 5.0+)
func CreateTimeSeriesCollection(ctx context.Context, db *mongo.Database) {
	collectionName := "user_activity_log_timeseries"

	// Time series collection options
	timeSeriesOptions := options.TimeSeries().
		SetTimeField("timestamp_utc").
		SetMetaField("user_id").
		SetGranularity("minutes")

	opts := options.CreateCollection().SetTimeSeriesOptions(timeSeriesOptions)

	err := db.CreateCollection(ctx, collectionName, opts)
	if err != nil {
		log.Printf("Failed to create time series collection: %v", err)
		return
	}

	fmt.Printf("Time series collection '%s' created successfully\n", collectionName)
}
