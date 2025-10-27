package generator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	mathRand "math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var globalConn *amqp.Connection
var globalChannel *amqp.Channel

func RabbitMQ() {
	username := "guest"
	password := "guest"
	host := "localhost"
	port := 5672

	// Create RabbitMQ connection
	connURL := fmt.Sprintf("amqp://%s:%s@%s:%d/", username, password, host, port)
	conn, err := amqp.Dial(connURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	// Create channel
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	globalConn = conn
	globalChannel = ch

	insertRabbitMQ()
}

func insertRabbitMQ() {
	ctx := context.Background()
	queueName := "user_activity_queue"

	// Declare a durable queue
	_, err := globalChannel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Enable publisher confirms for reliability
	err = globalChannel.Confirm(false)
	if err != nil {
		log.Fatalf("Failed to enable publisher confirms: %v", err)
	}

	totalRecords := 1000000 // Insert 1 million records
	batchSize := 1000

	for i := 0; i < totalRecords; i += batchSize {
		// Publish batch of messages
		for j := 0; j < batchSize && i+j < totalRecords; j++ {
			recordID := int64(i + j + 1)
			userActivity := generateUserActivityMQ(recordID)

			// Convert to JSON
			messageBody, err := json.Marshal(userActivity)
			if err != nil {
				log.Fatalf("Failed to marshal user activity: %v", err)
			}

			// Publish message
			err = globalChannel.PublishWithContext(
				ctx,
				"",        // exchange
				queueName, // routing key (queue name)
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					DeliveryMode: amqp.Persistent, // Make message persistent
					ContentType:  "application/json",
					Body:         messageBody,
					Timestamp:    time.Now(),
				},
			)
			if err != nil {
				log.Fatalf("Failed to publish message: %v", err)
			}
		}

		if (i+batchSize)%50000 == 0 || i+batchSize >= totalRecords {
			fmt.Printf("Published %d messages\n", i+batchSize)
		}
	}

	fmt.Printf("Completed: 1 million messages published to queue '%s'\n", queueName)
}

func generateUserActivityMQ(recordID int64) map[string]interface{} {
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

// Close gracefully closes RabbitMQ resources
func CloseRabbitMQ() {
	if globalChannel != nil {
		globalChannel.Close()
	}
	if globalConn != nil {
		globalConn.Close()
	}
}
