package generator

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq" // postgres driver
)

func Postgres() {
	username := "postgres"
	password := "postgres"
	host := "localhost"
	port := 5432
	database := "source_data_db"
	schema := "source_schema"

	// Build DSN
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s search_path=%s sslmode=disable",
		host, port, username, password, database, schema)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("failed to open connection: %v", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("failed to ping database: %v", err)
	}
	createTable := `
	CREATE TABLE IF NOT EXISTS user_activity_log (
		id BIGINT GENERATED ALWAYS AS IDENTITY,
		user_id BIGINT NOT NULL,
		session_id UUID NOT NULL,
		event_type SMALLINT NOT NULL,
		timestamp_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		ip_address INET,
		user_agent_hash BIGINT,
		page_url_hash BIGINT,
		referrer_hash BIGINT,
		country_code CHAR(2),
		device_type SMALLINT,
		response_time_ms INTEGER CHECK (response_time_ms >= 0 AND response_time_ms <= 65535),
		status_code INTEGER CHECK (status_code >= 0 AND status_code <= 65535),
		bytes_transferred BIGINT CHECK (bytes_transferred >= 0),
		PRIMARY KEY (id)
	);`
	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("failed creating table: %v", err)
	}
	insertPostgres(db)
}

func insertPostgres(db *sql.DB) {
	totalRecords := 1000000 // Insert 1 million for testing

	stmt, err := db.Prepare(`
		INSERT INTO user_activity_log 
		(user_id, session_id, event_type, timestamp_utc, ip_address, user_agent_hash, 
		 page_url_hash, referrer_hash, country_code, device_type, response_time_ms, status_code, bytes_transferred) 
		VALUES ($1, gen_random_uuid(), $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`)
	if err != nil {
		log.Fatalf("prepare failed: %v", err)
	}
	defer stmt.Close()

	for i := 0; i < totalRecords; i++ {
		timestamp := time.Now().Add(-time.Duration(rand.Intn(86400)) * time.Second) // within last 24h

		_, err := stmt.Exec(
			rand.Int63n(100000), // user_id
			rand.Intn(5)+1,      // event_type
			timestamp,           // timestamp_utc
			fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)), // ip_address
			rand.Int63(),     // user_agent_hash
			rand.Int63(),     // page_url_hash
			rand.Int63(),     // referrer_hash
			"US",             // country_code
			rand.Intn(3)+1,   // device_type
			rand.Intn(5000),  // response_time_ms
			200,              // status_code
			rand.Intn(10000), // bytes_transferred
		)
		if err != nil {
			log.Fatalf("insert failed: %v", err)
		}

		if i%50000 == 0 {
			fmt.Printf("Inserted %d records\n", i)
		}
	}
	fmt.Println("✅ Completed: 1 million records inserted into Postgres user_activity_log")
}
func PostgresRelational() {
	username := "postgres"
	password := "postgres"
	host := "localhost"
	port := 5432
	database := "source_data_db"
	schema := "source_schema"

	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s search_path=%s sslmode=disable",
		host, port, username, password, database, schema)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer db.Close()

	createUsersTablePostgres(db)
	createSessionsTablePostgres(db)
	createActivityTablePostgres(db)
	insertRelationalDataPostgres(db)
}

func createUsersTablePostgres(db *sql.DB) {
	createTable := `
 CREATE TABLE IF NOT EXISTS users (
  id BIGSERIAL PRIMARY KEY,
  country_code CHAR(2) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
 )`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func createSessionsTablePostgres(db *sql.DB) {
	createTable := `
 CREATE TABLE IF NOT EXISTS sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id BIGINT NOT NULL,
  device_type SMALLINT,
  user_agent_hash BIGINT,
  created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
 )`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func createActivityTablePostgres(db *sql.DB) {
	createTable := `
 CREATE TABLE IF NOT EXISTS activity_events (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,
  session_id UUID NOT NULL,
  event_type SMALLINT NOT NULL,
  timestamp_utc TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip_address INET,
  page_url_hash BIGINT,
  referrer_hash BIGINT,
  response_time_ms INTEGER,
  status_code INTEGER,
  bytes_transferred BIGINT,
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (session_id) REFERENCES sessions(id)
 )`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func insertRelationalDataPostgres(db *sql.DB) {
	totalRecords := 1000000

	userStmt, err := db.Prepare("INSERT INTO users (id, country_code) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING")
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer userStmt.Close()

	sessionStmt, err := db.Prepare("INSERT INTO sessions (user_id, device_type, user_agent_hash) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING RETURNING id")
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer sessionStmt.Close()

	activityStmt, err := db.Prepare(`
  INSERT INTO activity_events 
  (user_id, session_id, event_type, timestamp_utc, ip_address, page_url_hash, response_time_ms, status_code, bytes_transferred) 
  VALUES ($1, (SELECT id FROM sessions WHERE user_id = $2 ORDER BY RANDOM() LIMIT 1), $3, $4, $5, $6, $7, $8, $9)`)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer activityStmt.Close()

	for i := 0; i < totalRecords; i++ {
		userID := rand.Int63n(100000) + 1
		deviceType := rand.Intn(3) + 1
		userAgentHash := rand.Int63()
		timestamp := time.Now().Add(-time.Duration(rand.Intn(86400)) * time.Second)
		eventType := rand.Intn(5) + 1
		pageURLHash := rand.Int63()
		responseTime := rand.Intn(5000)
		statusCode := 200
		bytesTransferred := rand.Intn(10000)
		ipAddress := fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255))
		countryCode := "US"

		userStmt.Exec(userID, countryCode)
		sessionStmt.Exec(userID, deviceType, userAgentHash)
		activityStmt.Exec(userID, userID, eventType, timestamp, ipAddress, pageURLHash, responseTime, statusCode, bytesTransferred)

		if i%50000 == 0 {
			fmt.Printf("Inserted %d relational records\n", i)
		}
	}
	fmt.Println("Completed: 1 million records inserted into relational tables")
}
