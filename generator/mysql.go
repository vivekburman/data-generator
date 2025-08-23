package generator

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func MySQL() {
	username := "root"
	password := "mysql"
	host := "localhost"
	port := 3306
	database := "source_data_db"
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("err: %s\n", err.Error())
		return
	}
	defer db.Close()

	// Create table designed for 1 billion records with proper partitioning
	createTable := `
 CREATE TABLE IF NOT EXISTS user_activity_log (
    id BIGINT UNSIGNED AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    session_id BINARY(16) NOT NULL,
    event_type TINYINT UNSIGNED NOT NULL,
    timestamp_utc TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    partition_date DATE NOT NULL,
    ip_address VARBINARY(16),
    user_agent_hash BIGINT,
    page_url_hash BIGINT,
    referrer_hash BIGINT,
    country_code CHAR(2),
    device_type TINYINT UNSIGNED,
    response_time_ms SMALLINT UNSIGNED,
    status_code SMALLINT UNSIGNED,
    bytes_transferred INT UNSIGNED,
    INDEX idx_user_time (user_id, timestamp_utc),
    INDEX idx_event_time (event_type, timestamp_utc),
    INDEX idx_session (session_id),
    PRIMARY KEY (id, partition_date)
) ENGINE=InnoDB 
PARTITION BY RANGE (TO_DAYS(partition_date)) (
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p202509 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p202510 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p202511 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p202512 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
)`

	_, err = db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	insertMySQL(db)
}

func insertMySQL(db *sql.DB) {
	totalRecords := 1000000 // Insert 1 million for testing
	stmt, err := db.Prepare(`
     INSERT INTO user_activity_log 
     (user_id, session_id, event_type, timestamp_utc, partition_date, ip_address, user_agent_hash, 
      page_url_hash, country_code, device_type, response_time_ms, status_code, bytes_transferred) 
     VALUES (?, UNHEX(REPLACE(UUID(), '-', '')), ?, ?, DATE(?), INET6_ATON(?), ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
		return
	}
	defer stmt.Close()

	for i := 0; i < totalRecords; i++ {
		timestamp := time.Now().Add(-time.Duration(rand.Intn(86400)) * time.Second)
		_, err := stmt.Exec(
			rand.Int63n(100000), // user_id
			rand.Intn(5)+1,      // event_type
			timestamp,           // timestamp_utc
			timestamp,           // partition_date (extracted from timestamp)
			fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255)), // ip
			rand.Int63(),     // user_agent_hash
			rand.Int63(),     // page_url_hash
			"US",             // country_code
			rand.Intn(3)+1,   // device_type
			rand.Intn(5000),  // response_time_ms
			200,              // status_code
			rand.Intn(10000), // bytes_transferred
		)
		if err != nil {
			fmt.Printf("Insert err: %s\n", err.Error())
			return
		}

		if i%50000 == 0 {
			fmt.Printf("Inserted %d records\n", i)
		}
	}
	fmt.Println("Completed: 1 million records inserted into billion-capable table")
}

// New relational approach
func MySQLRelational() {
	username := "root"
	password := "mysql"
	host := "localhost"
	port := 3306
	database := "source_data_db"
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("err: %s\n", err.Error())
		return
	}
	defer db.Close()

	createUsersTable(db)
	createSessionsTable(db)
	createActivityTable(db)
	insertRelationalData(db)
}

func createUsersTable(db *sql.DB) {
	createTable := `
 CREATE TABLE IF NOT EXISTS users (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    country_code CHAR(2) NOT NULL,
    created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_country (country_code)
 ) ENGINE=InnoDB`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func createSessionsTable(db *sql.DB) {
	createTable := `
 CREATE TABLE IF NOT EXISTS sessions (
    id BINARY(16) PRIMARY KEY,
    user_id BIGINT UNSIGNED NOT NULL,
    device_type TINYINT UNSIGNED,
    user_agent_hash BIGINT,
    created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3),
    INDEX idx_user (user_id),
    FOREIGN KEY (user_id) REFERENCES users(id)
 ) ENGINE=InnoDB`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func createActivityTable(db *sql.DB) {
	createTable := `
 CREATE TABLE IF NOT EXISTS activity_events (
    id BIGINT UNSIGNED AUTO_INCREMENT,
    user_id BIGINT UNSIGNED NOT NULL,
    session_id BINARY(16) NOT NULL,
    event_type TINYINT UNSIGNED NOT NULL,
    timestamp_utc TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    partition_date DATE NOT NULL,
    ip_address VARBINARY(16),
    page_url_hash BIGINT,
    referrer_hash BIGINT,
    response_time_ms SMALLINT UNSIGNED,
    status_code SMALLINT UNSIGNED,
    bytes_transferred INT UNSIGNED,
    INDEX idx_user_time (user_id, timestamp_utc),
    INDEX idx_event_time (event_type, timestamp_utc),
    INDEX idx_session (session_id),
    PRIMARY KEY (id, partition_date)
 ) ENGINE=InnoDB 
 PARTITION BY RANGE (TO_DAYS(partition_date)) (
    PARTITION p202508 VALUES LESS THAN (TO_DAYS('2025-09-01')),
    PARTITION p202509 VALUES LESS THAN (TO_DAYS('2025-10-01')),
    PARTITION p202510 VALUES LESS THAN (TO_DAYS('2025-11-01')),
    PARTITION p202511 VALUES LESS THAN (TO_DAYS('2025-12-01')),
    PARTITION p202512 VALUES LESS THAN (TO_DAYS('2026-01-01')),
    PARTITION p_future VALUES LESS THAN MAXVALUE
 )`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func insertRelationalData(db *sql.DB) {
	totalRecords := 1000000

	userStmt, err := db.Prepare("INSERT IGNORE INTO users (id, country_code) VALUES (?, ?)")
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer userStmt.Close()

	sessionStmt, err := db.Prepare("INSERT IGNORE INTO sessions (id, user_id, device_type, user_agent_hash) VALUES (UNHEX(REPLACE(UUID(), '-', '')), ?, ?, ?)")
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer sessionStmt.Close()

	activityStmt, err := db.Prepare(`
    INSERT INTO activity_events 
    (user_id, session_id, event_type, timestamp_utc, partition_date, ip_address, page_url_hash, response_time_ms, status_code, bytes_transferred) 
    VALUES (?, (SELECT id FROM sessions WHERE user_id = ? ORDER BY RAND() LIMIT 1), ?, ?, DATE(?), INET6_ATON(?), ?, ?, ?, ?)`)
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
		activityStmt.Exec(userID, userID, eventType, timestamp, timestamp, ipAddress, pageURLHash, responseTime, statusCode, bytesTransferred)

		if i%50000 == 0 {
			fmt.Printf("Inserted %d relational records\n", i)
		}
	}
	fmt.Println("Completed: 1 million records inserted into relational tables")
}
