package generator

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func MariaDB() {
	username := "root"
	password := "mariadb"
	host := "localhost"
	port := 3307
	database := "source_data_db"
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
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
	insertMariaDB(db)
}

func insertMariaDB(db *sql.DB) {
	totalRecords := 1000000 // Insert 1 million for testing
	stmt, err := db.Prepare(`
  INSERT INTO user_activity_log 
  (user_id, session_id, event_type, timestamp_utc, partition_date, ip_address, user_agent_hash, 
   page_url_hash, country_code, device_type, response_time_ms, status_code, bytes_transferred) 
  VALUES (?, UNHEX(REPLACE(UUID(), '-', '')), ?, ?, DATE(?), INET6_ATON(?), ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
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
			log.Fatalf("Insert err: %s\n", err.Error())
			return
		}

		if i%50000 == 0 {
			fmt.Printf("Inserted %d records\n", i)
		}
	}
	fmt.Println("Completed: 1 million records inserted into billion-capable table")
}
