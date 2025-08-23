package generator

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/godror/godror"
)

func Oracle() {
	username := "pdbadmin"
	password := "oracledb"
	host := "localhost"
	port := 1521
	database := "source_data_db"

	// Create connection string
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s", username, password, host, port, database)

	// Connect to database
	db, err := sql.Open("godror", dsn)
	if err != nil {
		log.Fatalf("Error connecting to database: %v\n", err)
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Error pinging database: %v\n", err)
	}

	fmt.Println("Successfully connected to Oracle database!")

	// Check if table exists
	if !tableExists(db, "USER_ACTIVITY_LOG") {
		// Create table equivalent to MySQL version
		createTableSQL := `
		CREATE TABLE user_activity_log (
			id NUMBER(19) NOT NULL,
			user_id NUMBER(19) NOT NULL,
			session_id RAW(16) NOT NULL,
			event_type NUMBER(3) NOT NULL,
			timestamp_utc TIMESTAMP(3) DEFAULT SYSTIMESTAMP NOT NULL,
			partition_date DATE NOT NULL,
			ip_address RAW(16),
			user_agent_hash NUMBER(19),
			page_url_hash NUMBER(19),
			referrer_hash NUMBER(19),
			country_code CHAR(2),
			device_type NUMBER(3),
			response_time_ms NUMBER(5),
			status_code NUMBER(5),
			bytes_transferred NUMBER(10),
			CONSTRAINT pk_user_activity PRIMARY KEY (id, partition_date)
		) 
		PARTITION BY RANGE (partition_date) (
			PARTITION p202508 VALUES LESS THAN (DATE '2025-09-01'),
			PARTITION p202509 VALUES LESS THAN (DATE '2025-10-01'),
			PARTITION p202510 VALUES LESS THAN (DATE '2025-11-01'),
			PARTITION p202511 VALUES LESS THAN (DATE '2025-12-01'),
			PARTITION p202512 VALUES LESS THAN (DATE '2026-01-01'),
			PARTITION p_future VALUES LESS THAN (MAXVALUE)
		)
		COMPRESS FOR OLTP`

		_, err = db.Exec(createTableSQL)
		if err != nil {
			log.Fatalf("Error creating table: %v\n", err)
		}
		fmt.Println("Table user_activity_log created successfully!")
	} else {
		fmt.Println("Table user_activity_log already exists, skipping creation")
	}

	// Check if sequence exists
	if !sequenceExists(db, "USER_ACTIVITY_LOG_SEQ") {
		// Create sequence for auto-incrementing ID (equivalent to AUTO_INCREMENT)
		createSeqSQL := `
		CREATE SEQUENCE user_activity_log_seq
		START WITH 1
		INCREMENT BY 1
		CACHE 1000
		NOCYCLE`

		_, err = db.Exec(createSeqSQL)
		if err != nil {
			log.Fatalf("Error creating sequence: %v\n", err)
		}
		fmt.Println("Sequence user_activity_log_seq created successfully!")
	} else {
		fmt.Println("Sequence user_activity_log_seq already exists, skipping creation")
	}

	// Check if trigger exists
	if !triggerExists(db, "TRG_USER_ACTIVITY_LOG_ID") {
		// Create trigger for auto-increment (equivalent to AUTO_INCREMENT)
		createTriggerSQL := `
		CREATE OR REPLACE TRIGGER trg_user_activity_log_id
		BEFORE INSERT ON user_activity_log
		FOR EACH ROW
		WHEN (NEW.id IS NULL)
		BEGIN
			:NEW.id := user_activity_log_seq.NEXTVAL;
		END;`

		_, err = db.Exec(createTriggerSQL)
		if err != nil {
			log.Fatalf("Error creating trigger: %v\n", err)
		}
		fmt.Println("Trigger trg_user_activity_log_id created successfully!")
	} else {
		fmt.Println("Trigger trg_user_activity_log_id already exists, skipping creation")
	}

	fmt.Println("All database objects are ready!")
	insertOracle(db)
}

// Helper function to check if table exists
func tableExists(db *sql.DB, tableName string) bool {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) 
		FROM user_tables 
		WHERE table_name = :1
	`, tableName).Scan(&count)

	if err != nil {
		log.Printf("Error checking table existence: %v", err)
		return false
	}
	return count > 0
}

// Helper function to check if sequence exists
func sequenceExists(db *sql.DB, sequenceName string) bool {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) 
		FROM user_sequences 
		WHERE sequence_name = :1
	`, sequenceName).Scan(&count)

	if err != nil {
		log.Printf("Error checking sequence existence: %v", err)
		return false
	}
	return count > 0
}

// Helper function to check if trigger exists
func triggerExists(db *sql.DB, triggerName string) bool {
	var count int
	err := db.QueryRow(`
		SELECT COUNT(*) 
		FROM user_triggers 
		WHERE trigger_name = :1
	`, triggerName).Scan(&count)

	if err != nil {
		log.Printf("Error checking trigger existence: %v", err)
		return false
	}
	return count > 0
}

func insertOracle(db *sql.DB) {
	totalRecords := 1000000 // Insert 1 million for testing

	// Oracle with numbered placeholders (:1, :2, etc.)
	// Use TO_DATE for explicit date conversion
	stmt, err := db.Prepare(`
		INSERT INTO user_activity_log 
		(user_id, session_id, event_type, timestamp_utc, partition_date, ip_address, user_agent_hash, 
		 page_url_hash, country_code, device_type, response_time_ms, status_code, bytes_transferred) 
		VALUES (:1, SYS_GUID(), :2, :3, TO_DATE(:4, 'YYYY-MM-DD'), HEXTORAW(:5), :6, :7, :8, :9, :10, :11, :12)`)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer stmt.Close()

	for i := 0; i < totalRecords; i++ {
		timestamp := time.Now().Add(-time.Duration(rand.Intn(86400)) * time.Second)

		// Convert IP address to hex string for Oracle RAW type
		ipHex := fmt.Sprintf("%02X%02X%02X%02X", 192, 168, rand.Intn(255), rand.Intn(255))

		// Format partition_date as DATE string for Oracle (YYYY-MM-DD format)
		partitionDate := timestamp.Format("2006-01-02")

		_, err := stmt.Exec(
			rand.Int63n(100000), // :1 - user_id
			rand.Intn(5)+1,      // :2 - event_type
			timestamp,           // :3 - timestamp_utc
			partitionDate,       // :4 - partition_date (string format YYYY-MM-DD)
			ipHex,               // :5 - ip_address as hex string for RAW type
			rand.Int63(),        // :6 - user_agent_hash
			rand.Int63(),        // :7 - page_url_hash
			"US",                // :8 - country_code
			rand.Intn(3)+1,      // :9 - device_type
			rand.Intn(5000),     // :10 - response_time_ms
			200,                 // :11 - status_code
			rand.Intn(10000),    // :12 - bytes_transferred
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
