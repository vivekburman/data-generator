package generator

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
)

func MSSQL() {
	username := "sa"
	password := "Mssql@123"
	host := "localhost"
	port := 1433
	database := "source_data_db"

	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
		host, username, password, port, database)

	// Open connection
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
		return
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to database: ", err.Error())
		return
	}
	// Create partitioned table for 1 billion records
	createTable := `
	-- Step 1: Create Partition Function for monthly partitioning
	IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_monthly_timestamp')
	BEGIN
		CREATE PARTITION FUNCTION pf_monthly_timestamp(DATETIMEOFFSET)
		AS RANGE RIGHT FOR VALUES 
		('2024-01-01', '2024-02-01', '2024-03-01', '2024-04-01', 
		 '2024-05-01', '2024-06-01', '2024-07-01', '2024-08-01',
		 '2024-09-01', '2024-10-01', '2024-11-01', '2024-12-01',
		 '2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01',
		 '2025-05-01', '2025-06-01', '2025-07-01', '2025-08-01');
	END;

	-- Step 2: Create Partition Scheme
	IF NOT EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_monthly_timestamp')
	BEGIN
		CREATE PARTITION SCHEME ps_monthly_timestamp
		AS PARTITION pf_monthly_timestamp ALL TO ([PRIMARY]);
	END;

	-- Step 3: Create partitioned table
	IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='user_activity_log' AND xtype='U')
	BEGIN
		CREATE TABLE user_activity_log (
			id BIGINT IDENTITY(1,1),
			user_id BIGINT NOT NULL,
			session_id UNIQUEIDENTIFIER NOT NULL,
			event_type SMALLINT NOT NULL,
			timestamp_utc DATETIMEOFFSET NOT NULL DEFAULT SYSDATETIMEOFFSET(),
			ip_address VARCHAR(45), -- Supports both IPv4 and IPv6
			user_agent_hash BIGINT,
			page_url_hash BIGINT,
			referrer_hash BIGINT,
			country_code CHAR(2),
			device_type SMALLINT,
			response_time_ms INT CHECK (response_time_ms >= 0 AND response_time_ms <= 65535),
			status_code INT CHECK (status_code >= 0 AND status_code <= 65535),
			bytes_transferred BIGINT CHECK (bytes_transferred >= 0),
			
			-- Clustered primary key must include partition key
			CONSTRAINT PK_user_activity_log PRIMARY KEY CLUSTERED (id, timestamp_utc)
		) ON ps_monthly_timestamp(timestamp_utc);
	END;`

	if _, err := db.Exec(createTable); err != nil {
		log.Fatalf("Failed creating table: %v", err)
	}
	insertMSSQL(db)
}

func insertMSSQL(db *sql.DB) {
	totalRecords := 1000000 // Insert 1 million for testing

	stmt, err := db.Prepare(`
		INSERT INTO user_activity_log 
		(user_id, session_id, event_type, timestamp_utc, ip_address, user_agent_hash, 
		 page_url_hash, referrer_hash, country_code, device_type, response_time_ms, status_code, bytes_transferred) 
		VALUES (@p1, NEWID(), @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12)`)
	if err != nil {
		log.Fatalf("prepare failed: %v", err)
	}
	defer stmt.Close()

	// Generate timestamps across different partitions for better distribution
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	timeRange := 365 * 24 * time.Hour // Spread across 2024

	for i := 0; i < totalRecords; i++ {
		// Random timestamp within 2024 to distribute across partitions
		randomDuration := time.Duration(rand.Int63n(int64(timeRange)))
		timestamp := baseTime.Add(randomDuration)

		_, err := stmt.Exec(
			sql.Named("p1", rand.Int63n(100000)), // user_id
			sql.Named("p2", rand.Intn(5)+1),      // event_type
			sql.Named("p3", timestamp),           // timestamp_utc
			sql.Named("p4", fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255))), // ip_address
			sql.Named("p5", rand.Int63()),        // user_agent_hash
			sql.Named("p6", rand.Int63()),        // page_url_hash
			sql.Named("p7", rand.Int63()),        // referrer_hash
			sql.Named("p8", "US"),                // country_code
			sql.Named("p9", rand.Intn(3)+1),      // device_type
			sql.Named("p10", rand.Intn(5000)),    // response_time_ms
			sql.Named("p11", 200),                // status_code
			sql.Named("p12", rand.Int63n(10000)), // bytes_transferred
		)
		if err != nil {
			log.Fatalf("insert failed: %v", err)
		}

		if i%50000 == 0 {
			fmt.Printf("Inserted %d records\n", i)
		}
	}

	fmt.Printf("✅ Completed: %d records inserted into MSSQL user_activity_log\n", totalRecords)
}
func MSQLRelational() {
	username := "sa"
	password := "Mssql@123"
	host := "localhost"
	port := 1433
	database := "source_data_db"

	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
		host, username, password, port, database)
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
		return
	}
	defer db.Close()

	createUsersTableMSSQL(db)
	createSessionsTableMSSQL(db)
	createActivityTableMSSQL(db)
	insertRelationalDataMSSQL(db)
}

func createUsersTableMSSQL(db *sql.DB) {
	createTable := `
 IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
 BEGIN
  CREATE TABLE users (
   id BIGINT IDENTITY(1,1) PRIMARY KEY,
   country_code CHAR(2) NOT NULL,
   created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET()
  );
 END`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func createSessionsTableMSSQL(db *sql.DB) {
	createTable := `
 IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sessions' AND xtype='U')
 BEGIN
  CREATE TABLE sessions (
   id UNIQUEIDENTIFIER PRIMARY KEY DEFAULT NEWID(),
   user_id BIGINT NOT NULL,
   device_type SMALLINT,
   user_agent_hash BIGINT,
   created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
   CONSTRAINT FK_sessions_user FOREIGN KEY (user_id) REFERENCES users(id)
  );
 END`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func createActivityTableMSSQL(db *sql.DB) {
	createTable := `
 IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='activity_events' AND xtype='U')
 BEGIN
  CREATE TABLE activity_events (
   id BIGINT IDENTITY(1,1) PRIMARY KEY,
   user_id BIGINT NOT NULL,
   session_id UNIQUEIDENTIFIER NOT NULL,
   event_type SMALLINT NOT NULL,
   timestamp_utc DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
   ip_address VARCHAR(45),
   page_url_hash BIGINT,
   referrer_hash BIGINT,
   response_time_ms INT,
   status_code INT,
   bytes_transferred BIGINT,
   CONSTRAINT FK_activity_user FOREIGN KEY (user_id) REFERENCES users(id),
   CONSTRAINT FK_activity_session FOREIGN KEY (session_id) REFERENCES sessions(id)
  );
 END`

	_, err := db.Exec(createTable)
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
}

func insertRelationalDataMSSQL(db *sql.DB) {
	totalRecords := 1000000

	userStmt, err := db.Prepare("IF NOT EXISTS (SELECT 1 FROM users WHERE id = @p1) INSERT INTO users (id, country_code) VALUES (@p1, @p2)")
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer userStmt.Close()

	sessionStmt, err := db.Prepare("INSERT INTO sessions (user_id, device_type, user_agent_hash) VALUES (@p1, @p2, @p3)")
	if err != nil {
		log.Fatalf("err: %s\n", err.Error())
	}
	defer sessionStmt.Close()

	activityStmt, err := db.Prepare(`
  INSERT INTO activity_events 
  (user_id, session_id, event_type, timestamp_utc, ip_address, page_url_hash, response_time_ms, status_code, bytes_transferred) 
  VALUES (@p1, (SELECT TOP 1 id FROM sessions WHERE user_id = @p2 ORDER BY NEWID()), @p3, @p4, @p5, @p6, @p7, @p8, @p9)`)
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
		bytesTransferred := rand.Int63n(10000)
		ipAddress := fmt.Sprintf("192.168.%d.%d", rand.Intn(255), rand.Intn(255))
		countryCode := "US"

		userStmt.Exec(sql.Named("p1", userID), sql.Named("p2", countryCode))
		sessionStmt.Exec(sql.Named("p1", userID), sql.Named("p2", deviceType), sql.Named("p3", userAgentHash))
		activityStmt.Exec(
			sql.Named("p1", userID),
			sql.Named("p2", userID),
			sql.Named("p3", eventType),
			sql.Named("p4", timestamp),
			sql.Named("p5", ipAddress),
			sql.Named("p6", pageURLHash),
			sql.Named("p7", responseTime),
			sql.Named("p8", statusCode),
			sql.Named("p9", bytesTransferred),
		)

		if i%50000 == 0 {
			fmt.Printf("Inserted %d relational records\n", i)
		}
	}
	fmt.Println("Completed: 1 million records inserted into relational tables")
}
