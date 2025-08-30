package generator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"strings"
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

// Order represents an e-commerce order
type Order struct {
	OrderID         string      `json:"order_id"`
	CustomerID      string      `json:"customer_id"`
	CustomerEmail   string      `json:"customer_email"`
	OrderDate       time.Time   `json:"order_date"`
	Status          string      `json:"status"`
	TotalAmount     float64     `json:"total_amount"`
	Currency        string      `json:"currency"`
	PaymentMethod   string      `json:"payment_method"`
	ShippingMethod  string      `json:"shipping_method"`
	Items           []OrderItem `json:"items"`
	ShippingAddress Address     `json:"shipping_address"`
	BillingAddress  Address     `json:"billing_address"`
	OrderSource     string      `json:"order_source"`
	DiscountCode    string      `json:"discount_code,omitempty"`
	DiscountAmount  float64     `json:"discount_amount"`
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID   string  `json:"product_id"`
	ProductName string  `json:"product_name"`
	Category    string  `json:"category"`
	Quantity    int     `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	TotalPrice  float64 `json:"total_price"`
	SKU         string  `json:"sku"`
}

// Address represents shipping/billing address
type Address struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	Street    string `json:"street"`
	City      string `json:"city"`
	State     string `json:"state"`
	ZipCode   string `json:"zip_code"`
	Country   string `json:"country"`
	Phone     string `json:"phone"`
}

var (
	// Sample data for generating realistic orders
	firstNames      = []string{"John", "Jane", "Michael", "Sarah", "David", "Lisa", "Robert", "Emily", "James", "Ashley", "William", "Jessica", "Christopher", "Amanda", "Daniel", "Stephanie"}
	lastNames       = []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"}
	cities          = []string{"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville"}
	states          = []string{"NY", "CA", "IL", "TX", "AZ", "PA", "FL", "NC", "OH", "GA", "MI", "WA"}
	countries       = []string{"USA", "Canada", "Mexico", "UK", "Germany", "France", "Italy", "Spain", "Australia", "Japan"}
	productNames    = []string{"Wireless Headphones", "Smartphone", "Laptop Computer", "Running Shoes", "Coffee Maker", "Bluetooth Speaker", "Fitness Tracker", "Tablet", "Gaming Chair", "Backpack", "Sunglasses", "Water Bottle", "Keyboard", "Mouse", "Monitor", "Webcam"}
	categories      = []string{"Electronics", "Clothing", "Home & Garden", "Sports & Outdoors", "Books", "Health & Beauty", "Toys & Games", "Automotive", "Office Supplies", "Kitchen & Dining"}
	orderStatuses   = []string{"pending", "processing", "shipped", "delivered", "cancelled", "refunded"}
	paymentMethods  = []string{"credit_card", "debit_card", "paypal", "apple_pay", "google_pay", "bank_transfer", "cash_on_delivery"}
	shippingMethods = []string{"standard", "express", "overnight", "two_day", "ground", "priority"}
	orderSources    = []string{"website", "mobile_app", "phone", "in_store", "marketplace", "social_media"}
	discountCodes   = []string{"SAVE10", "WELCOME20", "SUMMER15", "FREESHIP", "NEWCUSTOMER", "LOYALTY25", "FLASH30", "WEEKEND10"}
	queueTypes      = []string{
		"new_orders", "payment_processing", "inventory_updates",
		"shipping_notifications", "customer_service",
		"order_validation", "fraud_detection", "tax_calculation",
		"digital_delivery", "express_delivery", "international_shipping",
		"analytics_processing", "refund_processing", "audit_logs",
		"warehouse_fulfillment", "priority_processing", "pickup_orders",
		"discount_processing", "loyalty_points", "marketing_campaigns",
		"bulk_orders", "vendor_notifications", "supplier_updates",
		"stock_alerts", "price_monitoring", "returns_management",
		"subscription_billing", "gift_cards", "third_party_logistics",
		"backorder_processing",
	}
)

func MSSQLECommerceOrderBroker() {
	username := "sa"
	password := "Mssql@123"
	host := "localhost"
	port := 1433
	database := "ecommerce_orders_db"

	// Build connection string
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d;database=%s",
		host, username, password, port, database)

	// Open connection
	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Error creating connection pool: ", err.Error())
	}
	defer db.Close()

	// Test the connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Error connecting to database: ", err.Error())
	}
	fmt.Println("Connected to SQL Server!")

	// Configuration
	numQueues := len(queueTypes)            // 30 different queue types
	totalOrders := 25000 + rand.Intn(25001) // Random between 25K-50K orders

	fmt.Printf("Creating %d e-commerce order queues and generating %d orders\n", numQueues, totalOrders)

	// Ensure Service Broker setup exists
	err = ensureECommerceServiceBrokerSetup(db)
	if err != nil {
		log.Fatal("Setup failed:", err)
	}

	// Generate order messages across multiple queues
	err = sendBulkOrderMessages(db, totalOrders)
	if err != nil {
		log.Fatal("Failed to send order messages:", err)
	}
}

func ensureECommerceServiceBrokerSetup(db *sql.DB) error {
	// Enable Service Broker and create basic components
	setupSQL := `
		-- Enable Service Broker if not already enabled
		IF NOT EXISTS (
			SELECT 1 FROM sys.databases WHERE name = DB_NAME() AND is_broker_enabled = 1
		)
		BEGIN
			ALTER DATABASE [ecommerce_orders_db] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;
		END;

		-- Order Message Types
		IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = '//ECommerce/OrderMessage')
			CREATE MESSAGE TYPE [//ECommerce/OrderMessage] VALIDATION = NONE;

		IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = '//ECommerce/PaymentMessage')
			CREATE MESSAGE TYPE [//ECommerce/PaymentMessage] VALIDATION = NONE;

		IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = '//ECommerce/InventoryMessage')
			CREATE MESSAGE TYPE [//ECommerce/InventoryMessage] VALIDATION = NONE;

		IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = '//ECommerce/ShippingMessage')
			CREATE MESSAGE TYPE [//ECommerce/ShippingMessage] VALIDATION = NONE;

		IF NOT EXISTS (SELECT * FROM sys.service_message_types WHERE name = '//ECommerce/CustomerServiceMessage')
			CREATE MESSAGE TYPE [//ECommerce/CustomerServiceMessage] VALIDATION = NONE;

		-- Contracts
		IF NOT EXISTS (SELECT * FROM sys.service_contracts WHERE name = '//ECommerce/OrderContract')
			CREATE CONTRACT [//ECommerce/OrderContract]
			([//ECommerce/OrderMessage] SENT BY INITIATOR,
			 [//ECommerce/PaymentMessage] SENT BY INITIATOR,
			 [//ECommerce/InventoryMessage] SENT BY INITIATOR,
			 [//ECommerce/ShippingMessage] SENT BY INITIATOR,
			 [//ECommerce/CustomerServiceMessage] SENT BY INITIATOR);
	`

	_, err := db.Exec(setupSQL)
	if err != nil {
		return fmt.Errorf("failed to setup basic Service Broker components: %w", err)
	}

	// Create queues and services for each order processing type
	for _, queueType := range queueTypes {
		queueSQL := fmt.Sprintf(`
			-- %s Queues
			IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '%s_initiator_queue')
				CREATE QUEUE %s_initiator_queue;

			IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '%s_target_queue')
				CREATE QUEUE %s_target_queue;

			-- %s Services
			IF NOT EXISTS (SELECT * FROM sys.services WHERE name = '%s_initiator_service')
				CREATE SERVICE %s_initiator_service
				ON QUEUE %s_initiator_queue
				([//ECommerce/OrderContract]);

			IF NOT EXISTS (SELECT * FROM sys.services WHERE name = '%s_target_service')
				CREATE SERVICE %s_target_service
				ON QUEUE %s_target_queue
				([//ECommerce/OrderContract]);
		`, queueType, queueType, queueType, queueType, queueType, queueType, queueType, queueType, queueType, queueType, queueType, queueType)

		_, err = db.Exec(queueSQL)
		if err != nil {
			return fmt.Errorf("failed to create %s queue/service: %w", queueType, err)
		}
	}

	fmt.Printf("✅ Successfully created %d e-commerce order processing queues\n", len(queueTypes))
	return nil
}

type QueueConversation struct {
	Handle      string
	QueueType   string
	MessageType string
}

func generateRandomOrder(orderNum int) Order {
	rand.Seed(time.Now().UnixNano() + int64(orderNum))

	// Generate customer info
	firstName := firstNames[rand.Intn(len(firstNames))]
	lastName := lastNames[rand.Intn(len(lastNames))]

	// Generate order items (1-5 items per order)
	numItems := rand.Intn(5) + 1
	items := make([]OrderItem, numItems)
	totalAmount := 0.0

	for i := 0; i < numItems; i++ {
		productName := productNames[rand.Intn(len(productNames))]
		category := categories[rand.Intn(len(categories))]
		quantity := rand.Intn(3) + 1
		unitPrice := float64(rand.Intn(500)+10) + rand.Float64()
		totalPrice := float64(quantity) * unitPrice

		items[i] = OrderItem{
			ProductID:   fmt.Sprintf("PROD-%06d", rand.Intn(100000)),
			ProductName: productName,
			Category:    category,
			Quantity:    quantity,
			UnitPrice:   unitPrice,
			TotalPrice:  totalPrice,
			SKU:         fmt.Sprintf("SKU-%s-%06d", category[:3], rand.Intn(100000)),
		}
		totalAmount += totalPrice
	}

	// Generate addresses
	shippingAddr := Address{
		FirstName: firstName,
		LastName:  lastName,
		Street:    fmt.Sprintf("%d %s St", rand.Intn(9999)+1, []string{"Main", "Oak", "Pine", "Maple", "Cedar", "Elm"}[rand.Intn(6)]),
		City:      cities[rand.Intn(len(cities))],
		State:     states[rand.Intn(len(states))],
		ZipCode:   fmt.Sprintf("%05d", rand.Intn(99999)),
		Country:   countries[rand.Intn(len(countries))],
		Phone:     fmt.Sprintf("+1-%03d-%03d-%04d", rand.Intn(999)+100, rand.Intn(999)+100, rand.Intn(9999)),
	}

	// Billing address (80% same as shipping, 20% different)
	var billingAddr Address
	if rand.Float32() < 0.8 {
		billingAddr = shippingAddr
	} else {
		billingAddr = Address{
			FirstName: firstName,
			LastName:  lastName,
			Street:    fmt.Sprintf("%d %s Ave", rand.Intn(9999)+1, []string{"First", "Second", "Third", "Broadway", "Park"}[rand.Intn(5)]),
			City:      cities[rand.Intn(len(cities))],
			State:     states[rand.Intn(len(states))],
			ZipCode:   fmt.Sprintf("%05d", rand.Intn(99999)),
			Country:   countries[rand.Intn(len(countries))],
			Phone:     fmt.Sprintf("+1-%03d-%03d-%04d", rand.Intn(999)+100, rand.Intn(999)+100, rand.Intn(9999)),
		}
	}

	// Calculate discount
	var discountCode string
	discountAmount := 0.0
	if rand.Float32() < 0.3 { // 30% of orders have discount
		discountCode = discountCodes[rand.Intn(len(discountCodes))]
		discountAmount = totalAmount * (float64(rand.Intn(25)+5) / 100.0) // 5-30% discount
	}

	finalAmount := totalAmount - discountAmount

	return Order{
		OrderID:         fmt.Sprintf("ORD-%d-%06d", time.Now().Year(), orderNum),
		CustomerID:      fmt.Sprintf("CUST-%08d", rand.Intn(1000000)),
		CustomerEmail:   fmt.Sprintf("%s.%s@%s", firstName, lastName, []string{"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}[rand.Intn(4)]),
		OrderDate:       time.Now().Add(-time.Duration(rand.Intn(365*24)) * time.Hour),
		Status:          orderStatuses[rand.Intn(len(orderStatuses))],
		TotalAmount:     finalAmount,
		Currency:        "USD",
		PaymentMethod:   paymentMethods[rand.Intn(len(paymentMethods))],
		ShippingMethod:  shippingMethods[rand.Intn(len(shippingMethods))],
		Items:           items,
		ShippingAddress: shippingAddr,
		BillingAddress:  billingAddr,
		OrderSource:     orderSources[rand.Intn(len(orderSources))],
		DiscountCode:    discountCode,
		DiscountAmount:  discountAmount,
	}
}
func selectQueueForOrder(conversations []QueueConversation, order Order) QueueConversation {
	weights := make(map[string]int)
	// Initialize all queues with base weight
	for _, conv := range conversations {
		weights[conv.QueueType] = 1
	}
	// Add bonus weights based on order characteristics
	switch order.Status {
	case "pending":
		weights["new_orders"] += 3
		weights["order_validation"] += 2
		weights["fraud_detection"] += 1
	case "processing":
		weights["payment_processing"] += 2
		weights["inventory_updates"] += 2
		weights["warehouse_fulfillment"] += 1
	case "shipped":
		weights["shipping_notifications"] += 3
		weights["analytics_processing"] += 1
	case "delivered":
		weights["analytics_processing"] += 2
		weights["customer_service"] += 1
	case "cancelled":
		weights["refund_processing"] += 2
		weights["customer_service"] += 1
		weights["analytics_processing"] += 1
	case "refunded":
		weights["refund_processing"] += 3
		weights["audit_logs"] += 1
	}
	// Payment method weights
	if order.PaymentMethod == "credit_card" || order.PaymentMethod == "debit_card" {
		weights["payment_processing"] += 1
		weights["fraud_detection"] += 1
	}
	// Shipping method weights
	if order.ShippingMethod == "express" || order.ShippingMethod == "overnight" {
		weights["express_delivery"] += 2
		weights["priority_processing"] += 1
	}
	// International orders
	if order.ShippingAddress.Country != "USA" {
		weights["international_shipping"] += 2
		weights["tax_calculation"] += 1
	}
	// High value orders
	if order.TotalAmount > 500 {
		weights["bulk_orders"] += 1
		weights["fraud_detection"] += 1
		weights["audit_logs"] += 1
	}
	// Discount orders
	if order.DiscountCode != "" {
		weights["discount_processing"] += 2
		weights["marketing_campaigns"] += 1
	}
	// Multiple items
	if len(order.Items) > 2 {
		weights["inventory_updates"] += 1
		weights["warehouse_fulfillment"] += 1
	}
	// Category-based routing
	for _, item := range order.Items {
		switch item.Category {
		case "Electronics":
			weights["vendor_notifications"] += 1
			weights["stock_alerts"] += 1
		case "Clothing":
			weights["returns_management"] += 1
			weights["supplier_updates"] += 1
		}
	}
	// Order source weights
	switch order.OrderSource {
	case "mobile_app":
		weights["loyalty_points"] += 1
		weights["digital_delivery"] += 1
	case "marketplace":
		weights["third_party_logistics"] += 1
		weights["vendor_notifications"] += 1
	case "in_store":
		weights["pickup_orders"] += 2
	}
	// Special product handling
	if hasGiftCard(order) {
		weights["gift_cards"] += 3
	}
	if isSubscriptionOrder(order) {
		weights["subscription_billing"] += 3
	}
	// Add randomness to ensure distribution across unused queues
	unusedQueues := []string{
		"price_monitoring", "backorder_processing", "tax_calculation",
		"digital_delivery", "priority_processing", "analytics_processing",
	}
	// Randomly boost unused queues
	if rand.Float32() < 0.1 { // 10% chance
		randomQueue := unusedQueues[rand.Intn(len(unusedQueues))]
		weights[randomQueue] += 5
	}
	// Calculate total weight
	totalWeight := 0
	for _, weight := range weights {
		totalWeight += weight
	}
	// Select weighted random queue
	randomValue := rand.Intn(totalWeight)
	currentSum := 0
	for _, conv := range conversations {
		currentSum += weights[conv.QueueType]
		if currentSum > randomValue {
			return conv
		}
	}
	// Fallback to random
	return conversations[rand.Intn(len(conversations))]
}
func hasGiftCard(order Order) bool {
	for _, item := range order.Items {
		if strings.Contains(strings.ToLower(item.ProductName), "gift") {
			return true
		}
	}
	return false
}

func isSubscriptionOrder(order Order) bool {
	return strings.Contains(strings.ToLower(order.OrderSource), "subscription") ||
		order.PaymentMethod == "bank_transfer"
}

func sendBulkOrderMessages(db *sql.DB, totalOrders int) error {
	rand.Seed(time.Now().UnixNano())

	// Start dialogs for each queue type
	conversations := make([]QueueConversation, 0, len(queueTypes))

	// Message type mapping (existing code)
	messageTypeMapping := map[string]string{
		"new_orders":             "//ECommerce/OrderMessage",
		"payment_processing":     "//ECommerce/PaymentMessage",
		"inventory_updates":      "//ECommerce/InventoryMessage",
		"shipping_notifications": "//ECommerce/ShippingMessage",
		"customer_service":       "//ECommerce/CustomerServiceMessage",
		"order_validation":       "//ECommerce/OrderMessage",
		"fraud_detection":        "//ECommerce/PaymentMessage",
		"tax_calculation":        "//ECommerce/PaymentMessage",
		"digital_delivery":       "//ECommerce/ShippingMessage",
		"express_delivery":       "//ECommerce/ShippingMessage",
		"international_shipping": "//ECommerce/ShippingMessage",
		"analytics_processing":   "//ECommerce/OrderMessage",
		"refund_processing":      "//ECommerce/PaymentMessage",
		"audit_logs":             "//ECommerce/OrderMessage",
		"warehouse_fulfillment":  "//ECommerce/InventoryMessage",
		"priority_processing":    "//ECommerce/OrderMessage",
		"pickup_orders":          "//ECommerce/ShippingMessage",
		"discount_processing":    "//ECommerce/PaymentMessage",
		"loyalty_points":         "//ECommerce/CustomerServiceMessage",
		"marketing_campaigns":    "//ECommerce/CustomerServiceMessage",
		"bulk_orders":            "//ECommerce/OrderMessage",
		"vendor_notifications":   "//ECommerce/InventoryMessage",
		"supplier_updates":       "//ECommerce/InventoryMessage",
		"stock_alerts":           "//ECommerce/InventoryMessage",
		"price_monitoring":       "//ECommerce/InventoryMessage",
		"returns_management":     "//ECommerce/CustomerServiceMessage",
		"subscription_billing":   "//ECommerce/PaymentMessage",
		"gift_cards":             "//ECommerce/PaymentMessage",
		"third_party_logistics":  "//ECommerce/ShippingMessage",
		"backorder_processing":   "//ECommerce/InventoryMessage",
	}

	for _, queueType := range queueTypes {
		messageType, exists := messageTypeMapping[queueType]
		if !exists {
			return fmt.Errorf("no message type mapping found for queue type: %s", queueType)
		}

		startDialog := fmt.Sprintf(`
            DECLARE @ch UNIQUEIDENTIFIER;
            BEGIN DIALOG CONVERSATION @ch
                FROM SERVICE [%s_initiator_service]
                TO SERVICE '%s_target_service'
                ON CONTRACT [//ECommerce/OrderContract]
                WITH ENCRYPTION = OFF;
            SELECT CONVERT(VARCHAR(36), @ch);
        `, queueType, queueType)

		var conversationHandle string
		err := db.QueryRow(startDialog).Scan(&conversationHandle)
		if err != nil {
			return fmt.Errorf("failed to start dialog for %s queue: %w", queueType, err)
		}

		conversationHandle = strings.TrimSpace(conversationHandle)

		if len(conversationHandle) != 36 {
			return fmt.Errorf("invalid conversation handle format for %s: '%s'", queueType, conversationHandle)
		}

		conversations = append(conversations, QueueConversation{
			Handle:      conversationHandle,
			QueueType:   queueType,
			MessageType: messageType,
		})
	}

	fmt.Printf("✅ Started %d conversations for order processing\n", len(conversations))

	// Track orders per queue for reporting
	ordersPerQueue := make(map[string]int)
	for _, queueType := range queueTypes {
		ordersPerQueue[queueType] = 0
	}

	// Send orders in batches
	batchSize := 1000
	for i := 1; i <= totalOrders; i++ {
		// Generate realistic order data
		order := generateRandomOrder(i)

		// Select appropriate queue based on order characteristics
		selectedConv := selectQueueForOrder(conversations, order)

		// Convert order to JSON
		orderJSON, err := json.Marshal(order)
		if err != nil {
			return fmt.Errorf("failed to marshal order %d: %w", i, err)
		}

		// THE FIX: Use parameterized query with proper binary handling
		err = sendOrderMessage(db, selectedConv.Handle, selectedConv.MessageType, orderJSON)
		if err != nil {
			return fmt.Errorf("failed to send order %d to %s queue: %w",
				i, selectedConv.QueueType, err)
		}

		// Track order count per queue
		ordersPerQueue[selectedConv.QueueType]++

		// Progress reporting
		if i%batchSize == 0 {
			fmt.Printf("📦 Processed %d/%d orders...\n", i, totalOrders)
		}
	}

	// Report distribution (existing code)
	fmt.Printf("\n✅ Successfully sent %d e-commerce orders\n", totalOrders)
	fmt.Println("\n📊 Order Distribution by Queue Type:")
	fmt.Println("Queue Type\t\t\tOrders\t\tPercentage")
	fmt.Println("----------\t\t\t------\t\t----------")

	totalDistributed := 0
	queuesUsed := 0
	for _, queueType := range queueTypes {
		count := ordersPerQueue[queueType]
		percentage := float64(count) / float64(totalOrders) * 100
		fmt.Printf("%-25s\t%d\t\t%.1f%%\n", queueType, count, percentage)
		totalDistributed += count
		if count > 0 {
			queuesUsed++
		}
	}

	fmt.Printf("\nSummary: %d queues used out of %d total queues\n", queuesUsed, len(queueTypes))
	fmt.Printf("Total orders distributed: %d\n", totalDistributed)
	for _, handle := range conversations {
		_, err := db.ExecContext(context.Background(), `
			END CONVERSATION @handle
		`,
			sql.Named("handle", handle.Handle),
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// THE BEST FIX: Dedicated function for sending messages safely
func sendOrderMessage(db *sql.DB, conversationHandle, messageType string, jsonData []byte) error {
	// Method 1: Try with binary parameter (most reliable)
	sendSQL := `
		SEND ON CONVERSATION @handle
		MESSAGE TYPE @msgtype
		(@jsondata);
	`

	// Prepare statement with parameters
	stmt, err := db.Prepare(sendSQL)
	if err != nil {
		// Fallback method if parameterized queries aren't supported
		return sendOrderMessageFallback(db, conversationHandle, messageType, jsonData)
	}
	defer stmt.Close()

	// Execute with parameters - this prevents all encoding issues
	_, err = stmt.Exec(
		sql.Named("handle", conversationHandle),
		sql.Named("msgtype", messageType),
		sql.Named("jsondata", jsonData), // Pass as []byte directly
	)

	if err != nil {
		// If parameterized method fails, try fallback
		return sendOrderMessageFallback(db, conversationHandle, messageType, jsonData)
	}

	return nil
}

// Fallback method using string manipulation
func sendOrderMessageFallback(db *sql.DB, conversationHandle, messageType string, jsonData []byte) error {
	// Clean the JSON data
	cleanJSON := cleanJSONForSQL(string(jsonData))

	// Validate the cleaned JSON
	var testObj interface{}
	if err := json.Unmarshal([]byte(cleanJSON), &testObj); err != nil {
		return fmt.Errorf("JSON became invalid after cleaning: %w", err)
	}

	// Use VARBINARY to avoid string encoding issues
	sendMessage := fmt.Sprintf(`
		DECLARE @conversation_handle UNIQUEIDENTIFIER = '%s';
		DECLARE @message_type SYSNAME = N'%s';
		DECLARE @json_data NVARCHAR(MAX) = N'%s';
		DECLARE @binary_data VARBINARY(MAX) = CAST(@json_data AS VARBINARY(MAX));
		
		SEND ON CONVERSATION @conversation_handle
		MESSAGE TYPE @message_type
		(@binary_data);
	`, conversationHandle, messageType, cleanJSON)

	_, err := db.Exec(sendMessage)
	return err
}

// Enhanced JSON cleaning function
func cleanJSONForSQL(jsonStr string) string {
	// Step 1: Remove null bytes
	cleaned := strings.ReplaceAll(jsonStr, "\x00", "")

	// Step 2: Escape single quotes for SQL
	cleaned = strings.ReplaceAll(cleaned, "'", "''")

	// Step 3: Remove other problematic control characters
	var result strings.Builder
	result.Grow(len(cleaned)) // Pre-allocate for efficiency

	for _, r := range cleaned {
		// Keep printable characters and essential whitespace
		if r >= 32 || r == '\t' || r == '\n' || r == '\r' {
			result.WriteRune(r)
		}
		// Skip other control characters
	}

	return result.String()
}
