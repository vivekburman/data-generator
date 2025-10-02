package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

// Single index for all tenants - optimized for analytics
const (
	IndexTransactions = "transaction_analytics"
	IndexCustomers    = "customer_360"
)

// ============================================================================
// INDEX 1: TRANSACTION_ANALYTICS (Main Analytics Index - 100M+ docs)
// ============================================================================
// Combines: transactions + accounts + customers + transaction_legs + metadata
// Purpose: Transaction-level analytics, fraud detection, customer behavior

type TransactionAnalytics struct {
	// ===== Core Transaction Data =====
	TenantID        int64     `json:"tenant_id"`
	TransactionID   int64     `json:"transaction_id"`
	TransactionRef  string    `json:"transaction_ref"`
	TransactionType string    `json:"transaction_type"`
	Amount          float64   `json:"amount"`
	CurrencyCode    string    `json:"currency_code"`
	Status          string    `json:"status"`
	Description     string    `json:"description"`
	TransactionDate time.Time `json:"transaction_date"`
	ValueDate       time.Time `json:"value_date,omitempty"`
	CreatedAt       time.Time `json:"created_at"`

	// ===== FROM Account (Source Account) =====
	FromAccountID     int64   `json:"from_account_id"`
	FromAccountNumber string  `json:"from_account_number"`
	FromAccountType   string  `json:"from_account_type"`
	FromAccountStatus string  `json:"from_account_status"`
	FromCurrencyCode  string  `json:"from_currency_code"`
	FromBalanceBefore float64 `json:"from_balance_before"`
	FromBalanceAfter  float64 `json:"from_balance_after"`

	// ===== FROM Customer (Source Customer) =====
	FromCustomerID      int64  `json:"from_customer_id"`
	FromCustomerCode    string `json:"from_customer_code"`
	FromCustomerName    string `json:"from_customer_name"` // full_name for search
	FromCustomerEmail   string `json:"from_customer_email,omitempty"`
	FromCustomerStatus  string `json:"from_customer_status"`
	FromCustomerCountry string `json:"from_customer_country,omitempty"`
	FromCustomerSegment string `json:"from_customer_segment,omitempty"` // retail, premium, business

	// ===== TO Account (Destination Account) =====
	ToAccountID     int64   `json:"to_account_id"`
	ToAccountNumber string  `json:"to_account_number"`
	ToAccountType   string  `json:"to_account_type"`
	ToAccountStatus string  `json:"to_account_status"`
	ToCurrencyCode  string  `json:"to_currency_code"`
	ToBalanceBefore float64 `json:"to_balance_before"`
	ToBalanceAfter  float64 `json:"to_balance_after"`

	// ===== TO Customer (Destination Customer) =====
	ToCustomerID      int64  `json:"to_customer_id"`
	ToCustomerCode    string `json:"to_customer_code"`
	ToCustomerName    string `json:"to_customer_name"`
	ToCustomerEmail   string `json:"to_customer_email,omitempty"`
	ToCustomerStatus  string `json:"to_customer_status"`
	ToCustomerCountry string `json:"to_customer_country,omitempty"`
	ToCustomerSegment string `json:"to_customer_segment,omitempty"`

	// ===== Merchant Data (if applicable) =====
	MerchantName     string `json:"merchant_name,omitempty"`
	MerchantCategory string `json:"merchant_category,omitempty"`
	MerchantCountry  string `json:"merchant_country,omitempty"`

	// ===== Derived Analytics Fields =====
	DayOfWeek       string `json:"day_of_week"` // Monday, Tuesday...
	HourOfDay       int    `json:"hour_of_day"` // 0-23
	IsWeekend       bool   `json:"is_weekend"`
	IsBusinessHours bool   `json:"is_business_hours"` // 9AM-5PM
	MonthName       string `json:"month_name"`        // January, February...
	Quarter         string `json:"quarter"`           // Q1, Q2, Q3, Q4
	Year            int    `json:"year"`

	// ===== Transaction Classification =====
	AmountBucket    string  `json:"amount_bucket"`           // micro, small, medium, large, xlarge
	TransactionFlow string  `json:"transaction_flow"`        // internal, external, cross_border
	RiskScore       float64 `json:"risk_score,omitempty"`    // 0-100 fraud risk
	RiskCategory    string  `json:"risk_category,omitempty"` // low, medium, high

	// ===== Metadata (from transaction_metadata table) =====
	IPAddress string            `json:"ip_address,omitempty"`
	UserAgent string            `json:"user_agent,omitempty"`
	DeviceID  string            `json:"device_id,omitempty"`
	Location  GeoPoint          `json:"location,omitempty"` // lat/lon
	Metadata  map[string]string `json:"metadata,omitempty"` // flexible key-value

	// ===== Flags for Analytics =====
	IsFirstTransaction bool `json:"is_first_transaction"` // Customer's first txn
	IsRecurring        bool `json:"is_recurring"`         // Recurring payment
	IsReversed         bool `json:"is_reversed"`          // Was this reversed?
	IsCrossCurrency    bool `json:"is_cross_currency"`    // Different currencies
	IsHighValue        bool `json:"is_high_value"`        // Amount > threshold
}

type GeoPoint struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// ============================================================================
// INDEX 2: CUSTOMER_360 (Customer Profile + Aggregated Stats)
// ============================================================================
// Combines: customers + accounts + KYC + addresses + aggregated transaction stats
// Purpose: Customer profiling, segmentation, 360-degree view

type Customer360 struct {
	// ===== Core Customer Data =====
	TenantID     int64     `json:"tenant_id"`
	CustomerID   int64     `json:"customer_id"`
	CustomerCode string    `json:"customer_code"`
	FirstName    string    `json:"first_name"`
	LastName     string    `json:"last_name"`
	FullName     string    `json:"full_name"` // For full-text search
	Email        string    `json:"email"`
	Phone        string    `json:"phone"`
	DateOfBirth  time.Time `json:"date_of_birth,omitempty"`
	Nationality  string    `json:"nationality,omitempty"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`

	// ===== Customer Segmentation =====
	Segment          string    `json:"segment"`        // retail, premium, business, enterprise
	LifetimeValue    float64   `json:"lifetime_value"` // Total value
	RiskRating       string    `json:"risk_rating"`    // low, medium, high
	CreditScore      int       `json:"credit_score,omitempty"`
	CustomerSince    time.Time `json:"customer_since"`
	DaysSinceJoining int       `json:"days_since_joining"`

	// ===== KYC Data (from customer_kyc) =====
	KYCStatus        string    `json:"kyc_status"` // verified, pending, failed
	KYCVerifiedAt    time.Time `json:"kyc_verified_at,omitempty"`
	DocumentType     string    `json:"document_type,omitempty"`
	DocumentVerified bool      `json:"document_verified"`

	// ===== Addresses (from customer_addresses) =====
	PrimaryAddress Address `json:"primary_address,omitempty"`
	MailingAddress Address `json:"mailing_address,omitempty"`

	// ===== Accounts (denormalized array) =====
	Accounts []CustomerAccount `json:"accounts"` // All accounts for this customer

	// ===== Aggregated Transaction Stats (pre-computed) =====
	TotalTransactions        int64     `json:"total_transactions"`
	TotalTransactionVolume   float64   `json:"total_transaction_volume"`
	AvgTransactionAmount     float64   `json:"avg_transaction_amount"`
	LastTransactionDate      time.Time `json:"last_transaction_date,omitempty"`
	DaysSinceLastTransaction int       `json:"days_since_last_transaction"`

	// Last 30 days
	Transactions30d int64   `json:"transactions_30d"`
	Volume30d       float64 `json:"volume_30d"`

	// Last 90 days
	Transactions90d int64   `json:"transactions_90d"`
	Volume90d       float64 `json:"volume_90d"`

	// Last 365 days
	Transactions365d int64   `json:"transactions_365d"`
	Volume365d       float64 `json:"volume_365d"`

	// ===== Behavior Patterns =====
	PreferredTransactionType string   `json:"preferred_transaction_type"` // Most common
	PreferredTransactionTime string   `json:"preferred_transaction_time"` // morning, afternoon, evening
	TopMerchantCategories    []string `json:"top_merchant_categories"`    // Top 5

	// ===== Flags =====
	IsActive            bool `json:"is_active"`
	IsDormant           bool `json:"is_dormant"`             // No txn in 90 days
	IsHighValueCustomer bool `json:"is_high_value_customer"` // Top 10%
	HasMultipleAccounts bool `json:"has_multiple_accounts"`
	HasLoan             bool `json:"has_loan"`
	HasCard             bool `json:"has_card"`
}

type Address struct {
	AddressLine1 string `json:"address_line1,omitempty"`
	AddressLine2 string `json:"address_line2,omitempty"`
	City         string `json:"city,omitempty"`
	State        string `json:"state,omitempty"`
	PostalCode   string `json:"postal_code,omitempty"`
	Country      string `json:"country,omitempty"`
}

type CustomerAccount struct {
	AccountID        int64     `json:"account_id"`
	AccountNumber    string    `json:"account_number"`
	AccountType      string    `json:"account_type"`
	CurrencyCode     string    `json:"currency_code"`
	Status           string    `json:"status"`
	CurrentBalance   float64   `json:"current_balance"`
	AvailableBalance float64   `json:"available_balance"`
	OpenedDate       time.Time `json:"opened_date"`
}

// ============================================================================
// INDEX 3: ACCOUNT_ANALYTICS (Account-level aggregated data)
// ============================================================================
// Combines: accounts + customers + balance snapshots + transaction stats
// Purpose: Account performance, balance trends, account health

type AccountAnalytics struct {
	// ===== Core Account Data =====
	TenantID      int64     `json:"tenant_id"`
	AccountID     int64     `json:"account_id"`
	AccountNumber string    `json:"account_number"`
	AccountType   string    `json:"account_type"`
	CurrencyCode  string    `json:"currency_code"`
	Status        string    `json:"status"`
	OpenedDate    time.Time `json:"opened_date"`
	ClosedDate    time.Time `json:"closed_date,omitempty"`
	CreatedAt     time.Time `json:"created_at"`

	// ===== Primary Customer (from account_holders) =====
	PrimaryCustomerID    int64  `json:"primary_customer_id"`
	PrimaryCustomerName  string `json:"primary_customer_name"`
	PrimaryCustomerEmail string `json:"primary_customer_email,omitempty"`
	CustomerSegment      string `json:"customer_segment"`

	// ===== Joint Account Holders =====
	JointHolders   []AccountHolder `json:"joint_holders,omitempty"`
	IsJointAccount bool            `json:"is_joint_account"`

	// ===== Current Balances =====
	CurrentBalance   float64   `json:"current_balance"`
	AvailableBalance float64   `json:"available_balance"`
	HoldBalance      float64   `json:"hold_balance"`
	BalanceUpdatedAt time.Time `json:"balance_updated_at"`

	// ===== Balance Trends (derived) =====
	MinBalance30d    float64 `json:"min_balance_30d"`
	MaxBalance30d    float64 `json:"max_balance_30d"`
	AvgBalance30d    float64 `json:"avg_balance_30d"`
	BalanceChange30d float64 `json:"balance_change_30d"` // Net change

	MinBalance90d float64 `json:"min_balance_90d"`
	MaxBalance90d float64 `json:"max_balance_90d"`
	AvgBalance90d float64 `json:"avg_balance_90d"`

	// ===== Transaction Stats =====
	TotalInflow  float64 `json:"total_inflow"`  // All credits
	TotalOutflow float64 `json:"total_outflow"` // All debits
	NetFlow      float64 `json:"net_flow"`      // inflow - outflow

	TotalTransactions int64 `json:"total_transactions"`
	CreditsCount      int64 `json:"credits_count"`
	DebitsCount       int64 `json:"debits_count"`

	LastTransactionDate time.Time `json:"last_transaction_date,omitempty"`
	DaysSinceLastTxn    int       `json:"days_since_last_txn"`

	// Last 30 days
	Inflow30d       float64 `json:"inflow_30d"`
	Outflow30d      float64 `json:"outflow_30d"`
	Transactions30d int64   `json:"transactions_30d"`

	// Last 90 days
	Inflow90d       float64 `json:"inflow_90d"`
	Outflow90d      float64 `json:"outflow_90d"`
	Transactions90d int64   `json:"transactions_90d"`

	// ===== Account Health Indicators =====
	AccountAge    int     `json:"account_age_days"`
	IsActive      bool    `json:"is_active"`
	IsDormant     bool    `json:"is_dormant"`
	IsOverdrawn   bool    `json:"is_overdrawn"`
	DaysOverdrawn int     `json:"days_overdrawn"`
	HealthScore   float64 `json:"health_score"` // 0-100

	// ===== Linked Products =====
	HasCard       bool    `json:"has_card"`
	HasLoan       bool    `json:"has_loan"`
	LinkedCardIDs []int64 `json:"linked_card_ids,omitempty"`
	LinkedLoanIDs []int64 `json:"linked_loan_ids,omitempty"`
}

type AccountHolder struct {
	CustomerID   int64  `json:"customer_id"`
	CustomerName string `json:"customer_name"`
	HolderType   string `json:"holder_type"` // primary, joint, authorized
}

func GetTransactionAnalyticsMapping() map[string]interface{} {
	return map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   12, // 100M docs / 8M per shard
			"number_of_replicas": 2,
			"refresh_interval":   "30s",
			"index": map[string]interface{}{
				"sort.field": []string{"tenant_id", "transaction_date"},
				"sort.order": []string{"asc", "desc"},
				"codec":      "best_compression",
			},
			"analysis": map[string]interface{}{
				"analyzer": map[string]interface{}{
					"customer_name_analyzer": map[string]interface{}{
						"type":      "custom",
						"tokenizer": "standard",
						"filter":    []string{"lowercase", "asciifolding"},
					},
				},
			},
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"tenant_id":        keywordField(),
				"transaction_id":   longField(),
				"transaction_ref":  keywordField(),
				"transaction_type": keywordField(),
				"amount":           scaledFloatField(100),
				"currency_code":    keywordField(),
				"status":           keywordField(),
				"description":      textWithKeywordField(),
				"transaction_date": dateField(),
				"value_date":       dateField(),
				"created_at":       dateField(),

				// FROM fields
				"from_account_id":     longField(),
				"from_account_number": keywordField(),
				"from_account_type":   keywordField(),
				"from_account_status": keywordField(),
				"from_currency_code":  keywordField(),
				"from_balance_before": scaledFloatField(100),
				"from_balance_after":  scaledFloatField(100),

				"from_customer_id":      longField(),
				"from_customer_code":    keywordField(),
				"from_customer_name":    textWithAnalyzer("customer_name_analyzer"),
				"from_customer_email":   keywordField(),
				"from_customer_status":  keywordField(),
				"from_customer_country": keywordField(),
				"from_customer_segment": keywordField(),

				// TO fields
				"to_account_id":     longField(),
				"to_account_number": keywordField(),
				"to_account_type":   keywordField(),
				"to_account_status": keywordField(),
				"to_currency_code":  keywordField(),
				"to_balance_before": scaledFloatField(100),
				"to_balance_after":  scaledFloatField(100),

				"to_customer_id":      longField(),
				"to_customer_code":    keywordField(),
				"to_customer_name":    textWithAnalyzer("customer_name_analyzer"),
				"to_customer_email":   keywordField(),
				"to_customer_status":  keywordField(),
				"to_customer_country": keywordField(),
				"to_customer_segment": keywordField(),

				// Merchant
				"merchant_name":     keywordField(),
				"merchant_category": keywordField(),
				"merchant_country":  keywordField(),

				// Derived fields
				"day_of_week":       keywordField(),
				"hour_of_day":       byteField(),
				"is_weekend":        booleanField(),
				"is_business_hours": booleanField(),
				"month_name":        keywordField(),
				"quarter":           keywordField(),
				"year":              shortField(),

				// Classification
				"amount_bucket":    keywordField(),
				"transaction_flow": keywordField(),
				"risk_score":       floatField(),
				"risk_category":    keywordField(),

				// Metadata
				"ip_address": ipField(),
				"user_agent": keywordField(),
				"device_id":  keywordField(),
				"location":   geoPointField(),
				"metadata":   objectField(),

				// Flags
				"is_first_transaction": booleanField(),
				"is_recurring":         booleanField(),
				"is_reversed":          booleanField(),
				"is_cross_currency":    booleanField(),
				"is_high_value":        booleanField(),
			},
		},
	}
}

func GetCustomer360Mapping() map[string]interface{} {
	return map[string]interface{}{
		"settings": map[string]interface{}{
			"number_of_shards":   2, // Smaller dataset
			"number_of_replicas": 2,
			"refresh_interval":   "60s",
		},
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"tenant_id":     keywordField(),
				"customer_id":   longField(),
				"customer_code": keywordField(),
				"first_name":    textWithKeywordField(),
				"last_name":     textWithKeywordField(),
				"full_name":     textWithAnalyzer("standard"),
				"email":         keywordField(),
				"phone":         keywordField(),
				"date_of_birth": dateField(),
				"nationality":   keywordField(),
				"status":        keywordField(),
				"created_at":    dateField(),
				"updated_at":    dateField(),

				// Segmentation
				"segment":            keywordField(),
				"lifetime_value":     scaledFloatField(100),
				"risk_rating":        keywordField(),
				"credit_score":       shortField(),
				"customer_since":     dateField(),
				"days_since_joining": integerField(),

				// KYC
				"kyc_status":        keywordField(),
				"kyc_verified_at":   dateField(),
				"document_type":     keywordField(),
				"document_verified": booleanField(),

				// Nested addresses
				"primary_address": nestedAddressField(),
				"mailing_address": nestedAddressField(),

				// Nested accounts array
				"accounts": nestedField(map[string]interface{}{
					"account_id":        longField(),
					"account_number":    keywordField(),
					"account_type":      keywordField(),
					"currency_code":     keywordField(),
					"status":            keywordField(),
					"current_balance":   scaledFloatField(100),
					"available_balance": scaledFloatField(100),
					"opened_date":       dateField(),
				}),

				// Transaction stats
				"total_transactions":          longField(),
				"total_transaction_volume":    scaledFloatField(100),
				"avg_transaction_amount":      scaledFloatField(100),
				"last_transaction_date":       dateField(),
				"days_since_last_transaction": integerField(),

				"transactions_30d":  longField(),
				"volume_30d":        scaledFloatField(100),
				"transactions_90d":  longField(),
				"volume_90d":        scaledFloatField(100),
				"transactions_365d": longField(),
				"volume_365d":       scaledFloatField(100),

				// Behavior
				"preferred_transaction_type": keywordField(),
				"preferred_transaction_time": keywordField(),
				"top_merchant_categories":    keywordField(),

				// Flags
				"is_active":              booleanField(),
				"is_dormant":             booleanField(),
				"is_high_value_customer": booleanField(),
				"has_multiple_accounts":  booleanField(),
				"has_loan":               booleanField(),
				"has_card":               booleanField(),
			},
		},
	}
}

// Helper functions for field types
func keywordField() map[string]interface{} {
	return map[string]interface{}{"type": "keyword"}
}

func longField() map[string]interface{} {
	return map[string]interface{}{"type": "long"}
}

func integerField() map[string]interface{} {
	return map[string]interface{}{"type": "integer"}
}

func shortField() map[string]interface{} {
	return map[string]interface{}{"type": "short"}
}

func byteField() map[string]interface{} {
	return map[string]interface{}{"type": "byte"}
}

func floatField() map[string]interface{} {
	return map[string]interface{}{"type": "float"}
}

func scaledFloatField(factor int) map[string]interface{} {
	return map[string]interface{}{
		"type":           "scaled_float",
		"scaling_factor": factor,
	}
}

func booleanField() map[string]interface{} {
	return map[string]interface{}{"type": "boolean"}
}

func dateField() map[string]interface{} {
	return map[string]interface{}{"type": "date"}
}

func ipField() map[string]interface{} {
	return map[string]interface{}{"type": "ip"}
}

func geoPointField() map[string]interface{} {
	return map[string]interface{}{"type": "geo_point"}
}

func textWithKeywordField() map[string]interface{} {
	return map[string]interface{}{
		"type": "text",
		"fields": map[string]interface{}{
			"keyword": keywordField(),
		},
	}
}

func textWithAnalyzer(analyzer string) map[string]interface{} {
	return map[string]interface{}{
		"type":     "text",
		"analyzer": analyzer,
		"fields": map[string]interface{}{
			"keyword": keywordField(),
		},
	}
}

func objectField() map[string]interface{} {
	return map[string]interface{}{"type": "object"}
}

func nestedField(properties map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"type":       "nested",
		"properties": properties,
	}
}

func nestedAddressField() map[string]interface{} {
	return nestedField(map[string]interface{}{
		"address_line1": keywordField(),
		"address_line2": keywordField(),
		"city":          keywordField(),
		"state":         keywordField(),
		"postal_code":   keywordField(),
		"country":       keywordField(),
	})
}

// Create all indices
func CreateElasticsearchSchema() error {
	ctx := context.Background()
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
	indices := map[string]map[string]interface{}{
		IndexTransactions: GetTransactionAnalyticsMapping(),
		IndexCustomers:    GetCustomer360Mapping(),
	}

	for indexName, mapping := range indices {
		// Check if index exists
		exists, err := es.Indices.Exists([]string{indexName})
		if err != nil {
			return fmt.Errorf("failed to check index %s: %w", indexName, err)
		}

		if exists.StatusCode == 200 {
			fmt.Printf("Index %s already exists, skipping...\n", indexName)
			continue
		}

		// Create index
		body, _ := json.Marshal(mapping)
		res, err := es.Indices.Create(
			indexName,
			es.Indices.Create.WithBody(strings.NewReader(string(body))),
			es.Indices.Create.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("failed to create index %s: %w", indexName, err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return fmt.Errorf("error creating index %s: %s", indexName, res.String())
		}

		fmt.Printf("✓ Created index: %s\n", indexName)
	}

	return nil
}
