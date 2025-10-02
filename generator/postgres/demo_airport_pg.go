package generator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	_ "github.com/lib/pq" // postgres driver
)

func CreateAirportDemoPostgresSchema() {
	username := "postgres"
	password := "postgres"
	host := "localhost"
	port := 5432
	database := "banking_db"

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, username, password, database,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		log.Printf("error: %v", err.Error())
		return
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	defer db.Close()

	log.Println("Starting schema creation...")

	if err := createSchema(ctx, db); err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	log.Println("Schema creation completed successfully!")
}
func createSchema(ctx context.Context, db *sql.DB) error {
	schemas := []string{
		createTenantTables(),
		createCustomerTables(),
		createAccountTables(),
		createTransactionTables(),
		createPaymentInstrumentTables(),
		createLoanTables(),
		createAuditTables(),
		createIndexes(),
	}

	for _, schema := range schemas {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			return fmt.Errorf("error executing schema: %w", err)
		}
	}

	return nil
}

func createTenantTables() string {
	return `
-- Tenant Management Layer
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id BIGSERIAL PRIMARY KEY,
    tenant_code VARCHAR(50) UNIQUE NOT NULL,
    tenant_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    country_code VARCHAR(3),
    timezone VARCHAR(50) DEFAULT 'UTC',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tenant_configurations (
    config_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id) ON DELETE CASCADE,
    config_key VARCHAR(100) NOT NULL,
    config_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, config_key)
);
`
}

func createCustomerTables() string {
	return `
-- Customer Domain
CREATE TABLE IF NOT EXISTS customers (
    customer_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    customer_code VARCHAR(50) NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(20),
    date_of_birth DATE,
    nationality VARCHAR(3),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, customer_code)
);

CREATE TABLE IF NOT EXISTS customer_kyc (
    kyc_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    document_type VARCHAR(50) NOT NULL,
    document_number VARCHAR(100) NOT NULL,
    verification_status VARCHAR(20) DEFAULT 'pending',
    verified_at TIMESTAMP,
    expiry_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_addresses (
    address_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    address_type VARCHAR(20) DEFAULT 'primary',
    address_line1 VARCHAR(255),
    address_line2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_documents (
    document_id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    document_type VARCHAR(50),
    document_name VARCHAR(255),
    document_url TEXT,
    file_size BIGINT,
    mime_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`
}

func createAccountTables() string {
	return `
-- Account Domain
CREATE TABLE IF NOT EXISTS accounts (
    account_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    account_number VARCHAR(50) NOT NULL,
    account_type VARCHAR(30) NOT NULL,
    currency_code VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'active',
    opened_date DATE DEFAULT CURRENT_DATE,
    closed_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, account_number)
);

CREATE TABLE IF NOT EXISTS account_holders (
    holder_id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    holder_type VARCHAR(20) DEFAULT 'primary',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id, customer_id)
);

CREATE TABLE IF NOT EXISTS account_balances (
    balance_id BIGSERIAL PRIMARY KEY,
    account_id BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    available_balance DECIMAL(20, 4) DEFAULT 0.00,
    current_balance DECIMAL(20, 4) DEFAULT 0.00,
    hold_balance DECIMAL(20, 4) DEFAULT 0.00,
    last_transaction_date TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(account_id)
);
`
}

func createTransactionTables() string {
	return `
-- Transaction Domain (Partitioned for massive scale)
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id BIGSERIAL,
    tenant_id BIGINT NOT NULL,
    transaction_ref VARCHAR(100) NOT NULL,
    from_account_id BIGINT REFERENCES accounts(account_id),
    to_account_id BIGINT REFERENCES accounts(account_id),
    transaction_type VARCHAR(50) NOT NULL,
    amount DECIMAL(20, 4) NOT NULL,
    currency_code VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'pending',
    description TEXT,
    transaction_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    value_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (transaction_id, tenant_id, transaction_date)
) PARTITION BY RANGE (transaction_date);

-- Create initial partitions for transactions (example: quarterly)
CREATE TABLE IF NOT EXISTS transactions_2024_q4 
    PARTITION OF transactions 
    FOR VALUES FROM ('2024-10-01') TO ('2025-01-01');

CREATE TABLE IF NOT EXISTS transactions_2025_q1 
    PARTITION OF transactions 
    FOR VALUES FROM ('2025-01-01') TO ('2025-04-01');

CREATE TABLE IF NOT EXISTS transactions_2025_q2 
    PARTITION OF transactions 
    FOR VALUES FROM ('2025-04-01') TO ('2025-07-01');

CREATE TABLE IF NOT EXISTS transactions_2025_q3 
    PARTITION OF transactions 
    FOR VALUES FROM ('2025-07-01') TO ('2025-10-01');

CREATE TABLE IF NOT EXISTS transactions_2025_q4 
    PARTITION OF transactions 
    FOR VALUES FROM ('2025-10-01') TO ('2026-01-01');

-- Double-entry bookkeeping
CREATE TABLE IF NOT EXISTS transaction_legs (
    leg_id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    account_id BIGINT NOT NULL REFERENCES accounts(account_id),
    leg_type VARCHAR(10) NOT NULL CHECK (leg_type IN ('debit', 'credit')),
    amount DECIMAL(20, 4) NOT NULL,
    balance_after DECIMAL(20, 4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pending_transactions (
    pending_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    transaction_ref VARCHAR(100) NOT NULL,
    from_account_id BIGINT REFERENCES accounts(account_id),
    to_account_id BIGINT REFERENCES accounts(account_id),
    amount DECIMAL(20, 4) NOT NULL,
    currency_code VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'processing',
    initiated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_metadata (
    metadata_id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    metadata_key VARCHAR(100) NOT NULL,
    metadata_value TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`
}

func createPaymentInstrumentTables() string {
	return `
-- Payment Instruments
CREATE TABLE IF NOT EXISTS cards (
    card_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    account_id BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    card_number_hash VARCHAR(255) NOT NULL,
    card_last_four VARCHAR(4),
    card_type VARCHAR(20),
    card_brand VARCHAR(30),
    expiry_month INT,
    expiry_year INT,
    status VARCHAR(20) DEFAULT 'active',
    issued_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS card_transactions (
    card_txn_id BIGSERIAL PRIMARY KEY,
    card_id BIGINT NOT NULL REFERENCES cards(card_id),
    transaction_id BIGINT,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    merchant_name VARCHAR(255),
    merchant_category VARCHAR(50),
    amount DECIMAL(20, 4) NOT NULL,
    currency_code VARCHAR(3) DEFAULT 'USD',
    status VARCHAR(20) DEFAULT 'approved',
    authorization_code VARCHAR(50),
    transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS beneficiaries (
    beneficiary_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    beneficiary_name VARCHAR(255) NOT NULL,
    account_number VARCHAR(50),
    bank_code VARCHAR(50),
    bank_name VARCHAR(255),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`
}

func createLoanTables() string {
	return `
-- Loan/Credit Domain
CREATE TABLE IF NOT EXISTS loans (
    loan_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    customer_id BIGINT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    account_id BIGINT REFERENCES accounts(account_id),
    loan_number VARCHAR(50) NOT NULL,
    loan_type VARCHAR(50) NOT NULL,
    principal_amount DECIMAL(20, 4) NOT NULL,
    interest_rate DECIMAL(5, 4),
    tenure_months INT,
    status VARCHAR(20) DEFAULT 'active',
    disbursement_date DATE,
    maturity_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(tenant_id, loan_number)
);

CREATE TABLE IF NOT EXISTS loan_repayments (
    repayment_id BIGSERIAL PRIMARY KEY,
    loan_id BIGINT NOT NULL REFERENCES loans(loan_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    repayment_date DATE NOT NULL,
    principal_amount DECIMAL(20, 4) DEFAULT 0.00,
    interest_amount DECIMAL(20, 4) DEFAULT 0.00,
    penalty_amount DECIMAL(20, 4) DEFAULT 0.00,
    total_amount DECIMAL(20, 4) NOT NULL,
    status VARCHAR(20) DEFAULT 'paid',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS loan_schedules (
    schedule_id BIGSERIAL PRIMARY KEY,
    loan_id BIGINT NOT NULL REFERENCES loans(loan_id) ON DELETE CASCADE,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    installment_number INT NOT NULL,
    due_date DATE NOT NULL,
    principal_due DECIMAL(20, 4),
    interest_due DECIMAL(20, 4),
    total_due DECIMAL(20, 4),
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`
}

func createAuditTables() string {
	return `
-- Audit & Compliance
CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id BIGSERIAL,
    tenant_id BIGINT NOT NULL,
    user_id BIGINT,
    entity_type VARCHAR(50),
    entity_id BIGINT,
    action VARCHAR(50) NOT NULL,
    old_values JSONB,
    new_values JSONB,
    ip_address INET,
    user_agent TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (audit_id, tenant_id, created_at)
) PARTITION BY RANGE (created_at);

-- Create initial partitions for audit logs (monthly)
CREATE TABLE IF NOT EXISTS audit_logs_2025_01 
    PARTITION OF audit_logs 
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE IF NOT EXISTS audit_logs_2025_02 
    PARTITION OF audit_logs 
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');

CREATE TABLE IF NOT EXISTS audit_logs_2025_03 
    PARTITION OF audit_logs 
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    transaction_id BIGINT,
    customer_id BIGINT REFERENCES customers(customer_id),
    alert_type VARCHAR(50) NOT NULL,
    risk_score DECIMAL(5, 2),
    status VARCHAR(20) DEFAULT 'open',
    description TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS compliance_reports (
    report_id BIGSERIAL PRIMARY KEY,
    tenant_id BIGINT NOT NULL REFERENCES tenants(tenant_id),
    report_type VARCHAR(50) NOT NULL,
    report_period VARCHAR(20),
    report_data JSONB,
    generated_by BIGINT,
    generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
`
}

func createIndexes() string {
	return `
-- Performance Indexes for ETL and Queries
-- Tenant indexes
CREATE INDEX IF NOT EXISTS idx_tenants_code ON tenants(tenant_code);
CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status);

-- Customer indexes
CREATE INDEX IF NOT EXISTS idx_customers_tenant ON customers(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_phone ON customers(phone);
CREATE INDEX IF NOT EXISTS idx_customers_status ON customers(tenant_id, status);

-- Account indexes
CREATE INDEX IF NOT EXISTS idx_accounts_tenant ON accounts(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_accounts_number ON accounts(account_number);
CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(tenant_id, status);

-- Account holders indexes
CREATE INDEX IF NOT EXISTS idx_account_holders_customer ON account_holders(customer_id);
CREATE INDEX IF NOT EXISTS idx_account_holders_tenant ON account_holders(tenant_id);

-- Transaction indexes (critical for ETL)
CREATE INDEX IF NOT EXISTS idx_transactions_tenant_date ON transactions(tenant_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_ref ON transactions(transaction_ref);
CREATE INDEX IF NOT EXISTS idx_transactions_from_account ON transactions(from_account_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_to_account ON transactions(to_account_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_status ON transactions(tenant_id, status, transaction_date);
CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at);

-- Transaction legs indexes
CREATE INDEX IF NOT EXISTS idx_transaction_legs_txn ON transaction_legs(transaction_id);
CREATE INDEX IF NOT EXISTS idx_transaction_legs_account ON transaction_legs(account_id, created_at);
CREATE INDEX IF NOT EXISTS idx_transaction_legs_tenant ON transaction_legs(tenant_id);

-- Card indexes
CREATE INDEX IF NOT EXISTS idx_cards_tenant ON cards(tenant_id);
CREATE INDEX IF NOT EXISTS idx_cards_account ON cards(account_id);
CREATE INDEX IF NOT EXISTS idx_cards_customer ON cards(customer_id);
CREATE INDEX IF NOT EXISTS idx_cards_status ON cards(status);

-- Card transaction indexes
CREATE INDEX IF NOT EXISTS idx_card_txns_card ON card_transactions(card_id, transaction_date);
CREATE INDEX IF NOT EXISTS idx_card_txns_tenant ON card_transactions(tenant_id, transaction_date);

-- Loan indexes
CREATE INDEX IF NOT EXISTS idx_loans_tenant ON loans(tenant_id);
CREATE INDEX IF NOT EXISTS idx_loans_customer ON loans(customer_id);
CREATE INDEX IF NOT EXISTS idx_loans_status ON loans(status);

-- Audit log indexes
CREATE INDEX IF NOT EXISTS idx_audit_tenant_date ON audit_logs(tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_logs(entity_type, entity_id);

-- Fraud alert indexes
CREATE INDEX IF NOT EXISTS idx_fraud_tenant ON fraud_alerts(tenant_id, detected_at);
CREATE INDEX IF NOT EXISTS idx_fraud_status ON fraud_alerts(status);
`
}

const (
	BatchSize = 5000 // Insert records in batches for performance
)

// SeedConfig controls how much data to generate
type SeedConfig struct {
	Tenants              int
	CustomersPerTenant   int
	AccountsPerCustomer  int
	TransactionsToCreate int // This is the primary target for 1M per run
}

func PerformSeed() {
	username := "postgres"
	password := "postgres"
	host := "localhost"
	port := 5432
	database := "banking_db"

	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		host, port, username, password, database,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("error: %v", err.Error())
		return
	}
	defer db.Close()

	ctx := context.Background()

	// Configuration: Each run creates 1M transactions
	seedConfig := SeedConfig{
		Tenants:              10,        // Create 10 tenants if they don't exist
		CustomersPerTenant:   1000,      // 1K customers per tenant
		AccountsPerCustomer:  2,         // 2 accounts per customer
		TransactionsToCreate: 1_000_000, // 1 MILLION transactions per run
	}

	log.Println("Starting data seeding...")
	log.Printf("Target: %d transactions this run\n", seedConfig.TransactionsToCreate)

	if err := seedData(ctx, db, seedConfig); err != nil {
		log.Fatalf("Failed to seed data: %v", err)
	}

	log.Println("Data seeding completed successfully!")
}

func seedData(ctx context.Context, db *sql.DB, config SeedConfig) error {
	startTime := time.Now()

	// Step 1: Seed tenants (idempotent)
	tenantIDs, err := seedTenants(ctx, db, config.Tenants)
	if err != nil {
		return fmt.Errorf("failed to seed tenants: %w", err)
	}
	log.Printf("✓ Tenants ready: %d\n", len(tenantIDs))

	// Step 2: Seed customers for each tenant
	customerIDs, err := seedCustomers(ctx, db, tenantIDs, config.CustomersPerTenant)
	if err != nil {
		return fmt.Errorf("failed to seed customers: %w", err)
	}
	log.Printf("✓ Customers seeded: %d\n", len(customerIDs))

	// Step 3: Seed accounts for customers
	accountIDs, err := seedAccounts(ctx, db, customerIDs, config.AccountsPerCustomer)
	if err != nil {
		return fmt.Errorf("failed to seed accounts: %w", err)
	}
	log.Printf("✓ Accounts seeded: %d\n", len(accountIDs))

	// Step 4: Seed MASSIVE transactions (1M per run)
	if err := seedTransactions(ctx, db, accountIDs, config.TransactionsToCreate); err != nil {
		return fmt.Errorf("failed to seed transactions: %w", err)
	}
	log.Printf("✓ Transactions seeded: %d\n", config.TransactionsToCreate)

	// Step 5: Seed supporting data
	if err := seedSupportingData(ctx, db, tenantIDs, customerIDs, accountIDs); err != nil {
		return fmt.Errorf("failed to seed supporting data: %w", err)
	}

	elapsed := time.Since(startTime)
	log.Printf("Total time: %s\n", elapsed)
	log.Printf("Throughput: %.0f transactions/second\n",
		float64(config.TransactionsToCreate)/elapsed.Seconds())

	return nil
}

// seedTenants creates tenants (idempotent - skips existing)
func seedTenants(ctx context.Context, db *sql.DB, count int) ([]int64, error) {
	log.Println("Seeding tenants...")

	var existingIDs []int64
	rows, err := db.QueryContext(ctx, "SELECT tenant_id FROM tenants ORDER BY tenant_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		existingIDs = append(existingIDs, id)
	}

	// If we have enough tenants, return them
	if len(existingIDs) >= count {
		return existingIDs[:count], nil
	}

	// Create missing tenants
	needed := count - len(existingIDs)
	countries := []string{"USA", "GBR", "CAN", "AUS", "IND", "SGP", "DEU", "FRA", "JPN", "BRA"}

	for i := 0; i < needed; i++ {
		tenantCode := fmt.Sprintf("BANK%04d", len(existingIDs)+i+1)
		tenantName := fmt.Sprintf("Bank %s", tenantCode)
		country := countries[rand.Intn(len(countries))]

		var tenantID int64
		err := db.QueryRowContext(ctx, `
			INSERT INTO tenants (tenant_code, tenant_name, country_code, status)
			VALUES ($1, $2, $3, 'active')
			ON CONFLICT (tenant_code) DO NOTHING
			RETURNING tenant_id
		`, tenantCode, tenantName, country).Scan(&tenantID)

		if err == nil {
			existingIDs = append(existingIDs, tenantID)
		}
	}

	return existingIDs, nil
}

// seedCustomers creates customers in batches
func seedCustomers(ctx context.Context, db *sql.DB, tenantIDs []int64, perTenant int) ([]CustomerAccount, error) {
	log.Printf("Seeding %d customers per tenant...\n", perTenant)

	var customers []CustomerAccount
	firstNames := []string{"John", "Jane", "Michael", "Sarah", "David", "Emma", "James", "Olivia", "Robert", "Sophia"}
	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"}

	for _, tenantID := range tenantIDs {
		// Check existing customer count for this tenant
		var existingCount int
		err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM customers WHERE tenant_id = $1", tenantID).Scan(&existingCount)
		if err != nil {
			return nil, err
		}

		needed := perTenant - existingCount
		if needed <= 0 {
			// Load existing customers
			rows, err := db.QueryContext(ctx, `
				SELECT customer_id, tenant_id 
				FROM customers 
				WHERE tenant_id = $1 
				LIMIT $2
			`, tenantID, perTenant)
			if err != nil {
				return nil, err
			}
			defer rows.Close()

			for rows.Next() {
				var ca CustomerAccount
				if err := rows.Scan(&ca.CustomerID, &ca.TenantID); err != nil {
					return nil, err
				}
				customers = append(customers, ca)
			}
			continue
		}

		// Batch insert new customers
		for batch := 0; batch < needed; batch += BatchSize {
			batchEnd := batch + BatchSize
			if batchEnd > needed {
				batchEnd = needed
			}

			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				return nil, err
			}

			valueStrings := []string{}
			valueArgs := []interface{}{}
			argPos := 1

			for i := batch; i < batchEnd; i++ {
				customerCode := fmt.Sprintf("CUST%d%06d", tenantID, existingCount+i+1)
				firstName := firstNames[rand.Intn(len(firstNames))]
				lastName := lastNames[rand.Intn(len(lastNames))]
				email := fmt.Sprintf("%s.%s%d@email.com",
					strings.ToLower(firstName),
					strings.ToLower(lastName),
					rand.Intn(1000))
				phone := fmt.Sprintf("+1555%07d", rand.Intn(10000000))

				valueStrings = append(valueStrings,
					fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
						argPos, argPos+1, argPos+2, argPos+3, argPos+4, argPos+5, argPos+6))

				valueArgs = append(valueArgs, tenantID, customerCode, firstName, lastName,
					email, phone, "active")
				argPos += 7
			}

			query := fmt.Sprintf(`
				INSERT INTO customers (tenant_id, customer_code, first_name, last_name, email, phone, status)
				VALUES %s
				RETURNING customer_id, tenant_id
			`, strings.Join(valueStrings, ","))

			rows, err := tx.QueryContext(ctx, query, valueArgs...)
			if err != nil {
				tx.Rollback()
				return nil, err
			}

			for rows.Next() {
				var ca CustomerAccount
				if err := rows.Scan(&ca.CustomerID, &ca.TenantID); err != nil {
					rows.Close()
					tx.Rollback()
					return nil, err
				}
				customers = append(customers, ca)
			}
			rows.Close()

			if err := tx.Commit(); err != nil {
				return nil, err
			}
		}

		log.Printf("  Tenant %d: %d customers ready\n", tenantID, perTenant)
	}

	return customers, nil
}

type CustomerAccount struct {
	CustomerID int64
	TenantID   int64
	AccountIDs []int64
}

// seedAccounts creates accounts for customers
func seedAccounts(ctx context.Context, db *sql.DB, customers []CustomerAccount, perCustomer int) ([]AccountInfo, error) {
	log.Printf("Seeding %d accounts per customer...\n", perCustomer)

	var accounts []AccountInfo
	accountTypes := []string{"checking", "savings", "money_market", "credit"}
	currencies := []string{"USD", "EUR", "GBP", "CAD"}

	for idx, customer := range customers {
		// Check existing accounts
		var existingAccounts []int64
		rows, err := db.QueryContext(ctx, `
			SELECT a.account_id 
			FROM accounts a
			JOIN account_holders ah ON a.account_id = ah.account_id
			WHERE ah.customer_id = $1
			LIMIT $2
		`, customer.CustomerID, perCustomer)

		if err != nil {
			return nil, err
		}

		for rows.Next() {
			var accountID int64
			if err := rows.Scan(&accountID); err != nil {
				rows.Close()
				return nil, err
			}
			existingAccounts = append(existingAccounts, accountID)
		}
		rows.Close()

		// Add existing to result
		for _, accID := range existingAccounts {
			accounts = append(accounts, AccountInfo{
				AccountID:  accID,
				TenantID:   customer.TenantID,
				CustomerID: customer.CustomerID,
			})
		}

		needed := perCustomer - len(existingAccounts)
		if needed <= 0 {
			continue
		}

		// Create new accounts
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}

		for i := 0; i < needed; i++ {
			accountNumber := fmt.Sprintf("%d%010d", customer.TenantID, rand.Int63n(10000000000))
			accountType := accountTypes[rand.Intn(len(accountTypes))]
			currency := currencies[rand.Intn(len(currencies))]

			var accountID int64
			err := tx.QueryRowContext(ctx, `
				INSERT INTO accounts (tenant_id, account_number, account_type, currency_code, status)
				VALUES ($1, $2, $3, $4, 'active')
				RETURNING account_id
			`, customer.TenantID, accountNumber, accountType, currency).Scan(&accountID)

			if err != nil {
				tx.Rollback()
				return nil, err
			}

			// Link to customer
			_, err = tx.ExecContext(ctx, `
				INSERT INTO account_holders (account_id, customer_id, tenant_id, holder_type)
				VALUES ($1, $2, $3, 'primary')
			`, accountID, customer.CustomerID, customer.TenantID)

			if err != nil {
				tx.Rollback()
				return nil, err
			}

			// Initialize balance
			initialBalance := float64(rand.Intn(100000))
			_, err = tx.ExecContext(ctx, `
				INSERT INTO account_balances (account_id, tenant_id, available_balance, current_balance)
				VALUES ($1, $2, $3, $3)
			`, accountID, customer.TenantID, initialBalance)

			if err != nil {
				tx.Rollback()
				return nil, err
			}

			accounts = append(accounts, AccountInfo{
				AccountID:  accountID,
				TenantID:   customer.TenantID,
				CustomerID: customer.CustomerID,
			})
		}

		if err := tx.Commit(); err != nil {
			return nil, err
		}

		if (idx+1)%1000 == 0 {
			log.Printf("  Processed %d/%d customers\n", idx+1, len(customers))
		}
	}

	return accounts, nil
}

type AccountInfo struct {
	AccountID  int64
	TenantID   int64
	CustomerID int64
}

// seedTransactions creates MASSIVE transaction data (1M per run)
func seedTransactions(ctx context.Context, db *sql.DB, accounts []AccountInfo, count int) error {
	log.Printf("Seeding %d transactions...\n", count)

	if len(accounts) < 2 {
		return fmt.Errorf("need at least 2 accounts to create transactions")
	}

	transactionTypes := []string{"transfer", "deposit", "withdrawal", "payment", "refund"}
	statuses := []string{"completed", "completed", "completed", "pending", "failed"}

	startDate := time.Now().AddDate(0, -6, 0) // Start 6 months ago

	processed := 0
	for batch := 0; batch < count; batch += BatchSize {
		batchEnd := batch + BatchSize
		if batchEnd > count {
			batchEnd = count
		}

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		valueStrings := []string{}
		valueArgs := []interface{}{}
		argPos := 1

		for i := batch; i < batchEnd; i++ {
			// Random accounts
			fromAccount := accounts[rand.Intn(len(accounts))]
			toAccount := accounts[rand.Intn(len(accounts))]

			// Ensure different accounts
			for toAccount.AccountID == fromAccount.AccountID {
				toAccount = accounts[rand.Intn(len(accounts))]
			}

			txnRef := fmt.Sprintf("TXN%d%013d", fromAccount.TenantID, time.Now().UnixNano()+int64(i))
			txnType := transactionTypes[rand.Intn(len(transactionTypes))]
			amount := float64(rand.Intn(100000)) / 100.0 // $0.01 to $1000.00
			status := statuses[rand.Intn(len(statuses))]

			// Random date in the past 6 months
			randomDays := rand.Intn(180)
			txnDate := startDate.AddDate(0, 0, randomDays)

			valueStrings = append(valueStrings,
				fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
					argPos, argPos+1, argPos+2, argPos+3, argPos+4,
					argPos+5, argPos+6, argPos+7, argPos+8, argPos+9))

			valueArgs = append(valueArgs,
				fromAccount.TenantID,
				txnRef,
				fromAccount.AccountID,
				toAccount.AccountID,
				txnType,
				amount,
				"USD",
				status,
				fmt.Sprintf("%s transaction", txnType),
				txnDate,
			)
			argPos += 10
		}

		query := fmt.Sprintf(`
			INSERT INTO transactions 
			(tenant_id, transaction_ref, from_account_id, to_account_id, transaction_type, 
			 amount, currency_code, status, description, transaction_date)
			VALUES %s
		`, strings.Join(valueStrings, ","))

		_, err = tx.ExecContext(ctx, query, valueArgs...)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("batch insert failed: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return err
		}

		processed += (batchEnd - batch)
		if processed%50000 == 0 {
			log.Printf("  Progress: %d/%d (%.1f%%)\n", processed, count, float64(processed)/float64(count)*100)
		}
	}

	log.Printf("  ✓ %d transactions completed\n", count)
	return nil
}

// seedSupportingData creates cards, KYC, etc.
func seedSupportingData(ctx context.Context, db *sql.DB, tenantIDs []int64, customers []CustomerAccount, accounts []AccountInfo) error {
	log.Println("Seeding supporting data...")

	// Seed some cards (10% of accounts)
	cardCount := len(accounts) / 10
	if cardCount > 1000 {
		cardCount = 1000 // Limit for demo
	}

	for i := 0; i < cardCount; i++ {
		account := accounts[rand.Intn(len(accounts))]
		lastFour := fmt.Sprintf("%04d", rand.Intn(10000))
		cardTypes := []string{"debit", "credit"}
		brands := []string{"visa", "mastercard", "amex"}

		_, err := db.ExecContext(ctx, `
			INSERT INTO cards (tenant_id, account_id, customer_id, card_number_hash, 
				card_last_four, card_type, card_brand, expiry_month, expiry_year, status)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'active')
			ON CONFLICT DO NOTHING
		`, account.TenantID, account.AccountID, account.CustomerID,
			fmt.Sprintf("hash_%d", rand.Int63()),
			lastFour,
			cardTypes[rand.Intn(len(cardTypes))],
			brands[rand.Intn(len(brands))],
			rand.Intn(12)+1,
			time.Now().Year()+rand.Intn(5))

		if err != nil {
			log.Printf("Warning: card insert failed: %v\n", err)
		}
	}

	log.Printf("  ✓ Created %d cards\n", cardCount)
	return nil
}

// Add this to check current data counts
func printDataStats(ctx context.Context, db *sql.DB) error {
	tables := []string{"tenants", "customers", "accounts", "transactions"}

	fmt.Println("\n=== Current Data Statistics ===")
	for _, table := range tables {
		var count int64
		err := db.QueryRowContext(ctx,
			fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
		if err != nil {
			return err
		}
		fmt.Printf("%-15s: %s\n", table, formatNumber(count))
	}
	fmt.Println("================================\n")
	return nil
}

func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	var result []byte
	for i, digit := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(digit))
	}
	return string(result)
}
