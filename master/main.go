package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
)

func allowCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

type PendingOperation struct {
	Path string
	Data string
}

var downedSlaves = make(map[string]bool)
var pendingOps = make(map[string][]PendingOperation)

var db *sql.DB
var slaveAddresses = []string{"http://192.168.194.90:8002", "http://localhost:8003"}

func main() {
	// SQL Server connection parameters
	server := "localhost"
	database := "recordings" // Connect to the master database initially
	trustedConn := true
	user := "sa"
	password := "Root1234#"

	// Build connection string
	var connString string
	if trustedConn {
		connString = fmt.Sprintf("server=%s;database=%s;trusted_connection=yes;trustservercertificate=true", server, database)
	} else {
		connString = fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;trustservercertificate=true", server, database, user, password)
	}

	// Open SQL Server connection
	var err error
	db, err = sql.Open("sqlserver", connString)
	if err != nil {
		log.Fatal("Failed to open SQL Server connection:", err)
	}
	defer db.Close()

	// Verify connection
	err = db.Ping()
	if err != nil {
		log.Fatal("Failed to connect to SQL Server:", err)
	}

	http.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
	})

	http.HandleFunc("/createdb", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		createDB(w, r)
	})

	http.HandleFunc("/dropdb", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		dropDB(w, r)
	})

	http.HandleFunc("/createtable", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		createTable(w, r)
	})

	http.HandleFunc("/insert", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		insertRecord(w, r)
	})

	http.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		selectRecords(w, r)
	})

	http.HandleFunc("/update", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		updateRecord(w, r)
	})

	http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		deleteRecord(w, r)
	})

	// Replication endpoints
	http.HandleFunc("/replicate/insert", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			log.Printf("Method not allowed for /replicate/insert: %s", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			DBName           string `json:"dbname"`
			Table            string `json:"table"`
			Values           string `json:"values"`
			Sender           string `json:"sender"`
			FromSlave        bool   `json:"fromSlave"`
			OriginatingSlave string `json:"originatingSlave"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("Failed to decode /replicate/insert request: %v", err)
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		forwardToOtherSlaves("/replicate/insert", req, req.OriginatingSlave)
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/replicate/createtable", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			log.Printf("Method not allowed for /replicate/createtable: %s", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			DBName           string `json:"dbname"`
			Table            string `json:"table"`
			Schema           string `json:"schema"`
			Sender           string `json:"sender"`
			FromSlave        bool   `json:"fromSlave"`
			OriginatingSlave string `json:"originatingSlave"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("Failed to decode /replicate/createtable request: %v", err)
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		forwardToOtherSlaves("/replicate/createtable", req, req.OriginatingSlave)
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/replicate/update", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			log.Printf("Method not allowed for /replicate/update: %s", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			DBName           string `json:"dbname"`
			Table            string `json:"table"`
			Set              string `json:"set"`
			Where            string `json:"where"`
			Sender           string `json:"sender"`
			FromSlave        bool   `json:"fromSlave"`
			OriginatingSlave string `json:"originatingSlave"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("Failed to decode /replicate/update request: %v", err)
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		forwardToOtherSlaves("/replicate/update", req, req.OriginatingSlave)
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/replicate/delete", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost {
			log.Printf("Method not allowed for /replicate/delete: %s", r.Method)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			DBName           string `json:"dbname"`
			Table            string `json:"table"`
			Where            string `json:"where"`
			Sender           string `json:"sender"`
			FromSlave        bool   `json:"fromSlave"`
			OriginatingSlave string `json:"originatingSlave"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("Failed to decode /replicate/delete request: %v", err)
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}

		forwardToOtherSlaves("/replicate/delete", req, req.OriginatingSlave)
		w.WriteHeader(http.StatusOK)
	})

	http.HandleFunc("/slave/down", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		addr := r.URL.Query().Get("addr")
		if addr == "" {
			http.Error(w, "Missing addr param", http.StatusBadRequest)
			return
		}
		downedSlaves[addr] = true
		log.Printf("Slave %s marked as down", addr)
		w.Write([]byte("Marked as down"))
	})

	http.HandleFunc("/slave/up", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		addr := r.URL.Query().Get("addr")
		if addr == "" {
			http.Error(w, "Missing addr param", http.StatusBadRequest)
			return
		}
		downedSlaves[addr] = false
		log.Printf("Slave %s marked as up", addr)
		go replayPendingOps(addr)
		w.Write([]byte("Marked as up and replay started"))
	})

	// Start periodic retry for pending operations
	go startPendingOpsRetry()

	fmt.Println("Master server running on port 8001...")
	log.Fatal(http.ListenAndServe(":8001", nil))
}

func startPendingOpsRetry() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for addr := range pendingOps {
			if !downedSlaves[addr] {
				go replayPendingOps(addr)
			}
		}
	}
}

func sanitizeSQLIdentifier(name string) (string, error) {
	cleaned := strings.TrimSpace(name)
	for _, ch := range cleaned {
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
			return "", fmt.Errorf("invalid SQL identifier: %s contains illegal characters", name)
		}
	}
	if cleaned == "" {
		return "", fmt.Errorf("SQL identifier cannot be empty")
	}
	return cleaned, nil
}

func withDatabaseContext(dbName string, fn func(tx *sql.Tx) error) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	// Start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
		}
	}()

	// Switch to the target database within the transaction
	_, err = tx.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		return fmt.Errorf("failed to switch to database %s: %v", dbName, err)
	}

	// Execute the operation
	return fn(tx)
}

func createDB(w http.ResponseWriter, r *http.Request) {
	dbname := r.URL.Query().Get("name")
	if dbname == "" {
		log.Printf("Database creation failed: missing database name")
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	// Sanitize DBName
	dbName, err := sanitizeSQLIdentifier(dbname)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Execute on the master database context
	query := fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '%s') CREATE DATABASE %s", dbName, dbName)
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("Failed to create database %s on master: %v", dbName, err)
		http.Error(w, "Failed to create database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	go replicateToSlaves("/replicate/db?name=" + dbName)
	log.Printf("Created database %s on master", dbName)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Database created successfully"})
}

func dropDB(w http.ResponseWriter, r *http.Request) {
	dbname := r.URL.Query().Get("name")
	if dbname == "" {
		log.Printf("Database drop failed: missing database name")
		http.Error(w, "Database name is required", http.StatusBadRequest)
		return
	}

	// Sanitize DBName
	dbName, err := sanitizeSQLIdentifier(dbname)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	query := fmt.Sprintf("IF EXISTS (SELECT * FROM sys.databases WHERE name = '%s') DROP DATABASE %s", dbName, dbName)
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("Failed to drop database %s on master: %v", dbName, err)
		http.Error(w, "Failed to drop database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	go replicateToSlaves("/replicate/dropdb?name=" + dbName)
	log.Printf("Dropped database %s on master", dbName)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Database dropped successfully"})
}

func createTable(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	dbname := r.URL.Query().Get("dbname")
	table := r.URL.Query().Get("table")
	schema := r.URL.Query().Get("schema")

	if dbname == "" || table == "" || schema == "" {
		log.Printf("Table creation failed: missing parameters (dbname=%s, table=%s, schema=%s)", dbname, table, schema)
		http.Error(w, "All parameters (dbname, table, schema) are required", http.StatusBadRequest)
		return
	}

	// Sanitize DBName and Table
	dbName, err := sanitizeSQLIdentifier(dbname)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tableName, err := sanitizeSQLIdentifier(table)
	if err != nil {
		log.Printf("Invalid table name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Use withDatabaseContext to ensure the correct database context
	err = withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '%s') CREATE TABLE %s (%s)", tableName, tableName, schema)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Failed to create table %s in database %s on master: %v", tableName, dbName, err)
			return fmt.Errorf("failed to create table: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	go forwardToOtherSlaves("/replicate/createtable", map[string]string{
		"dbname": dbName,
		"table":  tableName,
		"schema": schema,
	}, "")
	log.Printf("Created table %s in database %s on master", tableName, dbName)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Table created successfully"})
}

func insertRecord(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName           string `json:"dbname"`
		Table            string `json:"table"`
		Values           string `json:"values"`
		Sender           string `json:"sender"`
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /insert request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.DBName == "" || req.Table == "" || req.Values == "" {
		log.Printf("Insert failed: missing fields (dbname=%s, table=%s, values=%s)", req.DBName, req.Table, req.Values)
		http.Error(w, "All fields (dbname, table, values) are required", http.StatusBadRequest)
		return
	}

	// Sanitize DBName and Table
	dbName, err := sanitizeSQLIdentifier(req.DBName)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	table, err := sanitizeSQLIdentifier(req.Table)
	if err != nil {
		log.Printf("Invalid table name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Parse the ID from the values (assuming format "(id, 'name')")
	cleanedValues := strings.Trim(req.Values, "()")
	parts := strings.SplitN(cleanedValues, ",", 2)
	if len(parts) < 1 {
		log.Printf("Invalid values format for INSERT into %s: %s", table, cleanedValues)
		http.Error(w, "Invalid values format", http.StatusBadRequest)
		return
	}
	id := strings.TrimSpace(parts[0])

	// Check if the ID already exists
	err = withDatabaseContext(dbName, func(tx *sql.Tx) error {
		var count int
		queryCheck := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = %s", table, id)
		err := tx.QueryRow(queryCheck).Scan(&count)
		if err != nil {
			log.Printf("Failed to check for existing record in table %s: %v", table, err)
			return fmt.Errorf("failed to check for existing record: %v", err)
		}
		if count > 0 {
			log.Printf("Record with id %s already exists in table %s", id, table)
			return fmt.Errorf("record with this ID already exists")
		}

		query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, cleanedValues)
		_, err = tx.Exec(query)
		if err != nil {
			log.Printf("Failed to insert record into table %s in database %s on master: %v", table, dbName, err)
			return fmt.Errorf("failed to insert record: %v", err)
		}
		return nil
	})
	if err != nil {
		if strings.Contains(err.Error(), "record with this ID already exists") {
			http.Error(w, err.Error(), http.StatusConflict)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	go forwardToOtherSlaves("/replicate/insert", req, req.OriginatingSlave)
	log.Printf("Inserted record into table %s in database %s on master", table, dbName)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record inserted successfully"})
}

func selectRecords(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	table := r.URL.Query().Get("table")
	dbName := r.URL.Query().Get("dbname")

	if table == "" || dbName == "" {
		log.Printf("Select failed: missing dbname or table (dbname=%s, table=%s)", dbName, table)
		http.Error(w, "Missing dbname or table", http.StatusBadRequest)
		return
	}

	// Sanitize DBName and Table
	dbName, err := sanitizeSQLIdentifier(dbName)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	tableName, err := sanitizeSQLIdentifier(table)
	if err != nil {
		log.Printf("Invalid table name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var results []map[string]interface{}
	err = withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("SELECT * FROM %s", tableName)
		rows, err := tx.Query(query)
		if err != nil {
			log.Printf("Failed to query table %s in database %s on master: %v", tableName, dbName, err)
			return fmt.Errorf("failed to query table: %v", err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			log.Printf("Failed to get columns for table %s in database %s on master: %v", tableName, dbName, err)
			return fmt.Errorf("failed to get columns: %v", err)
		}

		for rows.Next() {
			values := make([]interface{}, len(cols))
			valuePtrs := make([]interface{}, len(cols))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Printf("Failed to scan row for table %s in database %s on master: %v", tableName, dbName, err)
				return fmt.Errorf("failed to scan row: %v", err)
			}

			rowMap := make(map[string]interface{})
			for i, col := range cols {
				rowMap[col] = values[i]
			}
			results = append(results, rowMap)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func updateRecord(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName           string `json:"dbname"`
		Table            string `json:"table"`
		Set              string `json:"set"`
		Where            string `json:"where"`
		Sender           string `json:"sender"`
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /update request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Set == "" || req.Where == "" {
		log.Printf("Update failed: missing fields (dbname=%s, table=%s, set=%s, where=%s)", req.DBName, req.Table, req.Set, req.Where)
		http.Error(w, "All fields (dbname, table, set, where) are required", http.StatusBadRequest)
		return
	}

	// Sanitize DBName and Table
	dbName, err := sanitizeSQLIdentifier(req.DBName)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	table, err := sanitizeSQLIdentifier(req.Table)
	if err != nil {
		log.Printf("Invalid table name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, req.Set, req.Where)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Failed to update record in table %s in database %s on master: %v", table, dbName, err)
			return fmt.Errorf("failed to update record: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	go forwardToOtherSlaves("/replicate/update", req, req.OriginatingSlave)
	log.Printf("Updated record in table %s in database %s on master", table, dbName)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record updated successfully"})
}

func deleteRecord(w http.ResponseWriter, r *http.Request) {
	var req struct {
		DBName           string `json:"dbname"`
		Table            string `json:"table"`
		Where            string `json:"where"`
		Sender           string `json:"sender"`
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /delete request: %v", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Where == "" {
		log.Printf("Delete failed: missing fields (dbname=%s, table=%s, where=%s)", req.DBName, req.Table, req.Where)
		http.Error(w, "All fields (dbname, table, where) are required", http.StatusBadRequest)
		return
	}

	// Sanitize DBName and Table
	dbName, err := sanitizeSQLIdentifier(req.DBName)
	if err != nil {
		log.Printf("Invalid database name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	table, err := sanitizeSQLIdentifier(req.Table)
	if err != nil {
		log.Printf("Invalid table name: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, req.Where)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Failed to delete record from table %s in database %s on master: %v", table, dbName, err)
			return fmt.Errorf("failed to delete record: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	go forwardToOtherSlaves("/replicate/delete", req, req.OriginatingSlave)
	log.Printf("Deleted record from table %s in database %s on master", table, dbName)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record deleted successfully"})
}

func replicateToSlaves(path string) {
	for _, addr := range slaveAddresses {
		go func(address string) {
			maxRetries := 3
			retryDelay := 2 * time.Second
			for i := 0; i < maxRetries; i++ {
				client := &http.Client{Timeout: 10 * time.Second}
				resp, err := client.Get(address + path)
				if err == nil {
					defer resp.Body.Close()
					if resp.StatusCode == http.StatusOK {
						log.Printf("Replication to %s succeeded for path %s", address, path)
						return
					}
					log.Printf("Replication to %s failed with status %d for path %s", address, resp.StatusCode, path)
				} else {
					log.Printf("Replication to %s failed with error: %v for path %s", address, err, path)
				}
				if i < maxRetries-1 {
					log.Printf("Attempt %d failed for %s, retrying in %v...", i+1, address, retryDelay)
					time.Sleep(retryDelay)
					retryDelay *= 2
				}
			}
			log.Printf("Failed to replicate to %s after %d attempts for path %s", address, maxRetries, path)
			downedSlaves[address] = true
		}(addr)
	}
}

func replayPendingOps(addr string) {
	ops := pendingOps[addr]
	if len(ops) == 0 {
		log.Printf("No pending ops for %s", addr)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	maxRetries := 3
	retryDelay := 2 * time.Second

	var remainingOps []PendingOperation
	for _, op := range ops {
		success := false
		for i := 0; i < maxRetries; i++ {
			resp, err := client.Post(addr+op.Path, "application/json", strings.NewReader(op.Data))
			if err == nil && resp.StatusCode == http.StatusOK {
				log.Printf("Replayed op to %s succeeded for path %s", addr, op.Path)
				success = true
				resp.Body.Close()
				break
			} else {
				if err != nil {
					log.Printf("Replay to %s failed for path %s: %v", addr, op.Path, err)
				} else {
					log.Printf("Replay to %s failed for path %s: status code %d", addr, op.Path, resp.StatusCode)
					resp.Body.Close()
				}
				if i < maxRetries-1 {
					log.Printf("Retry %d for replay to %s for path %s in %v...", i+1, addr, op.Path, retryDelay)
					time.Sleep(retryDelay)
				}
			}
		}
		if !success {
			log.Printf("Failed to replay op to %s for path %s after %d attempts, keeping in pendingOps", addr, op.Path, maxRetries)
			remainingOps = append(remainingOps, op)
		}
	}

	if len(remainingOps) == 0 {
		log.Printf("All pending ops replayed successfully for %s, clearing pendingOps", addr)
		pendingOps[addr] = nil
	} else {
		log.Printf("%d ops still pending for %s", len(remainingOps), addr)
		pendingOps[addr] = remainingOps
	}
}

func forwardToOtherSlaves(path string, data interface{}, senderAddr string) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to marshal data for replication on path %s: %v", path, err)
		return
	}
	for _, addr := range slaveAddresses {
		if downedSlaves[addr] || addr == senderAddr {
			if downedSlaves[addr] {
				log.Printf("Slave %s is down, storing operation for path %s", addr, path)
				pendingOps[addr] = append(pendingOps[addr], PendingOperation{Path: path, Data: string(jsonData)})
			}
			continue
		}
		go func(address string) {
			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Post(address+path, "application/json", strings.NewReader(string(jsonData)))
			if err != nil {
				log.Printf("Failed to forward to %s for path %s: %v", address, path, err)
				downedSlaves[address] = true
				pendingOps[address] = append(pendingOps[address], PendingOperation{Path: path, Data: string(jsonData)})
			} else if resp.StatusCode != http.StatusOK {
				log.Printf("Failed to forward to %s for path %s: received status code %d", address, path, resp.StatusCode)
				downedSlaves[address] = true
				pendingOps[address] = append(pendingOps[address], PendingOperation{Path: path, Data: string(jsonData)})
				resp.Body.Close()
			} else {
				log.Printf("Forwarded to %s successfully for path %s", address, path)
				resp.Body.Close()
			}
		}(addr)
	}
}
