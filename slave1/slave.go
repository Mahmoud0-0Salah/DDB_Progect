package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

type Config struct {
	SlaveID          string
	SlavePort        int
	MasterURL        string
	HeartbeatSeconds int
	SQLServer        string
	SQLDatabase      string
	TrustedConn      bool
	SQLUser          string
	SQLPassword      string
}

type SlaveService struct {
	config      Config
	db          *sql.DB
	httpClient  *http.Client
	server      *http.Server
	shutdownCh  chan os.Signal
	isShutdown  bool
	shutdownMux sync.Mutex
}

func NewSlaveService(config Config) (*SlaveService, error) {
	var connString string
	if config.TrustedConn {
		connString = fmt.Sprintf("server=%s;database=%s;trusted_connection=yes;trustservercertificate=true",
			config.SQLServer, config.SQLDatabase)
	} else {
		connString = fmt.Sprintf("server=%s;database=%s;user id=%s;password=%s;trustservercertificate=true",
			config.SQLServer, config.SQLDatabase, config.SQLUser, config.SQLPassword)
	}

	db, err := sql.Open("sqlserver", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to SQL Server: %w", err)
	}

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping SQL Server: %w", err)
	}

	return &SlaveService{
		config:     config,
		db:         db,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		shutdownCh: make(chan os.Signal, 1),
	}, nil
}

func (s *SlaveService) Start() error {
	signal.Notify(s.shutdownCh, syscall.SIGINT, syscall.SIGTERM)
	s.startHTTPServer()

	if err := s.registerWithMaster(); err != nil {
		return fmt.Errorf("failed to register with master: %w", err)
	}
	log.Printf("Registered with master at %s", s.config.MasterURL)

	go s.startHeartbeat()

	<-s.shutdownCh
	s.shutdownMux.Lock()
	s.isShutdown = true
	s.shutdownMux.Unlock()

	if err := s.sendShutdownNotice(); err != nil {
		log.Printf("Error sending shutdown notice: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.server.Shutdown(ctx); err != nil {
		log.Printf("Error shutting down HTTP server: %v", err)
	}

	if err := s.db.Close(); err != nil {
		log.Printf("Error closing SQL Server connection: %v", err)
	}

	return nil
}

func (s *SlaveService) registerWithMaster() error {
	addr := fmt.Sprintf("http://localhost:%d", s.config.SlavePort)
	resp, err := s.httpClient.Get(fmt.Sprintf("%s/slave/up?addr=%s", s.config.MasterURL, addr))
	if err != nil {
		return fmt.Errorf("failed to register with master: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master returned non-OK status: %d", resp.StatusCode)
	}

	log.Printf("Successfully marked as up with master at %s", s.config.MasterURL)
	return nil
}

func (s *SlaveService) startHeartbeat() {
	ticker := time.NewTicker(time.Duration(s.config.HeartbeatSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.shutdownMux.Lock()
			if s.isShutdown {
				s.shutdownMux.Unlock()
				return
			}
			s.shutdownMux.Unlock()

			if err := s.sendHeartbeat(); err != nil {
				log.Printf("Error sending heartbeat: %v", err)
			}
		case <-s.shutdownCh:
			return
		}
	}
}

func (s *SlaveService) sendHeartbeat() error {
	resp, err := s.httpClient.Get(fmt.Sprintf("%s/ping", s.config.MasterURL))
	if err != nil {
		log.Printf("Master is down: %v", err)
		// Attempt to re-register
		if err := s.registerWithMaster(); err != nil {
			log.Printf("Failed to re-register with master: %v", err)
		}
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err := fmt.Errorf("master returned non-OK status: %d", resp.StatusCode)
		log.Printf("Master is down: %v", err)
		// Attempt to re-register
		if err := s.registerWithMaster(); err != nil {
			log.Printf("Failed to re-register with master: %v", err)
		}
		return err
	}

	return nil
}

func (s *SlaveService) sendShutdownNotice() error {
	addr := fmt.Sprintf("http://localhost:%d", s.config.SlavePort)
	resp, err := s.httpClient.Get(fmt.Sprintf("%s/slave/down?addr=%s", s.config.MasterURL, addr))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("master returned non-OK status: %d", resp.StatusCode)
	}

	return nil
}

func (s *SlaveService) startHTTPServer() {
	mux := http.NewServeMux()

	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		allowCORS(w)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("pong"))
	})

	mux.HandleFunc("/replicate/insert", s.handleReplicateInsert)
	mux.HandleFunc("/replicate/createtable", s.handleReplicateCreateTable)
	mux.HandleFunc("/replicate/update", s.handleReplicateUpdate)
	mux.HandleFunc("/replicate/delete", s.handleReplicateDelete)
	mux.HandleFunc("/replicate/db", s.handleReplicateCreateDB)
	mux.HandleFunc("/replicate/dropdb", s.handleReplicateDropDB)

	mux.HandleFunc("/insert", s.handleInsert)
	mux.HandleFunc("/select", s.handleSelect)
	mux.HandleFunc("/update", s.handleUpdate)
	mux.HandleFunc("/delete", s.handleDelete)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.SlavePort),
		Handler: mux,
	}

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Error starting HTTP server: %v", err)
		}
	}()

	log.Printf("HTTP server started on port %d", s.config.SlavePort)
}

func allowCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

func (s *SlaveService) withDatabaseContext(dbName string, fn func(tx *sql.Tx) error) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	tx, err := s.db.Begin()
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

	_, err = tx.Exec(fmt.Sprintf("USE %s", dbName))
	if err != nil {
		return fmt.Errorf("failed to switch to database %s: %v", dbName, err)
	}

	return fn(tx)
}

func (s *SlaveService) checkRecordExists(dbName, table, id string) (bool, error) {
	var count int
	err := s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE id = %s", table, id)
		return tx.QueryRow(query).Scan(&count)
	})
	if err != nil {
		return false, fmt.Errorf("failed to check for existing record in table %s: %v", table, err)
	}
	return count > 0, nil
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

func (s *SlaveService) handleReplicateInsert(w http.ResponseWriter, r *http.Request) {
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
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /replicate/insert request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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

	// Check if the record already exists
	exists, err := s.checkRecordExists(dbName, table, id)
	if err != nil {
		log.Printf("Error checking for existing record: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if exists {
		log.Printf("Record with id %s already exists in table %s, skipping insert", id, table)
		w.WriteHeader(http.StatusOK)
		return
	}

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, cleanedValues)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Insert failed on slave into table %s in database %s: %v", table, dbName, err)
			return fmt.Errorf("insert failed on slave: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Replicated INSERT into table %s in database %s: %s", table, dbName, cleanedValues)
	w.WriteHeader(http.StatusOK)
}

func (s *SlaveService) handleReplicateCreateTable(w http.ResponseWriter, r *http.Request) {
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
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /replicate/createtable request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '%s') CREATE TABLE %s (%s)", table, table, req.Schema)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Create table failed on slave for table %s in database %s: %v", table, dbName, err)
			return fmt.Errorf("create table failed on slave: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Replicated CREATE TABLE %s in database %s", table, dbName)
	w.WriteHeader(http.StatusOK)
}

func (s *SlaveService) handleReplicateUpdate(w http.ResponseWriter, r *http.Request) {
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
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /replicate/update request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, req.Set, req.Where)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Update failed on slave for table %s in database %s: %v", table, dbName, err)
			return fmt.Errorf("update failed on slave: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Replicated UPDATE on table %s in database %s: SET %s WHERE %s", table, dbName, req.Set, req.Where)
	w.WriteHeader(http.StatusOK)
}

func (s *SlaveService) handleReplicateDelete(w http.ResponseWriter, r *http.Request) {
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
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /replicate/delete request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
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

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, req.Where)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Delete failed on slave for table %s in database %s: %v", table, dbName, err)
			return fmt.Errorf("delete failed on slave: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Replicated DELETE from table %s in database %s: WHERE %s", table, dbName, req.Where)
	w.WriteHeader(http.StatusOK)
}

func (s *SlaveService) handleReplicateCreateDB(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		log.Printf("Method not allowed for /replicate/db: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbname := r.URL.Query().Get("name")
	if dbname == "" {
		log.Printf("Database creation failed on slave: missing database name")
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

	query := fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '%s') CREATE DATABASE %s", dbName, dbName)
	_, err = s.db.Exec(query)
	if err != nil {
		log.Printf("Failed to create database %s on slave: %v", dbName, err)
		http.Error(w, "Failed to create database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Replicated CREATE DATABASE %s on slave", dbName)
	w.WriteHeader(http.StatusOK)
}

func (s *SlaveService) handleReplicateDropDB(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		log.Printf("Method not allowed for /replicate/dropdb: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbname := r.URL.Query().Get("name")
	if dbname == "" {
		log.Printf("Database drop failed on slave: missing database name")
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
	_, err = s.db.Exec(query)
	if err != nil {
		log.Printf("Failed to drop database %s on slave: %v", dbName, err)
		http.Error(w, "Failed to drop database: "+err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("Replicated DROP DATABASE %s on slave", dbName)
	w.WriteHeader(http.StatusOK)
}

func (s *SlaveService) handleInsert(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		log.Printf("Method not allowed for /insert: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DBName string `json:"dbname"`
		Table  string `json:"table"`
		Values string `json:"values"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /insert request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Values == "" {
		log.Printf("Insert failed on slave: missing fields (dbname=%s, table=%s, values=%s)", req.DBName, req.Table, req.Values)
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

	// Check if the record already exists
	exists, err := s.checkRecordExists(dbName, table, id)
	if err != nil {
		log.Printf("Error checking for existing record: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if exists {
		log.Printf("Record with id %s already exists in table %s", id, table)
		http.Error(w, "Record with this ID already exists", http.StatusConflict)
		return
	}

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, cleanedValues)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Failed to insert record into table %s in database %s on slave: %v", table, dbName, err)
			return fmt.Errorf("failed to insert record: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload := struct {
		DBName           string `json:"dbname"`
		Table            string `json:"table"`
		Values           string `json:"values"`
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}{
		DBName:           dbName,
		Table:            table,
		Values:           req.Values,
		FromSlave:        true,
		OriginatingSlave: fmt.Sprintf("http://localhost:%d", s.config.SlavePort),
	}

	if err := s.forwardToMaster("/insert", payload); err != nil {
		log.Printf("Error forwarding insert to master: %v", err)
	}

	log.Printf("Inserted record into table %s in database %s on slave: %s", table, dbName, cleanedValues)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record inserted successfully"})
}

func (s *SlaveService) handleSelect(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		log.Printf("Method not allowed for /select: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	dbname := r.URL.Query().Get("dbname")
	table := r.URL.Query().Get("table")
	where := r.URL.Query().Get("where")

	if dbname == "" || table == "" {
		log.Printf("Select failed on slave: missing dbname or table (dbname=%s, table=%s)", dbname, table)
		http.Error(w, "dbname and table are required", http.StatusBadRequest)
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

	var results []map[string]interface{}
	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("SELECT * FROM %s", tableName)
		if where != "" {
			query += " WHERE " + where
		}

		rows, err := tx.Query(query)
		if err != nil {
			log.Printf("Failed to select records from table %s in database %s on slave: %v", tableName, dbName, err)
			return fmt.Errorf("failed to select records: %v", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			log.Printf("Failed to get columns for table %s in database %s on slave: %v", tableName, dbName, err)
			return fmt.Errorf("failed to get columns: %v", err)
		}

		for rows.Next() {
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			if err := rows.Scan(valuePtrs...); err != nil {
				log.Printf("Failed to scan row for table %s in database %s on slave: %v", tableName, dbName, err)
				return fmt.Errorf("failed to scan row: %v", err)
			}

			row := make(map[string]interface{})
			for i, col := range columns {
				var v interface{}
				val := values[i]
				b, ok := val.([]byte)
				if ok {
					v = string(b)
				} else {
					v = val
				}
				row[col] = v
			}
			results = append(results, row)
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

func (s *SlaveService) handleUpdate(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		log.Printf("Method not allowed for /update: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DBName string `json:"dbname"`
		Table  string `json:"table"`
		Set    string `json:"set"`
		Where  string `json:"where"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /update request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Set == "" || req.Where == "" {
		log.Printf("Update failed on slave: missing fields (dbname=%s, table=%s, set=%s, where=%s)", req.DBName, req.Table, req.Set, req.Where)
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

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, req.Set, req.Where)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Failed to update record in table %s in database %s on slave: %v", table, dbName, err)
			return fmt.Errorf("failed to update record: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload := struct {
		DBName           string `json:"dbname"`
		Table            string `json:"table"`
		Set              string `json:"set"`
		Where            string `json:"where"`
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}{
		DBName:           dbName,
		Table:            table,
		Set:              req.Set,
		Where:            req.Where,
		FromSlave:        true,
		OriginatingSlave: fmt.Sprintf("http://localhost:%d", s.config.SlavePort),
	}

	if err := s.forwardToMaster("/update", payload); err != nil {
		log.Printf("Error forwarding update to master: %v", err)
	}

	log.Printf("Updated record in table %s in database %s on slave: SET %s WHERE %s", table, dbName, req.Set, req.Where)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record updated successfully"})
}

func (s *SlaveService) handleDelete(w http.ResponseWriter, r *http.Request) {
	allowCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPost {
		log.Printf("Method not allowed for /delete: %s", r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DBName string `json:"dbname"`
		Table  string `json:"table"`
		Where  string `json:"where"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Failed to decode /delete request: %v", err)
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.DBName == "" || req.Table == "" || req.Where == "" {
		log.Printf("Delete failed on slave: missing fields (dbname=%s, table=%s, where=%s)", req.DBName, req.Table, req.Where)
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

	err = s.withDatabaseContext(dbName, func(tx *sql.Tx) error {
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", table, req.Where)
		_, err := tx.Exec(query)
		if err != nil {
			log.Printf("Failed to delete record from table %s in database %s on slave: %v", table, dbName, err)
			return fmt.Errorf("failed to delete record: %v", err)
		}
		return nil
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	payload := struct {
		DBName           string `json:"dbname"`
		Table            string `json:"table"`
		Where            string `json:"where"`
		FromSlave        bool   `json:"fromSlave"`
		OriginatingSlave string `json:"originatingSlave"`
	}{
		DBName:           dbName,
		Table:            table,
		Where:            req.Where,
		FromSlave:        true,
		OriginatingSlave: fmt.Sprintf("http://localhost:%d", s.config.SlavePort),
	}

	if err := s.forwardToMaster("/delete", payload); err != nil {
		log.Printf("Error forwarding delete to master: %v", err)
	}

	log.Printf("Deleted record from table %s in database %s on slave: WHERE %s", table, dbName, req.Where)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Record deleted successfully"})
}

func (s *SlaveService) forwardToMaster(path string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to marshal data for forwarding to master on path %s: %v", path, err)
		return err
	}

	maxRetries := 3
	retryDelay := 2 * time.Second
	for i := 0; i < maxRetries; i++ {
		resp, err := s.httpClient.Post(
			s.config.MasterURL+path,
			"application/json",
			bytes.NewBuffer(jsonData),
		)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				log.Printf("Successfully forwarded to master on path %s", path)
				return nil
			}
			log.Printf("Failed to forward to master on path %s: status code %d", path, resp.StatusCode)
		} else {
			log.Printf("Failed to forward to master on path %s: %v", path, err)
		}
		if i < maxRetries-1 {
			log.Printf("Attempt %d failed for forwarding to master on path %s, retrying in %v...", i+1, path, retryDelay)
			time.Sleep(retryDelay)
			retryDelay *= 2
		}
	}
	return fmt.Errorf("failed to forward to master on path %s after %d attempts", path, maxRetries)
}

func main() {
	config := Config{
		SlaveID:          "slave1",
		SlavePort:        8003,
		MasterURL:        "http://192.168.194.127:8001",
		HeartbeatSeconds: 5,
		SQLServer:        "localhost",
		SQLDatabase:      "recordings", // Connect to master database initially
		TrustedConn:      true,
		SQLUser:          "sa",
		SQLPassword:      "Root1234#",
	}

	slave, err := NewSlaveService(config)
	if err != nil {
		log.Fatalf("Error creating slave service: %v", err)
	}
	if err := slave.Start(); err != nil {
		log.Fatalf("Error starting slave service: %v", err)
	}
}
