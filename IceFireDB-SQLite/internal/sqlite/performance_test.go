package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupPerformanceDB(t *testing.T) *sql.DB {
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	err := db.Ping()
	require.NoError(t, err)
	return db
}

func TestPerformanceConcurrentOperations(t *testing.T) {
	db := setupPerformanceDB(t)
	defer db.Close()

	// Create test table
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS perf_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		value INTEGER,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	require.NoError(t, err)

	// Test concurrent inserts using a single connection
	numWorkers := 5
	numInserts := 10
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numInserts; j++ {
				_, err := db.Exec(
					"INSERT INTO perf_test (name, value) VALUES (?, ?)",
					fmt.Sprintf("worker-%d-insert-%d", workerID, j),
					workerID*1000+j,
				)
				if err != nil {
					t.Logf("Insert error: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)
	totalOps := numWorkers * numInserts
	t.Logf("Completed %d concurrent inserts in %v (%.2f ops/sec)",
		totalOps, duration, float64(totalOps)/duration.Seconds())

	// Verify all records were inserted
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM perf_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, totalOps, count)
}

func TestPerformanceLargeDataset(t *testing.T) {
	db := setupPerformanceDB(t)
	defer db.Close()

	// Create table for large dataset
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS large_data (
		id INTEGER PRIMARY KEY,
		data TEXT,
		category TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`)
	require.NoError(t, err)

	// Insert large dataset
	numRecords := 10000
	start := time.Now()

	tx, err := db.Begin()
	require.NoError(t, err)

	for i := 0; i < numRecords; i++ {
		_, err := tx.Exec(
			"INSERT INTO large_data (data, category) VALUES (?, ?)",
			fmt.Sprintf("data-%d-"+generateRandomString(100), i),
			fmt.Sprintf("category-%d", i%10),
		)
		assert.NoError(t, err)
	}

	err = tx.Commit()
	require.NoError(t, err)

	duration := time.Since(start)
	t.Logf("Inserted %d records in %v (%.2f records/sec)",
		numRecords, duration, float64(numRecords)/duration.Seconds())

	// Test query performance
	start = time.Now()
	rows, err := db.Query("SELECT COUNT(*) FROM large_data WHERE category = ?", "category-5")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	if rows.Next() {
		err = rows.Scan(&count)
		require.NoError(t, err)
	}
	queryDuration := time.Since(start)
	t.Logf("Query completed in %v, found %d records", queryDuration, count)
	assert.Equal(t, numRecords/10, count) // Should be 1000 records (10000/10)
}

func TestPerformanceIndexing(t *testing.T) {
	db := setupPerformanceDB(t)
	defer db.Close()

	// Create table without index
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS index_test (
		id INTEGER PRIMARY KEY,
		name TEXT,
		value INTEGER
	)`)
	require.NoError(t, err)

	// Insert test data
	for i := 0; i < 1000; i++ {
		_, err := db.Exec(
			"INSERT INTO index_test (name, value) VALUES (?, ?)",
			fmt.Sprintf("item-%d", i),
			i,
		)
		assert.NoError(t, err)
	}

	// Test query without index
	start := time.Now()
	rows, err := db.Query("SELECT * FROM index_test WHERE value > 500")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	for rows.Next() {
		count++
	}
	noIndexDuration := time.Since(start)
	t.Logf("Query without index took %v, found %d records", noIndexDuration, count)

	// Create index
	_, err = db.Exec("CREATE INDEX idx_value ON index_test(value)")
	require.NoError(t, err)

	// Test query with index
	start = time.Now()
	rows, err = db.Query("SELECT * FROM index_test WHERE value > 500")
	require.NoError(t, err)
	defer rows.Close()

	count = 0
	for rows.Next() {
		count++
	}
	indexDuration := time.Since(start)
	t.Logf("Query with index took %v, found %d records", indexDuration, count)

	// Index should be faster (or at least not slower)
	t.Logf("Index performance improvement: %.2fx faster",
		float64(noIndexDuration)/float64(indexDuration))
}

func TestPerformanceMemoryUsage(t *testing.T) {
	db := setupPerformanceDB(t)
	defer db.Close()

	// Test memory usage with large result sets
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS memory_test (
		id INTEGER PRIMARY KEY,
		large_data TEXT
	)`)
	require.NoError(t, err)

	// Insert data with large text
	for i := 0; i < 100; i++ {
		_, err := db.Exec(
			"INSERT INTO memory_test (large_data) VALUES (?)",
			generateRandomString(10000), // 10KB per record
		)
		assert.NoError(t, err)
	}

	// Query all data at once
	start := time.Now()
	rows, err := db.Query("SELECT * FROM memory_test")
	require.NoError(t, err)
	defer rows.Close()

	var totalSize int
	for rows.Next() {
		var id int
		var data string
		err := rows.Scan(&id, &data)
		require.NoError(t, err)
		totalSize += len(data)
	}
	duration := time.Since(start)
	t.Logf("Processed %d bytes in %v (%.2f MB/s)",
		totalSize, duration, float64(totalSize)/(1024*1024)/duration.Seconds())
}

func TestPerformanceConnectionPool(t *testing.T) {
	// Test concurrent connections
	numConnections := 5
	var wg sync.WaitGroup
	errors := make(chan error, numConnections)

	for i := 0; i < numConnections; i++ {
		wg.Add(1)
		go func(connID int) {
			defer wg.Done()
			db := setupPerformanceDB(t)
			defer db.Close()

			// Each connection performs operations
			tableName := fmt.Sprintf("conn_test_%d", connID)
			_, err := db.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY)`, tableName))
			if err != nil {
				errors <- err
				return
			}

			for j := 0; j < 10; j++ {
				_, err := db.Exec(fmt.Sprintf("INSERT INTO %s (id) VALUES (?)", tableName), j)
				if err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for any errors
	for err := range errors {
		assert.NoError(t, err)
	}
}

func TestPerformanceTransactionThroughput(t *testing.T) {
	db := setupPerformanceDB(t)
	defer db.Close()

	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS tx_test (
		id INTEGER PRIMARY KEY,
		counter INTEGER
	)`)
	require.NoError(t, err)

	numTransactions := 100
	opsPerTransaction := 10
	start := time.Now()

	for i := 0; i < numTransactions; i++ {
		tx, err := db.Begin()
		require.NoError(t, err)

		for j := 0; j < opsPerTransaction; j++ {
			_, err := tx.Exec("INSERT INTO tx_test (counter) VALUES (?)", i*opsPerTransaction+j)
			assert.NoError(t, err)
		}

		err = tx.Commit()
		assert.NoError(t, err)
	}

	duration := time.Since(start)
	totalOps := numTransactions * opsPerTransaction
	t.Logf("Completed %d transactions (%d ops) in %v (%.2f tx/sec, %.2f ops/sec)",
		numTransactions, totalOps, duration,
		float64(numTransactions)/duration.Seconds(),
		float64(totalOps)/duration.Seconds())

	// Verify data integrity
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM tx_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, totalOps, count)
}

// Helper function to generate random strings for testing
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[i%len(charset)]
	}
	return string(result)
}
