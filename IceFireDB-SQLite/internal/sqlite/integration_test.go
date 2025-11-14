package sqlite

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationCompleteWorkflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration workflow test in short mode")
	}
	// Test a complete application workflow
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// 1. Create database schema
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id INTEGER PRIMARY KEY,
			username TEXT UNIQUE NOT NULL,
			email TEXT UNIQUE NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS posts (
			id INTEGER PRIMARY KEY,
			user_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			content TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (user_id) REFERENCES users (id)
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS comments (
			id INTEGER PRIMARY KEY,
			post_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			content TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			FOREIGN KEY (post_id) REFERENCES posts (id),
			FOREIGN KEY (user_id) REFERENCES users (id)
		)
	`)
	require.NoError(t, err)

	// 2. Insert test data
	tx, err := db.Begin()
	require.NoError(t, err)

	// Insert users
	userIDs := make([]int64, 3)
	for i := 0; i < 3; i++ {
		result, err := tx.Exec(
			"INSERT INTO users (username, email) VALUES (?, ?)",
			fmt.Sprintf("user%d", i),
			fmt.Sprintf("user%d@example.com", i),
		)
		require.NoError(t, err)
		userID, _ := result.LastInsertId()
		userIDs[i] = userID
	}

	// Insert posts
	postIDs := make([]int64, 5)
	for i := 0; i < 5; i++ {
		result, err := tx.Exec(
			"INSERT INTO posts (user_id, title, content) VALUES (?, ?, ?)",
			userIDs[i%3],
			fmt.Sprintf("Post Title %d", i),
			fmt.Sprintf("This is the content of post %d", i),
		)
		require.NoError(t, err)
		postID, _ := result.LastInsertId()
		postIDs[i] = postID
	}

	// Insert comments
	for i := 0; i < 10; i++ {
		_, err := tx.Exec(
			"INSERT INTO comments (post_id, user_id, content) VALUES (?, ?, ?)",
			postIDs[i%5],
			userIDs[i%3],
			fmt.Sprintf("Comment %d on post", i),
		)
		require.NoError(t, err)
	}

	err = tx.Commit()
	require.NoError(t, err)

	// 3. Test complex queries
	// Get user with post count
	rows, err := db.Query(`
		SELECT 
			u.username,
			COUNT(p.id) as post_count,
			COUNT(c.id) as comment_count
		FROM users u
		LEFT JOIN posts p ON u.id = p.user_id
		LEFT JOIN comments c ON u.id = c.user_id
		GROUP BY u.id, u.username
		ORDER BY post_count DESC
	`)
	require.NoError(t, err)
	defer rows.Close()

	var userStats []struct {
		Username     string
		PostCount    int
		CommentCount int
	}

	for rows.Next() {
		var username string
		var postCount, commentCount int
		err := rows.Scan(&username, &postCount, &commentCount)
		require.NoError(t, err)
		userStats = append(userStats, struct {
			Username     string
			PostCount    int
			CommentCount int
		}{username, postCount, commentCount})
	}

	assert.Len(t, userStats, 3)
	for _, stat := range userStats {
		assert.True(t, stat.PostCount >= 1)
		assert.True(t, stat.CommentCount >= 3)
	}

	// 4. Test transactions with rollback
	tx, err = db.Begin()
	require.NoError(t, err)

	// Try to insert duplicate user (should fail)
	_, err = tx.Exec("INSERT INTO users (username, email) VALUES (?, ?)", "user0", "duplicate@example.com")
	assert.Error(t, err) // Should fail due to unique constraint

	// Rollback the transaction
	err = tx.Rollback()
	require.NoError(t, err)

	// Verify no duplicate was inserted
	var userCount int
	err = db.QueryRow("SELECT COUNT(*) FROM users WHERE username = 'user0'").Scan(&userCount)
	require.NoError(t, err)
	assert.Equal(t, 1, userCount)

	// 5. Test data integrity with foreign keys
	// Enable foreign key constraints
	_, err = db.Exec("PRAGMA foreign_keys = ON")
	require.NoError(t, err)

	// Try to insert comment with invalid post_id (should fail)
	_, err = db.Exec("INSERT INTO comments (post_id, user_id, content) VALUES (?, ?, ?)", 999, userIDs[0], "Invalid comment")
	assert.Error(t, err) // Should fail due to foreign key constraint

	// 6. Test performance with indexes
	// Create indexes for better performance
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_posts_user_id ON posts(user_id)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments(post_id)")
	require.NoError(t, err)
	_, err = db.Exec("CREATE INDEX IF NOT EXISTS idx_comments_user_id ON comments(user_id)")
	require.NoError(t, err)

	// Test query performance with joins and filters
	rows, err = db.Query(`
		SELECT 
			p.title,
			u.username,
			COUNT(c.id) as comment_count
		FROM posts p
		JOIN users u ON p.user_id = u.id
		LEFT JOIN comments c ON p.id = c.post_id
		WHERE p.created_at > datetime('now', '-1 day')
		GROUP BY p.id, p.title, u.username
		ORDER BY comment_count DESC
	`)
	require.NoError(t, err)
	defer rows.Close()

	var postStats []struct {
		Title        string
		Username     string
		CommentCount int
	}

	for rows.Next() {
		var title, username string
		var commentCount int
		err := rows.Scan(&title, &username, &commentCount)
		require.NoError(t, err)
		postStats = append(postStats, struct {
			Title        string
			Username     string
			CommentCount int
		}{title, username, commentCount})
	}

	assert.Len(t, postStats, 5)

	// 7. Test cleanup
	_, err = db.Exec("DROP TABLE IF EXISTS comments")
	require.NoError(t, err)
	_, err = db.Exec("DROP TABLE IF EXISTS posts")
	require.NoError(t, err)
	_, err = db.Exec("DROP TABLE IF EXISTS users")
	require.NoError(t, err)

	t.Logf("Integration test completed successfully with %d users, %d posts, and %d comments",
		len(userIDs), len(postIDs), 10)
}

func TestIntegrationWithRealFile(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration with real file test in short mode")
	}
	// Test with actual file-based database
	ctx := context.Background()
	testFile := "test_integration.db"

	db := InitSQLite(ctx, testFile)
	defer db.Close()

	// Create and test basic operations
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS integration_test (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			value INTEGER
		)
	`)
	require.NoError(t, err)

	// Insert test data
	_, err = db.Exec("INSERT INTO integration_test (name, value) VALUES (?, ?)", "test1", 100)
	require.NoError(t, err)
	_, err = db.Exec("INSERT INTO integration_test (name, value) VALUES (?, ?)", "test2", 200)
	require.NoError(t, err)

	// Query data
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM integration_test").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Cleanup
	_, err = db.Exec("DROP TABLE IF EXISTS integration_test")
	require.NoError(t, err)

	// Note: File cleanup would normally happen here, but we'll leave it for manual cleanup
	// os.Remove(testFile)
}

func TestIntegrationErrorScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration error scenarios test in short mode")
	}
	ctx := context.Background()
	db := InitSQLite(ctx, ":memory:")
	defer db.Close()

	// Test various error scenarios

	// 1. Invalid SQL syntax
	_, err := db.Exec("INVALID SQL SYNTAX")
	assert.Error(t, err)

	// 2. Query non-existent table
	_, err = db.Exec("SELECT * FROM non_existent_table")
	assert.Error(t, err)

	// 3. Invalid transaction usage
	tx, err := db.Begin()
	require.NoError(t, err)

	// Try to use committed transaction
	err = tx.Commit()
	require.NoError(t, err)

	_, err = tx.Exec("INSERT INTO test (name) VALUES ('should fail')")
	assert.Error(t, err)

	// 4. Database constraints
	_, err = db.Exec(`
		CREATE TABLE constraint_test (
			id INTEGER PRIMARY KEY,
			unique_col TEXT UNIQUE
		)
	`)
	require.NoError(t, err)

	_, err = db.Exec("INSERT INTO constraint_test (unique_col) VALUES ('unique_value')")
	require.NoError(t, err)

	// Try to insert duplicate (should fail)
	_, err = db.Exec("INSERT INTO constraint_test (unique_col) VALUES ('unique_value')")
	assert.Error(t, err)

	// Cleanup
	_, err = db.Exec("DROP TABLE IF EXISTS constraint_test")
	require.NoError(t, err)
}
