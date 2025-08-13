package adodb

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// getTestMdbDsn returns a DSN for the test MDB file.
// It assumes testole.mdb will be created in the current directory.
func getTestMdbDsn(t *testing.T) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	mdbPath := filepath.Join(wd, "testole.mdb")

	// Attempt to create the MDB file if it doesn't exist.
	// This requires the Access ODBC driver to be able to create databases.
	// If it can't, the user must provide an empty testole.mdb file.
	if _, err := os.Stat(mdbPath); os.IsNotExist(err) {
		// Try to create the MDB file by opening a connection to it.
		// The Microsoft Access Driver can create a new file if it doesn't exist.
		connStr := fmt.Sprintf("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;CreateDB=%s;", mdbPath, mdbPath)
		db, errCreate := sql.Open("adodb", connStr)
		if errCreate != nil {
			t.Logf("Note: Could not automatically create MDB file via DSN '%s' (normal if driver does not support CreateDB): %v", connStr, errCreate)
			t.Logf("Please ensure an empty MDB file named 'testole.mdb' exists in the test directory: %s", wd)
			// Proceeding with assumption it might exist or be created by subsequent connection.
		} else {
			// Close immediately, we just wanted to create it.
			db.Close()
			t.Logf("Attempted to create MDB file at: %s", mdbPath)
		}
	} else if err == nil {
		t.Logf("Using existing MDB file at: %s", mdbPath)
	}


	return fmt.Sprintf("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;", mdbPath)
}

// TestOLEObjectHandling verifies reading and writing of binary data (simulating LONGVARBINARY).
func TestOLEObjectHandling(t *testing.T) {
	dsn := getTestMdbDsn(t)
	db, err := sql.Open("adodb", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to test MDB: %v. Ensure 'testole.mdb' exists or can be created, and ODBC drivers are installed.", err)
	}
	defer db.Close()

	// 1. Create table
	tableName := "BinaryTable"
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) // Use IF EXISTS if supported, or ignore error
	// if err != nil {
	// 	t.Logf("Note: Failed to drop table %s (may not exist): %v", tableName, err)
	// }

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (ID INTEGER PRIMARY KEY, BlobData OLEOBJECT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table %s: %v", tableName, err)
	}
	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
		if err != nil {
			t.Logf("Failed to drop table %s after test: %v", tableName, err)
		}
	}()

	// 2. Insert binary data
	// The "OLEOBJECT" type in Access is typically for linking/embedding objects.
	// For raw bytes, ADO usually maps this to adLongVarBinary.
	// We'll test with a byte slice.
	originalData := []byte{0, 1, 2, 3, 4, 250, 251, 252, 253, 254, 255}
	insertID := 1

	// Using a prepared statement for potentially better binary handling
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (ID, BlobData) VALUES (?, ?)", tableName))
	if err != nil {
		t.Fatalf("Failed to prepare insert statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(insertID, originalData)
	if err != nil {
		t.Fatalf("Failed to insert binary data: %v", err)
	}

	// 3. Query the data
	var retrievedData []byte
	var retrievedID int
	err = db.QueryRow(fmt.Sprintf("SELECT ID, BlobData FROM %s WHERE ID = ?", tableName), insertID).Scan(&retrievedID, &retrievedData)
	if err != nil {
		t.Fatalf("Failed to query binary data: %v", err)
	}

	// 4. Assertions
	if retrievedID != insertID {
		t.Errorf("Retrieved ID mismatch: got %d, want %d", retrievedID, insertID)
	}
	if !reflect.DeepEqual(retrievedData, originalData) {
		t.Errorf("Retrieved binary data mismatch: got %v, want %v", retrievedData, originalData)
	} else {
		t.Logf("Successfully retrieved and verified binary data: %v", retrievedData)
	}

	// Test with NULL binary data
	nullInsertID := 2
	_, err = stmt.Exec(nullInsertID, nil) // Insert NULL
	if err != nil {
		t.Fatalf("Failed to insert NULL binary data: %v", err)
	}

	var nullRetrievedData []byte // Should remain nil or be an empty slice if DB represents NULL that way
	var nullRetrievedID int
	// For some drivers/DBs, scanning a NULL BLOB into []byte might result in nil, for others an empty slice.
	// The adodb driver, after fixes, should produce a nil []byte for NULL binary types.
	err = db.QueryRow(fmt.Sprintf("SELECT ID, BlobData FROM %s WHERE ID = ?", tableName), nullInsertID).Scan(&nullRetrievedID, &nullRetrievedData)
	if err != nil {
		t.Fatalf("Failed to query NULL binary data: %v", err)
	}
	if nullRetrievedID != nullInsertID {
		t.Errorf("Retrieved ID for NULL data mismatch: got %d, want %d", nullRetrievedID, nullInsertID)
	}
	if nullRetrievedData != nil {
		// Depending on exact DB behavior for NULL OLEObject, it might be empty slice.
		// For our driver, we expect nil if the database field is NULL.
		t.Errorf("Retrieved NULL binary data was not nil: got %v (length %d)", nullRetrievedData, len(nullRetrievedData))
	} else {
		t.Log("Successfully retrieved NULL binary data as nil.")
	}
}

// TestQueryStress performs a large number of simple queries to check for stability.
func TestQueryStress(t *testing.T) {
	// This test can use any valid DSN.
	// If testing against MDB, it might be slow. Consider a faster in-memory DB if available via ADO.
	// For now, using the same MDB DSN but with a very simple query.
	dsn := getTestMdbDsn(t)
	db, err := sql.Open("adodb", dsn)
	if err != nil {
		t.Fatalf("Failed to connect for stress test: %v. Ensure 'testole.mdb' exists or can be created, and ODBC drivers are installed.", err)
	}
	defer db.Close()

	// Ping to ensure connection is alive before stress test
	err = db.Ping()
	if err != nil {
		t.Fatalf("Ping failed before stress test: %v", err)
	}

	iterations := 1000 // Can be increased for more thorough stress testing
	t.Logf("Starting query stress test with %d iterations...", iterations)

	startTime := time.Now()

	for i := 0; i < iterations; i++ {
		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("Query failed at iteration %d: %v", i, err)
		}
		if result != 1 {
			t.Fatalf("Query result unexpected at iteration %d: got %d, want 1", i, result)
		}
		if i%100 == 0 && i > 0 {
			t.Logf("Stress test progress: %d iterations completed.", i)
		}
	}

	duration := time.Since(startTime)
	t.Logf("Query stress test completed %d iterations successfully in %v (avg %.2f ms/query).", iterations, duration, float64(duration.Milliseconds())/float64(iterations))
}

// TestMain is used to ensure CoInitialize/CoUninitialize are called if tests run in parallel
// or if specific test setup requires STA. For ADODB, CoInitialize is called in Open.
// func TestMain(m *testing.M) {
// 	  // ADODB driver calls CoInitialize per connection.
// 	  // If global STA was needed:
// 	  // runtime.LockOSThread()
// 	  // ole.CoInitializeEx(0, ole.COINIT_APARTMENTTHREADED) // Or COINIT_MULTITHREADED
// 	  // exitCode := m.Run()
// 	  // ole.CoUninitialize()
// 	  // os.Exit(exitCode)
// 	  m.Run()
// }

// Note on MDB creation for TestOLEObjectHandling:
// The test attempts to create `testole.mdb` if it doesn't exist by using "CreateDB" in the DSN.
// This requires the "Microsoft Access Driver (*.mdb, *.accdb)" to be installed and to support this feature.
// If automatic creation fails, an empty MDB file named `testole.mdb` must be manually placed
// in the package directory (where the tests are run) for `TestOLEObjectHandling` to succeed.
// The table `BinaryTable` and its data are created by the test itself.

// Example of how to manually create a suitable MDB (e.g. testole.mdb):
// 1. Open MS Access.
// 2. Create a new Blank Database. Save it as `testole.mdb` in the root of this Go package.
// 3. The test `TestOLEObjectHandling` will then attempt to create the required table (`BinaryTable`)
//    and insert/query data from it.

// The DSN used is: "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=./testole.mdb;"
// This means the MDB file must be named `testole.mdb` and be in the same directory
// where `go test` is executed for this package.
// The `getTestMdbDsn` function handles path construction.

// The stress test `TestQueryStress` also uses this MDB by default but performs
// a very simple query (`SELECT 1`) that does not depend on specific table structures
// beyond what the driver needs for basic query execution.
// If `testole.mdb` cannot be created or found, both tests relying on it will fail early
// during `sql.Open`.
// Ensure the system running the tests has the "Microsoft Access Driver (*.mdb, *.accdb)" installed.
// For 64-bit Go, a 64-bit Access driver (Microsoft Access Database Engine Redistributable) is needed.
// For 32-bit Go, a 32-bit Access driver is needed.
// Mismatches in architecture between Go test binary and ODBC driver will cause connection failures.

package adodb

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// getTestMdbDsn returns a DSN for the test MDB file.
// It assumes testole.mdb will be created in the current directory.
func getTestMdbDsn(t *testing.T) string {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get working directory: %v", err)
	}
	mdbPath := filepath.Join(wd, "testole.mdb")

	// Attempt to create the MDB file if it doesn't exist.
	// This requires the Access ODBC driver to be able to create databases.
	// If it can't, the user must provide an empty testole.mdb file.
	if _, err := os.Stat(mdbPath); os.IsNotExist(err) {
		// Try to create the MDB file by opening a connection to it.
		// The Microsoft Access Driver can create a new file if it doesn't exist.
		connStr := fmt.Sprintf("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;CreateDB=%s;", mdbPath, mdbPath)
		db, errCreate := sql.Open("adodb", connStr)
		if errCreate != nil {
			t.Logf("Note: Could not automatically create MDB file via DSN '%s' (normal if driver does not support CreateDB): %v", connStr, errCreate)
			t.Logf("Please ensure an empty MDB file named 'testole.mdb' exists in the test directory: %s", wd)
			// Proceeding with assumption it might exist or be created by subsequent connection.
		} else {
			// Close immediately, we just wanted to create it.
			db.Close()
			t.Logf("Attempted to create MDB file at: %s", mdbPath)
		}
	} else if err == nil {
		t.Logf("Using existing MDB file at: %s", mdbPath)
	}


	return fmt.Sprintf("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s;", mdbPath)
}

// TestOLEObjectHandling verifies reading and writing of binary data (simulating LONGVARBINARY).
func TestOLEObjectHandling(t *testing.T) {
	dsn := getTestMdbDsn(t)
	db, err := sql.Open("adodb", dsn)
	if err != nil {
		t.Fatalf("Failed to connect to test MDB: %v. Ensure 'testole.mdb' exists or can be created, and ODBC drivers are installed.", err)
	}
	defer db.Close()

	// 1. Create table
	tableName := "BinaryTable"
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)) // Use IF EXISTS if supported, or ignore error
	// if err != nil {
	// 	t.Logf("Note: Failed to drop table %s (may not exist): %v", tableName, err)
	// }

	_, err = db.Exec(fmt.Sprintf("CREATE TABLE %s (ID INTEGER PRIMARY KEY, BlobData OLEOBJECT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table %s: %v", tableName, err)
	}
	defer func() {
		_, err := db.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
		if err != nil {
			t.Logf("Failed to drop table %s after test: %v", tableName, err)
		}
	}()

	// 2. Insert binary data
	// The "OLEOBJECT" type in Access is typically for linking/embedding objects.
	// For raw bytes, ADO usually maps this to adLongVarBinary.
	// We'll test with a byte slice.
	originalData := []byte{0, 1, 2, 3, 4, 250, 251, 252, 253, 254, 255}
	insertID := 1

	// Using a prepared statement for potentially better binary handling
	stmt, err := db.Prepare(fmt.Sprintf("INSERT INTO %s (ID, BlobData) VALUES (?, ?)", tableName))
	if err != nil {
		t.Fatalf("Failed to prepare insert statement: %v", err)
	}
	defer stmt.Close()

	_, err = stmt.Exec(insertID, originalData)
	if err != nil {
		t.Fatalf("Failed to insert binary data: %v", err)
	}

	// 3. Query the data
	var retrievedData []byte
	var retrievedID int
	err = db.QueryRow(fmt.Sprintf("SELECT ID, BlobData FROM %s WHERE ID = ?", tableName), insertID).Scan(&retrievedID, &retrievedData)
	if err != nil {
		t.Fatalf("Failed to query binary data: %v", err)
	}

	// 4. Assertions
	if retrievedID != insertID {
		t.Errorf("Retrieved ID mismatch: got %d, want %d", retrievedID, insertID)
	}
	if !reflect.DeepEqual(retrievedData, originalData) {
		t.Errorf("Retrieved binary data mismatch: got %v, want %v", retrievedData, originalData)
	} else {
		t.Logf("Successfully retrieved and verified binary data: %v", retrievedData)
	}

	// Test with NULL binary data
	nullInsertID := 2
	_, err = stmt.Exec(nullInsertID, nil) // Insert NULL
	if err != nil {
		t.Fatalf("Failed to insert NULL binary data: %v", err)
	}

	var nullRetrievedData []byte // Should remain nil or be an empty slice if DB represents NULL that way
	var nullRetrievedID int
	// For some drivers/DBs, scanning a NULL BLOB into []byte might result in nil, for others an empty slice.
	// The adodb driver, after fixes, should produce a nil []byte for NULL binary types.
	err = db.QueryRow(fmt.Sprintf("SELECT ID, BlobData FROM %s WHERE ID = ?", tableName), nullInsertID).Scan(&nullRetrievedID, &nullRetrievedData)
	if err != nil {
		t.Fatalf("Failed to query NULL binary data: %v", err)
	}
	if nullRetrievedID != nullInsertID {
		t.Errorf("Retrieved ID for NULL data mismatch: got %d, want %d", nullRetrievedID, nullInsertID)
	}
	if nullRetrievedData != nil {
		// Depending on exact DB behavior for NULL OLEObject, it might be empty slice.
		// For our driver, we expect nil if the database field is NULL.
		t.Errorf("Retrieved NULL binary data was not nil: got %v (length %d)", nullRetrievedData, len(nullRetrievedData))
	} else {
		t.Log("Successfully retrieved NULL binary data as nil.")
	}
}

// TestQueryStress performs a large number of simple queries to check for stability.
func TestQueryStress(t *testing.T) {
	// This test can use any valid DSN.
	// If testing against MDB, it might be slow. Consider a faster in-memory DB if available via ADO.
	// For now, using the same MDB DSN but with a very simple query.
	dsn := getTestMdbDsn(t)
	db, err := sql.Open("adodb", dsn)
	if err != nil {
		t.Fatalf("Failed to connect for stress test: %v. Ensure 'testole.mdb' exists or can be created, and ODBC drivers are installed.", err)
	}
	defer db.Close()

	// Ping to ensure connection is alive before stress test
	err = db.Ping()
	if err != nil {
		t.Fatalf("Ping failed before stress test: %v", err)
	}

	iterations := 1000 // Can be increased for more thorough stress testing
	t.Logf("Starting query stress test with %d iterations...", iterations)

	startTime := time.Now()

	for i := 0; i < iterations; i++ {
		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("Query failed at iteration %d: %v", i, err)
		}
		if result != 1 {
			t.Fatalf("Query result unexpected at iteration %d: got %d, want 1", i, result)
		}
		if i%100 == 0 && i > 0 {
			t.Logf("Stress test progress: %d iterations completed.", i)
		}
	}

	duration := time.Since(startTime)
	t.Logf("Query stress test completed %d iterations successfully in %v (avg %.2f ms/query).", iterations, duration, float64(duration.Milliseconds())/float64(iterations))
}

// TestMain is used to ensure CoInitialize/CoUninitialize are called if tests run in parallel
// or if specific test setup requires STA. For ADODB, CoInitialize is called in Open.
// func TestMain(m *testing.M) {
// 	  // ADODB driver calls CoInitialize per connection.
// 	  // If global STA was needed:
// 	  // runtime.LockOSThread()
// 	  // ole.CoInitializeEx(0, ole.COINIT_APARTMENTTHREADED) // Or COINIT_MULTITHREADED
// 	  // exitCode := m.Run()
// 	  // ole.CoUninitialize()
// 	  // os.Exit(exitCode)
// 	  m.Run()
// }

// Note on MDB creation for TestOLEObjectHandling:
// The test attempts to create `testole.mdb` if it doesn't exist by using "CreateDB" in the DSN.
// This requires the "Microsoft Access Driver (*.mdb, *.accdb)" to be installed and to support this feature.
// If automatic creation fails, an empty MDB file named `testole.mdb` must be manually placed
// in the package directory (where the tests are run) for `TestOLEObjectHandling` to succeed.
// The table `BinaryTable` and its data are created by the test itself.

// Example of how to manually create a suitable MDB (e.g. testole.mdb):
// 1. Open MS Access.
// 2. Create a new Blank Database. Save it as `testole.mdb` in the root of this Go package.
// 3. The test `TestOLEObjectHandling` will then attempt to create the required table (`BinaryTable`)
//    and insert/query data from it.

// The DSN used is: "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=./testole.mdb;"
// This means the MDB file must be named `testole.mdb` and be in the same directory
// where `go test` is executed for this package.
// The `getTestMdbDsn` function handles path construction.

// The stress test `TestQueryStress` also uses this MDB by default but performs
// a very simple query (`SELECT 1`) that does not depend on specific table structures
// beyond what the driver needs for basic query execution.
// If `testole.mdb` cannot be created or found, both tests relying on it will fail early
// during `sql.Open`.
// Ensure the system running the tests has the "Microsoft Access Driver (*.mdb, *.accdb)" installed.
// For 64-bit Go, a 64-bit Access driver (Microsoft Access Database Engine Redistributable) is needed.
// For 32-bit Go, a 32-bit Access driver is needed.
// Mismatches in architecture between Go test binary and ODBC driver will cause connection failures.
