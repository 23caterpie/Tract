package tract_test

import (
	"context"
	"database/sql"
	"fmt"

	"git.dev.kochava.com/ccurrin/tract"
)

var (
	_ tract.WorkerFactory = databaseWorkerFactory{}
	_ tract.Worker        = &databaseWorker{}
)

type DatabaseResultsKey struct{}

func NewDatabaseWorkerFactory(driverName, dataSourceName, query string, resultCount int) (tract.WorkerFactory, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return databaseWorkerFactory{
		db:          db,
		query:       query,
		resultCount: resultCount,
	}, nil
}

type databaseWorkerFactory struct {
	db          *sql.DB
	query       string
	resultCount int
}

func (f databaseWorkerFactory) MakeWorker() (tract.Worker, error) {
	return &databaseWorker{
		db:    f.db,
		query: f.query,
	}, nil
}

func (f databaseWorkerFactory) Close() {
	f.db.Close()
}

type databaseWorker struct {
	// resources from factory
	db          *sql.DB
	query       string
	resultCount int
}

func (w *databaseWorker) Work(r tract.Request) (tract.Request, bool) {
	results := make([]interface{}, w.resultCount)
	resultsPtrs := make([]interface{}, len(results))
	for i := range results {
		resultsPtrs[i] = &results[i]
	}
	err := w.db.QueryRow(w.query).Scan(resultsPtrs...)
	if err != nil {
		// Handle error
		return r, false
	}
	return context.WithValue(r, DatabaseResultsKey{}, results), true
}

func (w *databaseWorker) Close() {
	// No resources for the worker to close.
	// The database will be closed by the factory.
	// If we closed it here, other workers made by the factory may try
	// to query using a closed database connection.
}

func ExampleWorkerFactory() {
	dbWorkerFactory, err := NewDatabaseWorkerFactory("mysql", "mydatabase.internal", "SELECT value1, value2 FROM myTable LIMIT 1;", 2)
	if err != nil {
		// Handle error
		return
	}
	defer dbWorkerFactory.Close()

	dbWorker, err := dbWorkerFactory.MakeWorker()
	if err != nil {
		// Handle error
		return
	}
	defer dbWorker.Close()

	resultRequest, ok := dbWorker.Work(context.Background())
	if !ok {
		// Handle problem
		return
	}

	results, ok := resultRequest.Value(DatabaseResultsKey{}).([]interface{})
	if !ok {
		// Handle problem
		return
	}
	for _, result := range results {
		fmt.Println(result)
	}
}
