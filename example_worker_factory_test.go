package tract_test

import (
	"context"
	"database/sql"
	"fmt"

	tract "github.com/23caterpie/Tract"
)

var (
	_ tract.WorkerFactory[DatabaseArgs, DatabaseResults, *databaseWorker] = databaseWorkerFactory{}
	_ tract.Worker[DatabaseArgs, DatabaseResults]                         = &databaseWorker{}
)

type (
	DatabaseArgs    []interface{}
	DatabaseResults []interface{}
)

func NewDatabaseWorkerFactory(
	stmt *sql.Stmt, columnCount int,
) tract.WorkerFactory[DatabaseArgs, DatabaseResults, *databaseWorker] {
	return databaseWorkerFactory{
		stmt:        stmt,
		columnCount: columnCount,
	}
}

type databaseWorkerFactory struct {
	stmt        *sql.Stmt
	columnCount int
}

func (f databaseWorkerFactory) MakeWorker() (*databaseWorker, error) {
	results := make([]interface{}, f.columnCount)
	resultsPtrs := make([]interface{}, len(results))
	for i := range results {
		resultsPtrs[i] = &results[i]
	}
	return &databaseWorker{
		stmt:        f.stmt,
		results:     results,
		resultsPtrs: resultsPtrs,
	}, nil
}

func (f databaseWorkerFactory) Close() {}

type databaseWorker struct {
	// resources from factory
	stmt *sql.Stmt
	// local resources
	results, resultsPtrs []interface{}
}

func (w *databaseWorker) Work(ctx context.Context, args DatabaseArgs) (DatabaseResults, bool) {
	err := w.stmt.QueryRowContext(ctx, args...).Scan(w.resultsPtrs...)
	if err != nil {
		// Handle error
		return nil, false
	}

	return append([]interface{}{}, w.results...), true
}

func (w *databaseWorker) Close() {}

func ExampleWorkerFactory() {
	db, err := sql.Open("mysql", "mydatabase.internal")
	if err != nil {
		// Handle error
		return
	}

	stmt, err := db.Prepare("SELECT value1, value2 FROM myTable1 WHERE filter = ? LIMIT 1;")
	if err != nil {
		// Handle error
		return
	}

	dbWorkerFactory := NewDatabaseWorkerFactory(stmt, 2)

	dbWorker, err := dbWorkerFactory.MakeWorker()
	if err != nil {
		// Handle error
		return
	}
	defer dbWorker.Close()

	results, ok := dbWorker.Work(context.Background(), []interface{}{"foobar"})
	if !ok {
		// Handle problem
		return
	}

	for _, result := range results {
		fmt.Println(result)
	}
}
