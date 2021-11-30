package tract_test

import (
	"database/sql"
	"fmt"

	tract "github.com/23caterpie/Tract"
)

var (
	_ tract.WorkerFactory[DatabaseArgs, DatabaseResults] = databaseWorkerFactory{}
	_ tract.Worker[DatabaseArgs, DatabaseResults]        = &databaseWorker{}
)

type (
	DatabaseArgs    []interface{}
	DatabaseResults []interface{}
)

func NewDatabaseWorkerFactory(
	stmt *sql.Stmt, columnCount int,
) tract.WorkerFactory[DatabaseArgs, DatabaseResults] {
	return databaseWorkerFactory{
		stmt:        stmt,
		columnCount: columnCount,
	}
}

type databaseWorkerFactory struct {
	stmt        *sql.Stmt
	columnCount int
}

func (f databaseWorkerFactory) MakeWorker() (tract.WorkerCloser[DatabaseArgs, DatabaseResults], error) {
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

func (w *databaseWorker) Work(args DatabaseArgs) (DatabaseResults, bool) {
	err := w.stmt.QueryRow(args...).Scan(w.resultsPtrs...)
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

	results, ok := dbWorker.Work([]interface{}{"foobar"})
	if !ok {
		// Handle problem
		return
	}

	for _, result := range results {
		fmt.Println(result)
	}
}
