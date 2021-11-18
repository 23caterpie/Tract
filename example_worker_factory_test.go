package tract_test

import (
	"database/sql"
	"fmt"

	tract "github.com/23caterpie/Tract"
)

var (
	_ tract.WorkerFactory[*DatabaseArgs, *DatabaseResults] = databaseWorkerFactory{}
	_ tract.Worker[*DatabaseArgs, *DatabaseResults]        = &databaseWorker{}
)

type (
	// TODO: use this and fix the example...
	DatabaseArgs struct {
		args []interface{}
	}

	DatabaseResults struct {
		results []interface{}
	}
)

func NewDatabaseWorkerFactory(
	driverName1, dataSourceName1, query1 string, resultCount1 int,
	driverName2, dataSourceName2, query2 string, resultCount2 int,
) (tract.WorkerFactory[*DatabaseArgs, *DatabaseResults], error) {
	db, err := sql.Open(driverName1, dataSourceName1)
	if err != nil {
		return nil, err
	}
	return databaseWorkerFactory{
		db:                   db,
		query1:               query1,
		query2:               query2,
		resultCount1:         resultCount1,
		resultCount2:         resultCount2,
		workerDriverName:     driverName2,
		workerDataSourceName: dataSourceName2,
	}, nil
}

type databaseWorkerFactory struct {
	db                                     *sql.DB
	query1, query2                         string
	resultCount1, resultCount2             int
	workerDriverName, workerDataSourceName string
}

func (f databaseWorkerFactory) MakeWorker() (tract.Worker[*DatabaseArgs, *DatabaseResults], error) {
	db, err := sql.Open(f.workerDriverName, f.workerDataSourceName)
	if err != nil {
		return nil, err
	}
	results := make([]interface{}, f.resultCount1)
	resultsPtrs := make([]interface{}, len(results))
	for i := range results {
		resultsPtrs[i] = &results[i]
	}
	return &databaseWorker{
		db:           f.db,
		query1:       f.query1,
		localDB:      db,
		query2:       f.query2,
		resultCount2: f.resultCount2,
		results:      results,
		resultsPtrs:  resultsPtrs,
	}, nil
}

func (f databaseWorkerFactory) Close() {
	f.db.Close()
}

type databaseWorker struct {
	// resources from factory
	db           *sql.DB
	query1       string
	query2       string
	resultCount2 int
	// local resources
	localDB              *sql.DB
	results, resultsPtrs []interface{}
}

func (w *databaseWorker) Work(_ *DatabaseArgs) (*DatabaseResults, bool) {
	err := w.db.QueryRow(w.query1).Scan(w.resultsPtrs...)
	if err != nil {
		// Handle error
		return nil, false
	}

	results := make([]interface{}, w.resultCount2)
	resultsPtrs := make([]interface{}, len(results))
	for i := range results {
		resultsPtrs[i] = &results[i]
	}

	err = w.localDB.QueryRow(w.query2, w.results...).Scan(resultsPtrs...)
	if err != nil {
		// Handle error
		return nil, false
	}
	return &DatabaseResults{
		results: results,
	}, true
}

func (w *databaseWorker) Close() {
	w.localDB.Close()
}

func ExampleWorkerFactory() {
	dbWorkerFactory, err := NewDatabaseWorkerFactory(
		"mysql", "mydatabase.internal", "SELECT value1, value2 FROM myTable1 LIMIT 1;", 2,
		"mysql", "mydatabase.internal", "SELECT value1, value2, value3 FROM myTable2 WHERE value1 = ? AND value2 = ? LIMIT 1;", 3,
	)
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

	resultRequest, ok := dbWorker.Work(&DatabaseArgs{})
	if !ok {
		// Handle problem
		return
	}

	for _, result := range resultRequest.results {
		fmt.Println(result)
	}
}
