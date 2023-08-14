package timescaledb

import (
	"context"
	"datastore/common"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"os"
)

// TimescaleDB is an implementation of the StorageBackend interface that
// keeps data in a TimescaleDB database.
type TimescaleDB struct {
	Db *pgxpool.Pool
}

// Description ... (see documentation in StorageBackend interface)
func (sbe *TimescaleDB) Description() string {
	return "TimescaleDB database"
}

// openDB opens database identified by host/port/user/password/dbname.
// Returns (DB, nil) upon success, otherwise (..., error).
func openDB(host, port, user, password, dbname string, maxConns int32) (*pgxpool.Pool, error) {
	connString := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	connConf, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, err
	}
	connConf.MaxConns = maxConns

	// TODO: Read about context and how to correctly use it.
	db, err := pgxpool.ConnectConfig(context.Background(), connConf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}

	return db, nil
}

// NewTimescaleDB creates a new TimescaleDB instance.
// Returns (instance, nil) upon success, otherwise (..., error).
func NewTimescaleDB() (*TimescaleDB, error) {
	sbe := new(TimescaleDB)

	host := common.Getenv("TSDBHOST", "localhost")
	port := common.Getenv("TSDBPORT", "5433")
	user := common.Getenv("TSDBUSER", "postgres")
	password := common.Getenv("TSDBPASSWORD", "mysecretpassword")
	dbname := common.Getenv("TSDBDBNAME", "data")
	maxConns := int32(common.GetEnvInt("MAXCONNS", "100"))

	var err error

	sbe.Db, err = openDB(host, port, user, password, dbname, maxConns)
	if err != nil {
		return nil, fmt.Errorf("openDB() failed: %v", err)
	}

	if err = sbe.Db.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("sbe.Db.Ping() failed: %v", err)
	}

	return sbe, nil
}
