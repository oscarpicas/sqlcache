package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/oscarpicas/sqlcache"
	"github.com/oscarpicas/sqlcache/cache"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/dgraph-io/ristretto"
	_ "github.com/redis/go-redis/v9"
	_ "modernc.org/sqlite"
)

const (
	defaultMaxRowsToCache = 1000
)

func newRistrettoCache(maxRowsToCache int64) (cache.Cacher, error) {
	c, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 10 * maxRowsToCache,
		MaxCost:     maxRowsToCache,
		BufferItems: 64,
	})
	if err != nil {
		return nil, err
	}

	return sqlcache.NewRistretto(c), nil
}

/*
func newRedisCache() (cache.Cacher, error) {
	r := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{"127.0.0.1:6379"},
	})

	if _, err := r.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}

	return sqlcache.NewRedis(r, "sqc:"), nil
}
*/

func main() {

	acache, err := newRistrettoCache(defaultMaxRowsToCache)
	if err != nil {
		log.Fatalf("newRistrettoCache() failed: %v", err)
	}

	/*
		acache, err = newRedisCache()
		if err != nil {
			log.Fatalf("newRedisCache() failed: %v", err)
		}
	*/

	_, err = initDb()
	if err != nil {
		log.Fatalf("initDb() failed: %v", err)
	}

	interceptor, err := sqlcache.NewInterceptor(&sqlcache.Config{
		Cache: acache, // pick a Cacher interface implementation of your choice (redis or ristretto)
	})
	if err != nil {
		log.Fatalf("sqlcache.NewInterceptor() failed: %v", err)
	}

	defer func() {
		fmt.Printf("\nInterceptor metrics: %+v\n", interceptor.Stats())
	}()

	db, err := sql.Open("clickhouse", "tcp://localhost:9000?debug=true")
	if err != nil {
		log.Fatal(err)
	}

	// install the wrapper which wraps pgx driver
	sql.Register("clickhouse-cached", interceptor.Driver(db.Driver()))

	if err := run(); err != nil {
		log.Fatalf("run() failed: %v", err)
	}
}

func initDb() (*sql.DB, error) {
	// create a table in Sqlite memory database
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}

	// create a table in Sqlite
	_, err = db.Exec(`
    		CREATE TABLE example (
    		    				user String,
    		    				timestamp DateTime,
    		    				value Int32
			    			)`)
	if err != nil {
		return nil, err
	}

	// insert some example
	_, err = db.Exec(`INSERT INTO example (user, timestamp, value) VALUES
                                                				('user 1', '2021-01-01 00:00:00', 100),
                                                				('user 2', '2021-01-01 00:00:00', 200),
                                                				('user 3', '2021-01-01 00:00:00', 300),
                                                				('user 4', '2021-01-01 00:00:00', 400),
                                                				('user 5', '2021-01-01 00:00:00', 500)
                                                `)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func run() error {

	db, err := sql.Open("clickhouse-cached",
		"tcp://localhost:9000?debug=true")
	if err != nil {
		return err
	}
	defer db.Close()

	if err = db.PingContext(context.TODO()); err != nil {
		return fmt.Errorf("db.PingContext() failed: %w", err)
	}

	for i := 0; i < 15; i++ {
		start := time.Now()
		if err := doQuery(db); err != nil {
			return fmt.Errorf("doQuery() failed: %w", err)
		}
		fmt.Printf("i=%d; t=%s\n", i, time.Since(start))
		time.Sleep(1 * time.Second)
	}

	return nil
}

func doQuery(db *sql.DB) error {

	rows, err := db.QueryContext(context.TODO(), `
		-- @cache-ttl 30
		-- @cache-max-rows 1000
		SELECT user, timestamp FROM example WHERE value > $1`, 80)
	if err != nil {
		return fmt.Errorf("db.QueryContext() failed: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var name string
		var pages time.Time
		if err := rows.Scan(&name, &pages); err != nil {
			return fmt.Errorf("rows.Scan() failed: %w", err)
		}
		count++
	}
	fmt.Printf("count=%d\n", count)

	return rows.Err()
}
