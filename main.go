package main

import (
	"database/sql"
	"flag"
	"log"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var ops uint32

var cleanup = "DROP TABLE IF EXISTS jiajzhou.T1"
var create1 = `
CREATE TABLE T1 (
	id bigint(20) NOT NULL,
	PRIMARY KEY (id)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`
var create2 = `
CREATE TABLE T1 (
	id bigint(20) NOT NULL AUTO_INCREMENT,
	PRIMARY KEY (id)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
`

var query = "INSERT INTO jiajzhou.T1 (id) VALUES (?)"

func sendRequest(db *sql.DB, autoInc bool) {
	currentOps := atomic.AddUint32(&ops, 1)

	var value sql.NullInt64
	if autoInc {
		value = sql.NullInt64{}
	} else {
		value = sql.NullInt64{Int64: (int64)(currentOps), Valid: true}
	}

	_, err := db.Exec(query, value)

	if err != nil {
		log.Println("err", err)
	} else {
		if currentOps%1000 == 0 {
			log.Printf("%d queries executed\n", currentOps)
		}
	}
}

func main() {
	concurrency := flag.Int("concurrency", 10, "Concurent connection to the server")
	maxQPS := flag.Int("maxQPS", 100, "Maximum QPS the client will generate")
	url := flag.String("url", "", "DB url")
	start := flag.Int("startId", 0, "Starting Id")
	autoInc := flag.Bool("autoInc", false, "Use auto inc table")
	conn := flag.Int("conn", 100, "Max conn count")

	flag.Parse()

	ops = (uint32)(*start)

	log.Printf("Current concurrency: %d\n", *concurrency)
	log.Printf("Current max QPS: %d\n", *maxQPS)
	log.Printf("Testing agains: %s\n", *url)
	log.Printf("Starting id: %d\n", ops)
	log.Printf("AutoInc: %v\n", *autoInc)
	log.Printf("MaxConn: %d\n", *conn)

	fin := make(chan bool)
	bucket := make(chan bool, *maxQPS)

	go func() {
		// QPS calc
		for {
			currentOps := ops
			time.Sleep(time.Second)
			var qps = (int32)(ops - currentOps)
			log.Printf("QPS: %d\n", qps)
		}
	}()

	go func() {
		for {
			for i := 0; i < *maxQPS; i++ {
				select {
				case bucket <- true:
				default:
				}
			}
			time.Sleep(time.Second)
		}
	}()

	db, err := sql.Open("mysql", *url)

	if err != nil {
		log.Fatal(err)
		panic(err)
	}

	db.SetMaxOpenConns(*conn)
	db.SetMaxIdleConns(*conn)

	if _, err := db.Exec(cleanup); err != nil {
		log.Fatal(err)
		panic(err)
	}

	var create string
	if *autoInc {
		create = create2
	} else {
		create = create1
	}

	if _, err := db.Exec(create); err != nil {
		log.Fatal(err)
		panic(err)
	}

	for i := 0; i < *concurrency; i++ {
		go func() {

			if err := db.Ping(); err != nil {
				log.Fatal(err)
				panic(err)
			}

			for {
				<-bucket

				sendRequest(db, *autoInc)
			}
		}()
	}
	// make it never end
	<-fin
}
