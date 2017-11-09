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

var cleanup = "TRUNCATE TABLE jiajzhou.T1"
var query = "INSERT INTO jiajzhou.T1 (id) VALUES (?)"

func sendRequest(db *sql.DB) {
	currentOps := atomic.AddUint32(&ops, 1)

	_, err := db.Exec(query, currentOps)

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

	flag.Parse()

	log.Printf("Current concurrency: %d\n", *concurrency)
	log.Printf("Current max QPS: %d\n", *maxQPS)
	log.Printf("Testing agains: %s\n", *url)

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

	db.SetMaxOpenConns(1000)
	db.SetMaxIdleConns(1000)

	if _, err := db.Exec(cleanup); err != nil {
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

				sendRequest(db)
			}
		}()
	}
	// make it never end
	<-fin
}
