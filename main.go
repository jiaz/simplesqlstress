package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/satori/go.uuid"

	_ "github.com/go-sql-driver/mysql"
)

var ops uint32

var cleanup = "DROP TABLE IF EXISTS jiajzhou.eievents"
var create = `
CREATE TABLE jiajzhou.eievents (
	EVENT_ID varchar(64) COLLATE utf8_bin NOT NULL,
	INSERT_DATE datetime(3) NOT NULL,
	PRIMARY KEY (EVENT_ID,INSERT_DATE)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
  PARTITION BY RANGE  COLUMNS(INSERT_DATE)
  (
	PARTITION EIEVENTS_20171115 VALUES LESS THAN ('2017-11-15'),
	PARTITION EIEVENTS_20171116 VALUES LESS THAN ('2017-11-16'),
	PARTITION EIEVENTS_20171117 VALUES LESS THAN ('2017-11-17')
  )
`

var query = "INSERT INTO jiajzhou.eievents (EVENT_ID, INSERT_DATE) VALUES (?, ?)"

func generateID() string {
	u1 := uuid.NewV4()
	t := time.Now()

	return fmt.Sprintf("%d%d%d%d%d%d-%s", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), u1)
}

func sendRequest(db *sql.DB, autoInc bool) {
	currentOps := atomic.AddUint32(&ops, 1)

	var id = generateID()

	log.Printf("generated id: %s\n", id)

	_, err := db.Exec(query, id, time.Now().String())

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
