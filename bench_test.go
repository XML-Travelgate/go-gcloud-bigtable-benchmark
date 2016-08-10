package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"

	"google.golang.org/cloud"
	"google.golang.org/cloud/bigtable"
)

const (
	_user          = "jumbo"
	_prv           = "TTHOT"
	_hot           = "529612"
	_project       = "xtg-hub-cache"
	_instance      = "hubcache-bigtable"
	_tableName     = "hotel-cache"
	_pathToKeyFile = "C:/xtg/go/src/github.com/XML-Travelgate/go-gcloud-bigtable-benchmark/xtg-hub-cache-c768086c5bd8.json"
)

var (
	btClient    *bigtable.Client
	tbl         *bigtable.Table
	rowKeys10   []string //TTHOT|jumbo|529612|20161114|1|3030.3030|SP
	rowKeys100  []string
	rowKeys1000 []string
)

func init() {
	println("init")
	log.Println("Reading credentials file.")
	jsonKey, _ := ioutil.ReadFile(_pathToKeyFile)
	config, _ := google.JWTConfigFromJSON(jsonKey, bigtable.Scope)

	log.Println("New BigTable Client")
	ctx := context.Background()
	btClient, _ = bigtable.NewClient(ctx, _project, _instance, cloud.WithTokenSource(config.TokenSource(ctx)))
	tbl = btClient.Open(_tableName)

	rowKeys10 = make([]string, 10)
	rowKeys100 = make([]string, 100)
	rowKeys1000 = make([]string, 1000)

	log.Printf("Generating dummy RowKeys")
	for i := 0; i < 1000; i++ {
		//TTHOT|jumbo|529612|20161114|1|3030.3030|SP
		rowKey := fmt.Sprintf("%s|%s|%s|%s|%v|%s|%s", _prv, _user, _hot, "20161114", i+1, "3030.3030", "SP")
		if i < 10 {
			rowKeys10[i] = rowKey
		}
		if i < 100 {
			rowKeys100[i] = rowKey
		}
		if i < 1000 {
			rowKeys1000[i] = rowKey
		}
	}
	log.Printf("Dummy RowKeys generated OK")
}

func readRow(b *testing.B) {
	ctx := context.Background()
	_, err := tbl.ReadRow(ctx, rowKeys10[0])
	if err != nil {
		log.Fatalf("Could not read row with key %s: %v", rowKeys10[0], err)
	}

	/*if row != nil {
		log.Printf("\t%s = %s\n", rowKeys10[0], string(row["RS"][0].Value))
	} else {
		log.Printf("\t%s = null\n", rowKeys10[0])
	}
	*/
}

func readRowsv2(rowsToRead []string) {
	ctx, _ := context.WithTimeout(context.Background(), 2*time.Second)

	readRows := func(rows []string) ([]bigtable.Row, error) {
		results := make([]bigtable.Row, len(rows))
		errors := make([]error, len(rows))
		var wg sync.WaitGroup
		for i, row := range rows {
			wg.Add(1)
			go func(i int, row string) {
				defer wg.Done()
				results[i], errors[i] = tbl.ReadRow(ctx, row, bigtable.RowFilter(bigtable.LatestNFilter(1)))
			}(i, row)
		}
		wg.Wait()
		for _, err := range errors {
			if err != nil {
				return nil, err
			}
		}
		return results, nil
	}

	// For each query word, get the list of documents containing it.
	_, err := readRows(rowsToRead)
	if err != nil {
		log.Fatalf("Error reading index" + err.Error())
		return
	}

}

func readRows10v2(b *testing.B) {
	readRowsv2(rowKeys10)
}

func readRows100v2(b *testing.B) {
	readRowsv2(rowKeys100)
}

func readRows1000v2(b *testing.B) {
	readRowsv2(rowKeys1000)
}

func readRows10v1(b *testing.B) {
	/*	ctx := context.Background()
		   tbl.ReadRows(ctx, )

			resp := make(chan *Types.T_dbRQRS, len(diccDbRQ))

		   rowRange := RowRange(  st)
	*/
}

func BenchmarkReadRow1(b *testing.B)       { readRow(b) }
func BenchmarkReadRows10v2(b *testing.B)   { readRows10v2(b) }
func BenchmarkReadRows100v2(b *testing.B)  { readRows100v2(b) }
func BenchmarkReadRows1000v2(b *testing.B) { readRows1000v2(b) }
