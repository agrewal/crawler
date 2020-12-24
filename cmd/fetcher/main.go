package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/agrewal/fetcher"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetLevel(log.DebugLevel)
}

func main() {
	fetchCmd := flag.NewFlagSet("fetch", flag.ExitOnError)
	storeDir := fetchCmd.String("s", "", "Store directory (required)")
	conc := fetchCmd.Int("c", 5, "Concurrency (default: 5)")

	getCmd := flag.NewFlagSet("fetch", flag.ExitOnError)
	getStoreDir := getCmd.String("s", "", "Store directory (required)")

	if len(os.Args) < 2 {
		fmt.Println("Expected a subcommand")
	}

	switch os.Args[1] {
	case "fetch":
		fetchCmd.Parse(os.Args[2:])
		if len(*storeDir) == 0 {
			fmt.Println("Must specify store directory")
			os.Exit(1)
		}
		store, err := fetcher.NewPebbleStore(*storeDir)
		if err != nil {
			panic(err)
		}
		defer store.Close()
		fer := fetcher.NewStoreBackedFetcher(store, fetcher.NewFetcher(), 1*time.Minute)
		urlChan := fetcher.ReaderToStringFetchable(context.Background(), os.Stdin, 1)
		fetcher.FetchConcurrentlyWait(fer, urlChan, *conc)

	case "get":
		getCmd.Parse(os.Args[2:])
		if len(*getStoreDir) == 0 {
			fmt.Println("Must specify store directory")
			os.Exit(1)
		}
		store, err := fetcher.NewPebbleStore(*getStoreDir)
		if err != nil {
			panic(err)
		}
		defer store.Close()
		if len(getCmd.Args()) != 1 {
			fmt.Println("Must specify the url to get")
			os.Exit(1)
		}
		cu, closer, err := store.Get(getCmd.Args()[0])
		if err != nil {
			panic(err)
		}
		defer closer.Close()
		fmt.Printf("Crawl time: %s\n", time.Unix(cu.CrawlTs(), 0))
		fmt.Printf("Body size: %d bytes\n", len(cu.Body()))

	default:
		fmt.Printf("Unknown subcommand %s\n", os.Args[1])
		os.Exit(1)
	}
}
