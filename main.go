package main

import (
	"fmt"
	"github.com/cockroachdb/pebble"
)

func main() {
	db, err := pebble.Open("test", &pebble.Options{})
	if err != nil {
		fmt.Println("error")
	}
	defer db.Close()
	fmt.Println(db.Metrics())
}
