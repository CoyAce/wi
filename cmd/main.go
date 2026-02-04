package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/CoyAce/wi"
)

var (
	address = flag.String("a", "0.0.0.0:52000", "listen address")
)

func main() {
	flag.Parse()
	fmt.Println("address:", *address)
	fmt.Println("server mode")
	s := wi.Server{}
	log.Fatal(s.ListenAndServe(*address))
}
