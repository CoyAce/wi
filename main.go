package main

import (
	"flag"
	"fmt"
	"log"
)

var (
	address = flag.String("a", "0.0.0.0:52000", "listen address")
)

func main() {
	flag.Parse()
	fmt.Println("address:", *address)
	fmt.Println("server mode")
	s := Server{}
	log.Fatal(s.ListenAndServe(*address))
}
