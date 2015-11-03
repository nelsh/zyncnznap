package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

var (
	mode      string
	checkonly bool
)

func init() {
	/*
		Read command-line options and set usage information
	*/
	flag.StringVar(&mode, "mode", "",
		"Required. One of the options: check | sync | snap")
	flag.BoolVar(&checkonly, "checkonly", true,
		`Optional for mode 'check'.
        Set 'false' for creating ZFS partitions from config`)
	flag.Usage = func() {
		fmt.Printf("Usage:\n")
		fmt.Printf("  %s -mode=check [-checkonly=false]\n", filepath.Base(os.Args[0]))
		fmt.Printf("  %s -mode=sync [-group=<name>]\n", filepath.Base(os.Args[0]))
		fmt.Printf("  %s -mode=snap\n\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		fmt.Println("")
	}
	flag.Parse()
	if mode == "" || (mode != "check" && mode != "sync" && mode != "snap") {
		fmt.Printf("mode '%s' not set or not found\n", mode)
		flag.Usage()
	}
}

func main() {
}
