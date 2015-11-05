package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

var (
	task      string
	checkonly bool   // Optional for task 'check'
	group     string // Required for task 'sync'
)

func init() {
	/*
		Read command-line options and set usage information
	*/
	flag.StringVar(&task, "task", "",
		"Required. One of the options: check | sync | snap")
	flag.BoolVar(&checkonly, "checkonly", true,
		`Optional for task 'check'.
        Set 'false' for creating ZFS partitions from config`)
	flag.StringVar(&group, "group", "",
		"Required. Name of backup group.")
	flag.Usage = func() {
		fmt.Printf("Usage:\n")
		fmt.Printf("  %s -task=check [-checkonly=false]\n", filepath.Base(os.Args[0]))
		fmt.Printf("  %s -task=sync -group=<name>\n", filepath.Base(os.Args[0]))
		fmt.Printf("  %s -task=snap\n\n", filepath.Base(os.Args[0]))
		flag.PrintDefaults()
		fmt.Println("")
	}
	flag.Parse()
	if task == "" || (task != "check" && task != "sync" && task != "snap") {
		fmt.Printf("task '%s' not set or not found\n", task)
		flag.Usage()
		os.Exit(1)
	}
	if task == "sync" && group == "" {
		fmt.Println("not set group for task 'sync'")
		flag.Usage()
		os.Exit(1)
	}
	/*
		Read configuration
	*/
	viper.SetConfigFile(strings.Split(filepath.Base(os.Args[0]), ".")[0] + ".toml")
	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}
}

func main() {
	switch task {
	case "check":
		fmt.Println("Run task Check")
		checkcreate()
	case "sync":
		fmt.Println("Run task Sync")
		dorsync()
	case "snap":
		fmt.Println("Run task Snap")
	}
}
