package main

import (
	"flag"
	"fmt"
	"io"
	"log"
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
	logFileName := filepath.Join(
		viper.GetString("LogPath"),
		strings.Split(filepath.Base(os.Args[0]), ".")[0]+".log")
	/*
		DO
		 - delete next string
		 - ini-file for common settings github.com/sasbury/mini
		 - toml-file for backup desciption
	*/
	os.Remove(logFileName)
	logFile, err := os.OpenFile(logFileName,
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664)
	if err != nil {
		panic(err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	switch task {
	case "check":
		log.Println("INFO: Start task Check")
		checkcreate()
	case "sync":
		log.Println("INFO: Start task Sync")
		if err := dorsync(group); err != nil {
			log.Printf("Exit with fatal error: %s\n", err)
			/*
				DO - Sent message
			*/
			os.Exit(1)
		}
	case "snap":
		log.Println("INFO: Start task Snap")
	}

	log.Println("INFO: Stop Successfull")
	log.Println("----------------------")
}
