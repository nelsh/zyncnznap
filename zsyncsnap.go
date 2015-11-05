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
	checkonly bool // Optional for task 'check'
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
	flag.Usage = func() {
		fmt.Printf("Usage:\n")
		fmt.Printf("  %s -task=check [-checkonly=false]\n", filepath.Base(os.Args[0]))
		fmt.Printf("  %s -task=sync [-group=<name>]\n", filepath.Base(os.Args[0]))
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
	case "snap":
		fmt.Println("Run task Snap")
	}
	/*
		for group := range viper.GetStringMap("groups") {
			fmt.Printf("%s\n", group)
			for server := range viper.GetStringMap("groups." + group + ".servers") {
				fmt.Printf("\t%s\n", server)
				for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
					fmt.Printf("\t\t%s\n", dir)
				}
			}
		}
	*/
}
