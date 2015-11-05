package main

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
)

func dorsync() {
	if _, err := os.Stat(viper.GetString("BackupPath")); os.IsNotExist(err) {
		panic(fmt.Errorf("Fatal error: %s \n", err))
	}
	groups := viper.GetStringMap("groups")
	if len(groups) == 0 {
		fmt.Println("Empty group list")
		return
	}
	for group := range groups {
		fmt.Printf("%s\n", group)
		for server := range viper.GetStringMap("groups." + group + ".servers") {
			fmt.Printf("\t%s\n", server)
			for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
				fmt.Printf("\t\t%s\n", dir)
			}
		}
	}
}
