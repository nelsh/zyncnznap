package main

import (
	"fmt"

	"github.com/spf13/viper"
)

func main() {
	// Read configuration
	viper.SetConfigFile("zs.conf")
	viper.SetConfigType("toml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	for s := range viper.GetStringMap("group") {
		fmt.Print("group:" + s)
		fmt.Println(", type=" + viper.GetString("group."+s+".type"))
		for s := range viper.GetStringMap("group." + s + ".servers") {
			fmt.Println("\tserver:" + s)
		}
	}
}
