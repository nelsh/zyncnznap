package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

func dorsync(group string) error {
	// check group exist
	if !viper.IsSet("groups." + group) {
		return fmt.Errorf("Group '%s' not found in config", group)
	}
	// check backup path for group
	groupBackupPath := filepath.Join(viper.GetString("BackupPath"), group)
	if _, err := os.Stat(groupBackupPath); os.IsNotExist(err) {
		return fmt.Errorf("Path '%s' for group '%s' not exist", groupBackupPath, group)
	}
	// check list of servers
	keyOfServers := "groups." + group + ".servers"
	servers := viper.GetStringMap(keyOfServers)
	if len(servers) == 0 {
		return fmt.Errorf("Empty server list of group '%s'", group)
	}

	//enumerate servers
	for server := range servers {
		fmt.Printf("- Sync server '%s'\n", server)
		serverBackupPath := filepath.Join(groupBackupPath, server)
		// if backup path not exist - skip
		if _, err := os.Stat(serverBackupPath); os.IsNotExist(err) {
			fmt.Printf("  WARN: skip server '%s', path '%s' not exist\n", server, serverBackupPath)
			continue
		}
		// check list of dirs
		keyOfDirs := keyOfServers + "." + server + ".dirs"
		dirs := viper.GetStringMap(keyOfDirs)
		if len(dirs) == 0 {
			fmt.Printf("  WARN: skip server '%s', empty dir list\n", server)
			continue
		}
		// enumerate dirs
		for dir := range dirs {
			dirBackupPath := filepath.Join(serverBackupPath, dir)
			// if backup path not exist - skip
			if _, err := os.Stat(dirBackupPath); os.IsNotExist(err) {
				fmt.Printf("\tWARN: skip dir '%s', path '%s' not exist\n", dir, dirBackupPath)
				continue
			}
			fmt.Printf("\tsync dir '%s'\n", dir)
			fmt.Println(viper.GetString(keyOfDirs + "." + dir + ".remote"))
		}
	}
	return nil
}
