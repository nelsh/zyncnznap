package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

func dozip(group string) {
	hostname := getHostName()
	/*
		RUN CHECK'S
	*/
	// if next check's = failed
	// - send notice and exit
	exitWithMailMsg := func(msg string) {
		log.Printf("Exit with fatal error: %s\n", msg)
		subj := fmt.Sprintf("zync'n'znap %s/%s: Exit with fatal error",
			strings.ToUpper(hostname), strings.ToUpper(group))
		if err := sendReport(subj, msg); err != nil {
			log.Printf("WARN: '%s'", err)
		}
		os.Exit(1)
	}
	// check group exist
	if !viper.IsSet("groups." + group) {
		exitWithMailMsg(fmt.Sprintf("Group '%s' not found in config", group))
	}
	// check path for zip archive
	zipPath := viper.GetString("ZipPath")
	if _, err := os.Stat(zipPath); os.IsNotExist(err) {
		exitWithMailMsg(fmt.Sprintf("Path '%s' for zip not exist", zipPath))
	}
	// check backup path for group
	groupBackupPath := filepath.Join(viper.GetString("BackupPath"), group)
	if _, err := os.Stat(groupBackupPath); os.IsNotExist(err) {
		exitWithMailMsg(fmt.Sprintf("Path '%s' for group '%s' not exist", groupBackupPath, group))
	}
	// check list of servers
	keyOfServers := "groups." + group + ".servers"
	servers := viper.GetStringMap(keyOfServers)
	if len(servers) == 0 {
		exitWithMailMsg(fmt.Sprintf("Empty server list of group '%s'", group))
	}
	/* end common check's */

	/*
		MAIN PROCEDURE
	*/
	dateString := time.Now().Format("20060102")
	//
	// enumerate servers
	//
	for server := range servers {
		serverBackupPath := filepath.Join(groupBackupPath, server)
		if _, err := os.Stat(serverBackupPath); os.IsNotExist(err) {
			log.Printf("  WARN: skip server '%s', path '%s' not exist\n", server, serverBackupPath)
			continue
		}
		// check list of dirs
		keyOfDirs := keyOfServers + "." + server + ".dirs"
		dirs := viper.GetStringMap(keyOfDirs)
		if len(dirs) == 0 {
			log.Printf("  WARN: skip server '%s', empty dir list\n", server)
			continue
		}
		//
		// enumerate dirs
		//
		for dir := range dirs {
			dirBackupPath := filepath.Join(serverBackupPath, dir)
			// if backup path not exist - skip
			if _, err := os.Stat(dirBackupPath); os.IsNotExist(err) {
				log.Printf("  WARN: skip dir '%s', path '%s' not exist\n", dir, dirBackupPath)
				continue
			}
			// if packtozip = "false" for this dir - skip
			packtozip := true
			keyOfZip := keyOfDirs + "." + dir + ".packtozip"
			if viper.IsSet(keyOfZip) {
				packtozip = viper.GetBool(keyOfZip)
			}
			if !packtozip {
				log.Printf("  WARN: skip dir '%s', packtozip = '%t'\n", dir, packtozip)
				continue
			}
			zipArgsString := fmt.Sprintf("-r -lf %s %s %s",
				filepath.Join(viper.GetString("LogPath"), strings.Join([]string{"zip", group, server, dir}, "-")+".log"),
				filepath.Join(viper.GetString("ZipPath"), strings.Join([]string{group, server, dir, dateString}, "_")+".zip"),
				dirBackupPath)
			log.Printf("\tzip dir '%s' with par: %s\n", dir, zipArgsString)
			zipArgs := strings.Fields(zipArgsString)
			// execute zip
			// timeStart := time.Now()
			cmd := exec.Command("zip", zipArgs...)
			outputs, err := cmd.CombinedOutput()
			if err != nil {
				log.Println("\t\tzip output:\n" + string(outputs))
			}
			// timeStop := time.Now()
		}
	}
}
