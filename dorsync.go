package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
)

type RsyncPar struct {
	dnsname    string
	port       int
	remotepath string
	localpath  string
	logpath    string
}

func dorsync(group string) error {
	//check rsync binary
	if !viper.IsSet("RsyncBin") {
		return fmt.Errorf("Property 'RsyncBin' not found in config%s", ".")
	}
	rsyncBin := viper.GetString("RsyncBin")
	if _, err := os.Stat(rsyncBin); os.IsNotExist(err) {
		return fmt.Errorf("RsyncBin '%s' not exist", rsyncBin)
	}
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
	// check type/command of group
	groupTypeKey := "groups." + group + ".type"
	if !viper.IsSet(groupTypeKey) {
		return fmt.Errorf("Property 'type' for group '%s' not found in config", group)
	}
	rsyncCmdKey := "rsynccmd." + viper.GetString(groupTypeKey)
	if !viper.IsSet(rsyncCmdKey) {
		return fmt.Errorf("Rsync cmd '%s' not found in config", rsyncCmdKey)
	}
	rsyncCmdTmpl := viper.GetString(rsyncCmdKey)

	//enumerate servers
	for server := range servers {
		fmt.Printf("- Sync server '%s'\n", server)
		serverBackupPath := filepath.Join(groupBackupPath, server)
		// if backup path not exist - skip
		if _, err := os.Stat(serverBackupPath); os.IsNotExist(err) {
			fmt.Printf("  WARN: skip server '%s', path '%s' not exist\n", server, serverBackupPath)
			continue
		}
		dnsNameKey := keyOfServers + "." + server + ".host"
		if !viper.IsSet(dnsNameKey) {
			fmt.Printf("  WARN: skip server '%s', hostname not found in config\n", server)
			continue
		}
		rsyncPar := RsyncPar{dnsname: viper.GetString(dnsNameKey)}
		portKey := keyOfServers + "." + server + ".port"
		if !viper.IsSet(portKey) {
			rsyncPar.port = 22
		} else {
			rsyncPar.port = viper.GetInt(portKey)
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
			rsyncPar.localpath = filepath.Join(serverBackupPath, dir)
			// if backup path not exist - skip
			if _, err := os.Stat(rsyncPar.localpath); os.IsNotExist(err) {
				fmt.Printf("\tWARN: skip dir '%s', path '%s' not exist\n", dir, rsyncPar.localpath)
				continue
			}
			fmt.Printf("\tsync dir '%s' with par:\n", dir)
			// construct rsync parameters
			rsyncPar.remotepath = viper.GetString(keyOfDirs + "." + dir + ".remote")
			rsyncPar.logpath = filepath.Join(viper.GetString("LogPath"),
				strings.Join([]string{group, server, dir}, "-")+".log")
			rsyncArgString := fmt.Sprintf(rsyncCmdTmpl,
				rsyncPar.port, rsyncPar.logpath, rsyncPar.dnsname, rsyncPar.remotepath, rsyncPar.localpath)
			fmt.Println(rsyncArgString)
			rsyncArgs := strings.Fields(rsyncArgString)
			// if par ~= -e+ssh+-p+22+-i+rsbackup.rsa
			// change plus to space
			for i := 0; i < len(rsyncArgs); i++ {
				rsyncArgs[i] = strings.Replace(rsyncArgs[i], "+", " ", -1)
			}

			// execute rsync
			cmd := exec.Command(rsyncBin, rsyncArgs...)
			outputs, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Println("err: " + err.Error())
			}
			fmt.Println("out: " + string(outputs))

		}
	}
	return nil
}
