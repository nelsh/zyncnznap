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

type RsyncPar struct {
	port       int
	dnsname    string
	remotepath string
	localpath  string
	logpath    string
}

type RsyncRpt struct {
	serverdir      string
	numFilesTotal  string
	numFilesRcvd   string
	sizeFilesTotal string
	sizeFilesRcvd  string
	howlong        time.Duration
}

type Totals struct {
	warnMsg        string
	warnNum        int
	rsyncErrMsg    string
	rsyncErrorTask int
	rsyncTotalTask int
	report         string
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

	// it's already running?
	/*
		DO
	*/

	totals := Totals{
		report: fmt.Sprintf("%-18s | %16s | %22s | %7s |\n\n",
			"Server/Dir", "Files recv/total", "Size in Kb recv/total", "Minutes"),
	}
	//enumerate servers
	for server := range servers {
		log.Printf("- Sync server '%s'\n", server)
		serverBackupPath := filepath.Join(groupBackupPath, server)
		// if backup path not exist - skip
		if _, err := os.Stat(serverBackupPath); os.IsNotExist(err) {
			msg := fmt.Sprintf("  WARN: skip server '%s', path '%s' not exist\n", server, serverBackupPath)
			logTotals(&totals, msg)
			continue
		}
		dnsNameKey := keyOfServers + "." + server + ".host"
		if !viper.IsSet(dnsNameKey) {
			msg := fmt.Sprintf("  WARN: skip server '%s', hostname not found in config\n", server)
			logTotals(&totals, msg)
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
			msg := fmt.Sprintf("  WARN: skip server '%s', empty dir list\n", server)
			logTotals(&totals, msg)
			continue
		}
		// enumerate dirs
		for dir := range dirs {
			rsyncPar.localpath = filepath.Join(serverBackupPath, dir)
			// if backup path not exist - skip
			if _, err := os.Stat(rsyncPar.localpath); os.IsNotExist(err) {
				msg := fmt.Sprintf("  WARN: skip dir '%s', path '%s' not exist\n", dir, rsyncPar.localpath)
				logTotals(&totals, msg)
				continue
			}
			log.Printf("\tsync dir '%s' with par:\n", dir)
			// construct rsync parameters
			rsyncPar.remotepath = viper.GetString(keyOfDirs + "." + dir + ".remote")
			rsyncPar.logpath = filepath.Join(viper.GetString("LogPath"),
				strings.Join([]string{group, server, dir}, "-")+".log")
			rsyncArgString := fmt.Sprintf(rsyncCmdTmpl,
				rsyncPar.port, rsyncPar.logpath, rsyncPar.dnsname, rsyncPar.remotepath, rsyncPar.localpath)
			log.Println(rsyncArgString)
			rsyncArgs := strings.Fields(rsyncArgString)
			// if par ~= -e+ssh+-p+22+-i+rsbackup.rsa
			// change plus to space
			for i := 0; i < len(rsyncArgs); i++ {
				rsyncArgs[i] = strings.Replace(rsyncArgs[i], "+", " ", -1)
			}

			// execute rsync
			timeStart := time.Now()
			totals.rsyncTotalTask++
			cmd := exec.Command(rsyncBin, rsyncArgs...)
			outputs, err := cmd.CombinedOutput()
			if err != nil {
				totals.rsyncErrorTask++
				totals.rsyncErrMsg += fmt.Sprintf("  %s: %s\n",
					strings.Join([]string{group, server, dir}, "-"), err.Error())
				log.Println("\t\trsync output:\n" + string(outputs))
			}
			timeStop := time.Now()

			// make rsync summary
			rsyncRpt := RsyncRpt{
				serverdir:      fmt.Sprintf("%s/%s", server, dir),
				howlong:        timeStop.Sub(timeStart),
				numFilesTotal:  "err",
				numFilesRcvd:   "err",
				sizeFilesTotal: "err",
				sizeFilesRcvd:  "err",
			}
			// execute 'getValue' from next "for ... range"
			getNum := func(s string) string {
				return strings.TrimSpace(strings.Split(
					strings.TrimSpace(strings.Split(s, ":")[1]),
					" ")[0])
			}
			getSize := func(s string) string {
				return strings.Split(getNum(s), ",")[0]
			}
			for _, s := range strings.Split(string(outputs), "\n") {
				if strings.HasPrefix(s, "Number of files:") {
					rsyncRpt.numFilesTotal = getNum(s)
				} else if strings.HasPrefix(s, "Number of regular files transferred:") {
					rsyncRpt.numFilesRcvd = getNum(s)
				} else if strings.HasPrefix(s, "Total file size:") {
					rsyncRpt.sizeFilesTotal = getSize(s)
				} else if strings.HasPrefix(s, "Total transferred file size:") {
					rsyncRpt.sizeFilesRcvd = getSize(s)
				}
			}

			totals.report += fmt.Sprintf("%-18s | %6s / %7s | %9s / %10s | %7.2f |\n",
				rsyncRpt.serverdir,
				rsyncRpt.numFilesRcvd, rsyncRpt.numFilesTotal,
				rsyncRpt.sizeFilesRcvd, rsyncRpt.sizeFilesTotal,
				rsyncRpt.howlong.Minutes())
		}
	}

	// make report
	fmt.Printf("errors/totals: %d/%d.\n%s",
		totals.rsyncErrorTask, totals.rsyncTotalTask, totals.rsyncErrMsg)
	fmt.Printf("other warnings: %d.\n%s", totals.warnNum, totals.warnMsg)
	fmt.Println(totals.report)
	// make subj
	// task success/total totals.errNum

	return nil
}

func logTotals(totals *Totals, msg string) {
	totals.warnNum++
	totals.warnMsg += msg
	log.Printf(msg)
}
