package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
	/*
		RUN CHECK'S
	*/

	// it's already running?
	pidFileName := filepath.Join(
		"run", strings.Split(filepath.Base(os.Args[0]), ".")[0]+".pid")
	pidFileName = "test.pid"
	if _, err := os.Stat(pidFileName); err == nil {
		//return fmt.Errorf("RsyncBin '%s' not exist", rsyncBin)
		if procnum, err := ioutil.ReadFile(pidFileName); err != nil {
			fmt.Println(err.Error())
		} else {
			fmt.Println(string(procnum))
		}
	}
	/*
		DO
	*/
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

	/*
		MAIN PROCEDURE
	*/
	delimeter := func() string {
		return "\n" + strings.Repeat("-", 74) + "\n"
	}
	totals := Totals{
		report: fmt.Sprintf("%-18s | %16s | %22s | %7s |",
			"Server/Dir", "Files recv/total", "Size in Kb recv/total", "Minutes"),
	}
	totals.report += delimeter()
	//
	// enumerate servers
	//
	for server := range servers {
		// using 'logTotals' in local checks
		logTotals := func(totals *Totals, msg string) {
			totals.warnNum++
			totals.warnMsg += msg
			log.Printf(msg)
		}
		log.Printf("- Sync server '%s'\n", server)
		serverBackupPath := filepath.Join(groupBackupPath, server)
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
		//
		// enumerate dirs
		//
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
			// 		then change 'plus' to 'space'
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
			// execute 'getNum' and 'getSize' from next "for ... range"
			getNum := func(s string) string {
				return strings.TrimSpace(strings.Split(
					strings.TrimSpace(strings.Split(s, ":")[1]),
					" ")[0])
			}
			getSize := func(s string) string {
				i, err := strconv.Atoi(strings.Replace(getNum(s), ",", "", -1))
				if err != nil {
					return "err"
				}
				return fmt.Sprintf("%.1f", float64(i)/1024)
			}
			// reading rsync outputs
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

	//
	// make report
	//
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "(local)?"
	}
	subj := fmt.Sprintf("Zsync %s: err/warn/total = %d/%d/%d",
		strings.ToUpper(hostname), totals.rsyncErrorTask, totals.warnNum, totals.rsyncTotalTask)
	msg := totals.report + delimeter() + totals.rsyncErrMsg + delimeter() + totals.warnMsg
	// write report to logpath
	err = ioutil.WriteFile(
		filepath.Join(viper.GetString("LogPath"), "report.log"),
		[]byte(subj+"\n\n"+msg), 0644)
	if err != nil {
		log.Printf("WARN: '%s'", err)
	}
	// send report
	if err := sendReport(subj, msg); err != nil {
		log.Printf("WARN: '%s'", err)
	}

	return nil
}

func sendReport(subj string, msg string) error {
	par := []string{
		"--header", "'Auto-Submitted: auto-generated'",
		"--to", "root",
		"--subject", subj,
		"--body", msg,
	}
	cmd := exec.Command("mime-construct", par...)
	outputs, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s, %s", outputs, err)
	}
	return nil
}
