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

	"github.com/dustin/go-humanize"
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

func dorsync(group string) {
	hostname := getHostName()
	pidFileName := filepath.Join(
		"/run/lock", strings.Split(filepath.Base(os.Args[0]), ".")[0]+group+".pid")
	/*
		RUN CHECK'S
	*/
	// if next check's = failed
	// - send notice and exit
	exitWithMailMsg := func(msg string) {
		log.Printf("Exit with fatal error: %s\n", msg)
		subj := fmt.Sprintf("Zsync %s: Exit with fatal error", hostname)
		if err := sendReport(subj, msg); err != nil {
			log.Printf("WARN: '%s'", err)
		}
		os.Remove(pidFileName)
		os.Exit(1)
	}
	// check one: it's already running?
	if _, err := os.Stat(pidFileName); err == nil {
		if procnum, err := ioutil.ReadFile(pidFileName); err != nil {
			exitWithMailMsg("read pid file: " + err.Error())
		} else {
			exitWithMailMsg(fmt.Sprintf("File '%s' is exist, process number: %s",
				pidFileName, string(procnum)))
		}
	}
	// - create pid
	if err := ioutil.WriteFile(pidFileName, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
		exitWithMailMsg("write pid file: " + err.Error())
	}

	// check group exist
	if !viper.IsSet("groups." + group) {
		exitWithMailMsg(fmt.Sprintf("Group '%s' not found in config", group))
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
	// check type/command of group
	groupTypeKey := "groups." + group + ".type"
	if !viper.IsSet(groupTypeKey) {
		exitWithMailMsg(fmt.Sprintf("Property 'type' for group '%s' not found in config", group))
	}
	rsyncCmdKey := "rsynccmd." + viper.GetString(groupTypeKey)
	if !viper.IsSet(rsyncCmdKey) {
		exitWithMailMsg(fmt.Sprintf("Rsync cmd '%s' not found in config", rsyncCmdKey))
	}
	rsyncCmdTmpl := viper.GetString(rsyncCmdKey)
	/* end common check's */

	/*
		MAIN PROCEDURE
	*/
	delimeter := func() string {
		return "\n" + strings.Repeat("-", 80) + "\n"
	}
	totals := Totals{
		report: fmt.Sprintf("%-18s | %17s | %27s | %7s |",
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
			cmd := exec.Command("rsync", rsyncArgs...)
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
				return humanize.Commaf(float64(i) / 1024)
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
			totals.report += fmt.Sprintf("%-18s | %7s / %7s | %12s / %12s | %7.2f |\n",
				rsyncRpt.serverdir,
				rsyncRpt.numFilesRcvd, rsyncRpt.numFilesTotal,
				rsyncRpt.sizeFilesRcvd, rsyncRpt.sizeFilesTotal,
				rsyncRpt.howlong.Minutes())
		}
	}

	//
	// make report
	//
	subj := fmt.Sprintf("Zsync %s: err/warn/total = %d/%d/%d",
		strings.ToUpper(hostname), totals.rsyncErrorTask, totals.warnNum, totals.rsyncTotalTask)
	msg := totals.report + delimeter() + totals.rsyncErrMsg + delimeter() + totals.warnMsg
	// write report to logpath
	err := ioutil.WriteFile(
		filepath.Join(viper.GetString("LogPath"), "report.log"),
		[]byte(subj+"\n\n"+msg), 0666)
	if err != nil {
		log.Printf("WARN: '%s'", err)
	}
	// send report
	if err := sendReport(subj, msg); err != nil {
		log.Printf("WARN: '%s'", err)
	}
	os.Remove(pidFileName)
}
