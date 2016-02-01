package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
)

type rsyncPar struct {
	Port       int
	DNSName    string
	RemotePath string
	LocalPath  string
	LogPath    string
	CfgPath    string
	SSHUser    string
}

type rsyncRpt struct {
	serverdir      string
	numFilesTotal  string
	numFilesRcvd   string
	sizeFilesTotal string
	sizeFilesRcvd  string
	howlong        time.Duration
}

type syncTotals struct {
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
		subj := fmt.Sprintf("zync'n'znap %s/%s: Exit with fatal error",
			strings.ToUpper(hostname), strings.ToUpper(group))
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

	// check default SSHUser
	if !viper.IsSet("SSHUser") {
		exitWithMailMsg("Default 'SSHuser' not found in config")
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
	rsyncArgsKey := "rsyncargs." + viper.GetString(groupTypeKey)
	if !viper.IsSet(rsyncArgsKey) {
		exitWithMailMsg(fmt.Sprintf("Rsync cmd '%s' not found in config", rsyncArgsKey))
	}
	rsyncArgsTmpl, err := template.New("rsyncArgsTmpl").Parse(viper.GetString(rsyncArgsKey))
	if err != nil {
		exitWithMailMsg(fmt.Sprintf("Rsync template '%s' error", rsyncArgsKey))
	}
	/* end common check's */

	/*
		MAIN PROCEDURE
	*/
	delimeter := func() string {
		return "\n" + strings.Repeat("-", 80) + "\n"
	}
	totals := syncTotals{
		report: fmt.Sprintf("%-16s | %17s | %29s | %7s |",
			"Server/Dir", "Files recv/total", "Size in Kb recv/total", "Minutes"),
	}
	totals.report += delimeter()
	//
	// enumerate servers
	//
	for server := range servers {
		// using 'logTotals' in local checks
		logTotals := func(totals *syncTotals, msg string) {
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
		rsyncPar := rsyncPar{
			DNSName: viper.GetString(dnsNameKey),
			CfgPath: cfgPath + "/"}
		portKey := keyOfServers + "." + server + ".port"
		if !viper.IsSet(portKey) {
			rsyncPar.Port = 22
		} else {
			rsyncPar.Port = viper.GetInt(portKey)
		}
		sshUserKey := keyOfServers + "." + server + ".SSHUser"
		if !viper.IsSet(sshUserKey) {
			rsyncPar.SSHUser = viper.GetString("SSHUser")
		} else {
			rsyncPar.SSHUser = viper.GetString(sshUserKey)
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
			rsyncPar.LocalPath = filepath.Join(serverBackupPath, dir)
			// if backup path not exist - skip
			if _, err := os.Stat(rsyncPar.LocalPath); os.IsNotExist(err) {
				msg := fmt.Sprintf("  WARN: skip dir '%s', path '%s' not exist\n", dir, rsyncPar.LocalPath)
				logTotals(&totals, msg)
				continue
			}
			log.Printf("\tsync dir '%s' with par:\n", dir)
			// construct rsync parameters
			rsyncPar.RemotePath = viper.GetString(keyOfDirs + "." + dir + ".remote")
			rsyncPar.LogPath = filepath.Join(viper.GetString("LogPath"),
				strings.Join([]string{group, server, dir}, "-")+".log")
			var buf bytes.Buffer
			err = rsyncArgsTmpl.Execute(&buf, rsyncPar)
			if err != nil {
				msg := fmt.Sprintf("  WARN: skip dir '%s', template error '%s'n", dir, err)
				logTotals(&totals, msg)
				continue
			}
			rsyncArgString := buf.String()
			log.Println(rsyncArgString)
			rsyncArgs := strings.Fields(rsyncArgString)
			// if par like '-e_ssh_-p_22_-i_rsbackup.rsa'
			// 		then change 'underscore' to 'space'
			for i := 0; i < len(rsyncArgs); i++ {
				rsyncArgs[i] = strings.Replace(rsyncArgs[i], "_", " ", -1)
			}

			// execute rsync
			timeStart := time.Now()
			totals.rsyncTotalTask++
			cmd := exec.Command("rsync", rsyncArgs...)
			outputs, err := cmd.CombinedOutput()
			if err != nil {
				totals.rsyncErrorTask++
				totals.rsyncErrMsg += fmt.Sprintf("  %s: %s\n%s\n\n",
					strings.Join([]string{group, server, dir}, "-"),
					err.Error(), string(outputs))
				log.Println("\t\trsync output:\n" + string(outputs))
			}
			timeStop := time.Now()

			// make rsync summary
			rsyncRpt := rsyncRpt{
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
				s = humanize.Commaf(float64(i) / 1024)
				if strings.Contains(s, ".") {
					return s[:(strings.Index(s, ".") + 2)]
				}
				return s + ".0"
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
			totals.report += fmt.Sprintf("%-16s | %7s / %7s | %13s / %13s | %7.2f |\n",
				rsyncRpt.serverdir,
				rsyncRpt.numFilesRcvd, rsyncRpt.numFilesTotal,
				rsyncRpt.sizeFilesRcvd, rsyncRpt.sizeFilesTotal,
				rsyncRpt.howlong.Minutes())
		}
	}

	//
	// make report
	//
	subj := fmt.Sprintf("zync'n'znap %s/%s: err/warn/total = %d/%d/%d",
		strings.ToUpper(hostname), strings.ToUpper(group),
		totals.rsyncErrorTask, totals.warnNum, totals.rsyncTotalTask)
	msg := totals.report + delimeter() + totals.rsyncErrMsg + delimeter() + totals.warnMsg
	// write report to logpath
	err = ioutil.WriteFile(
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
