package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/viper"
)

type ZipTotals struct {
	warnMsg      string
	warnNum      int
	zipErrMsg    string
	zipErrorTask int
	zipTotalTask int
	report       string
}

func dozip(group string) {
	hostname := getHostName()
	/*
		RUN CHECK'S
	*/
	// if next check's = failed
	// - send notice and exit
	exitWithMailMsg := func(msg string) {
		log.Printf("Exit with fatal error: %s\n", msg)
		subj := fmt.Sprintf("zync'n'znap %s/%s/%s: Exit with fatal error",
			strings.ToUpper(hostname), strings.ToUpper(task), strings.ToUpper(group))
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
	delimeter := func() string {
		return "\n" + strings.Repeat("-", 60) + "\n"
	}
	totals := ZipTotals{
		report: fmt.Sprintf("%-16s | %29s | %7s |",
			"Server/Dir", "Size in Kb", "Minutes"),
	}
	totals.report += delimeter()

	dateString := time.Now().Format("20060102")
	//
	// enumerate servers
	//
	for server := range servers {
		// using 'logTotals' in local checks
		logTotals := func(totals *ZipTotals, msg string) {
			totals.warnNum++
			totals.warnMsg += msg
			log.Printf(msg)
		}
		log.Printf("- Zip server '%s'\n", server)
		serverBackupPath := filepath.Join(groupBackupPath, server)
		if _, err := os.Stat(serverBackupPath); os.IsNotExist(err) {
			msg := fmt.Sprintf("  WARN: skip server '%s', path '%s' not exist\n", server, serverBackupPath)
			logTotals(&totals, msg)
			continue
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
			dirBackupPath := filepath.Join(serverBackupPath, dir)
			// if backup path not exist - skip
			if _, err := os.Stat(dirBackupPath); os.IsNotExist(err) {
				msg := fmt.Sprintf("  WARN: skip dir '%s', path '%s' not exist\n", dir, dirBackupPath)
				logTotals(&totals, msg)
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
				totals.report += fmt.Sprintf("%-16s | %29s | %7.2f |\n",
					fmt.Sprintf("%s/%s", server, dir), "SKIP", 0.0)
				continue
			}
			zipFileName := filepath.Join(viper.GetString("ZipPath"), strings.Join([]string{group, server, dir, dateString}, "_")+".zip")
			zipArgsString := fmt.Sprintf("-r -lf %s %s %s",
				filepath.Join(viper.GetString("LogPath"), strings.Join([]string{"zip", group, server, dir}, "-")+".log"),
				zipFileName,
				dirBackupPath)
			log.Printf("\tzip dir '%s' with par: %s\n", dir, zipArgsString)
			zipArgs := strings.Fields(zipArgsString)
			// execute zip
			timeStart := time.Now()
			totals.zipTotalTask++
			cmd := exec.Command("zip", zipArgs...)
			outputs, err := cmd.CombinedOutput()
			if err != nil {
				totals.zipErrorTask++
				totals.zipErrMsg += fmt.Sprintf("  %s: %s\n%s\n\n",
					strings.Join([]string{group, server, dir}, "-"),
					err.Error(), string(outputs))
				log.Println("\t\tzip output:\n" + string(outputs))
			}
			timeStop := time.Now()

			fsize := "err"
			fstat, err := os.Stat(zipFileName)
			if err != nil {
				msg := fmt.Sprintf("  WARN: error stat '%s'\n", zipFileName)
				logTotals(&totals, msg)
			} else {
				fsize = humanize.Commaf(float64(fstat.Size()) / 1024)
			}
			totals.report += fmt.Sprintf("%-16s | %29s | %7.2f |\n",
				fmt.Sprintf("%s/%s", server, dir),
				fsize[:(strings.Index(fsize, ".")+2)],
				timeStop.Sub(timeStart).Minutes())
		}
	}

	//
	// make report
	//
	subj := fmt.Sprintf("zync'n'znap zip %s/%s: err/warn/total = %d/%d/%d",
		strings.ToUpper(hostname), strings.ToUpper(group),
		totals.zipErrorTask, totals.warnNum, totals.zipTotalTask)
	msg := totals.report + delimeter() + totals.zipErrMsg + delimeter() + totals.warnMsg
	// write report to logpath
	err := ioutil.WriteFile(
		filepath.Join(viper.GetString("LogPath"), "zip-report.log"),
		[]byte(subj+"\n\n"+msg), 0666)
	if err != nil {
		log.Printf("WARN: '%s'", err)
	}
	// send report
	if err := sendReport(subj, msg); err != nil {
		log.Printf("WARN: '%s'", err)
	}

}
