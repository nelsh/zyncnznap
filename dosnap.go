package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mistifyio/go-zfs"
	"github.com/spf13/viper"
)

type SnapTotals struct {
	warnMsg   string
	warnNum   int
	ErrMsg    string
	ErrNum    int
	TotalDirs int
	report    string
}

func dosnap() {
	// calculate snapshot name
	t := time.Now()
	snapName := t.Format("20060102")
	if t.Weekday() != time.Saturday {
		snapName += "d"
	} else {
		_, weekNum := t.ISOWeek()
		if weekNum%13 == 1 {
			snapName += "q"
		} else {
			snapName += "w"
		}
	}

	hostname := getHostName()
	// check root backup path
	if _, err := zfs.GetDataset(viper.GetString("ZfsPath")); err != nil {
		log.Printf("Exit with fatal error: %s\n", err)
		subj := fmt.Sprintf("zync'n'znap snap %s: Exit with fatal error",
			strings.ToUpper(hostname))
		if err := sendReport(subj, err.Error()); err != nil {
			log.Printf("WARN: '%s'", err)
		}
		os.Exit(1)
	}
	delimeter := func() string {
		return "\n" + strings.Repeat("-", 40) + "\n"
	}
	totals := SnapTotals{
		report: fmt.Sprintf("%-21s | %7s | %12s |\n",
			"Group/Server/Dir", "New", "Delete"),
	}

	// enumerate backups and check path
	for group := range viper.GetStringMap("groups") {
		// using 'logTotals' in local checks
		logWarnTotals := func(totals *SnapTotals, msg string) {
			totals.warnNum++
			totals.warnMsg += msg
			log.Printf(msg)
		}
		zPath := path.Join(viper.GetString("ZfsPath"), group)
		if _, err := zfs.GetDataset(zPath); err != nil {
			msg := fmt.Sprintf("WARN: skip group '%s', error: '%s'\n",
				group, err.Error())
			logWarnTotals(&totals, msg)
			continue
		}
		for server := range viper.GetStringMap("groups." + group + ".servers") {
			zPath := path.Join(zPath, server)
			if _, err := zfs.GetDataset(zPath); err != nil {
				msg := fmt.Sprintf("  WARN: skip server '%s/%s', error: '%s'\n",
					group, server, err.Error())
				logWarnTotals(&totals, msg)
				continue
			}
			for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
				zPath := path.Join(zPath, dir)
				ds, err := zfs.GetDataset(zPath)
				if err != nil {
					msg := fmt.Sprintf("    WARN: skip dir '%s/%s/%s', error: '%s'\n",
						group, server, dir, err.Error())
					logWarnTotals(&totals, msg)
					totals.report += fmt.Sprintf("%-21s | %7s | %12s |\n",
						fmt.Sprintf("%s/%s/%s", group, server, dir), "ERROR", "ERROR")
					continue
				}
				// make snap
				logSnapErrTotals := func(totals *SnapTotals, msg string) {
					totals.ErrNum++
					totals.ErrMsg += msg
					log.Printf(msg)
				}
				totals.TotalDirs++
				if _, err := ds.Snapshot(snapName, false); err != nil {
					msg := fmt.Sprintf("\tERROR: '%s/%s/%s', error: '%s'\n",
						group, server, dir, err.Error())
					logSnapErrTotals(&totals, msg)
					totals.report += fmt.Sprintf("%-21s | %7s | %12s |\n",
						fmt.Sprintf("%s/%s/%s", group, server, dir), "ERROR", "SKIP")
				} else {
					log.Printf("\tSNAP: '%s/%s/%s' = OK\n", group, server, dir)
					// make destroy old snapshot
					isDestroy := false
					if isDestroy {
						totals.report += fmt.Sprintf("%-21s | %7s | %12s |\n",
							fmt.Sprintf("%s/%s/%s", group, server, dir), "OK", "snapname")
					} else {
						totals.report += fmt.Sprintf("%-21s | %7s | %12s |\n",
							fmt.Sprintf("%s/%s/%s", group, server, dir), "OK", "SKIP")
					}
				}
			}
		}
	}

	//
	// make report
	//
	subj := fmt.Sprintf("zync'n'znap snap %s/%s: err/warn/total = %d/%d/%d",
		strings.ToUpper(hostname), strings.ToUpper(group),
		totals.ErrNum, totals.warnNum, totals.TotalDirs)
	msg := totals.report + delimeter() + totals.ErrMsg + delimeter() + totals.warnMsg
	// write report to logpath
	err := ioutil.WriteFile(
		filepath.Join(viper.GetString("LogPath"), "snap-report.log"),
		[]byte(subj+"\n\n"+msg), 0666)
	if err != nil {
		log.Printf("WARN: '%s'", err)
	}
	// send report
	if err := sendReport(subj, msg); err != nil {
		log.Printf("WARN: '%s'", err)
	}

}
