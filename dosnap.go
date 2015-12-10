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
	/* end check */

	/*
		MAIN PROCEDURE
	*/
	delimeter := func() string {
		return "\n" + strings.Repeat("-", 52) + "\n"
	}
	totals := SnapTotals{
		report: fmt.Sprintf("%-25s | %7s | %12s |",
			"Group/Server/Dir", "New", "Delete"),
	}
	totals.report += delimeter()

	// calculate snapshot name
	getStorageTime := func(label string) int {
		keyOfPeriod := "storageperiod." + label
		if viper.IsSet(keyOfPeriod) {
			storageTime := viper.GetInt(keyOfPeriod)
			if storageTime > 0 {
				log.Printf("INFO: '%s' = %d", keyOfPeriod, storageTime)
			} else {
				log.Printf("WARN: '%s' not set or zero.", keyOfPeriod)
			}
			return storageTime
		}
		log.Printf("WARN: '%s' not exist", keyOfPeriod)
		return 0
	}
	t := time.Now()
	var snapLabel string
	storagePeriod := 0
	if t.Weekday() != time.Saturday {
		snapLabel = "d"
		storagePeriod = getStorageTime("d")
	} else {
		_, weekNum := t.ISOWeek()
		if weekNum%13 == 1 {
			snapLabel = "q"
			storagePeriod = getStorageTime("q")
		} else {
			snapLabel = "w"
			storagePeriod = getStorageTime("w")
		}
	}
	snapName := t.Format("20060102") + snapLabel
	var oldSnapName string
	if storagePeriod == 0 {
		oldSnapName = t.Add(-time.Hour*24*(365*10)).Format("20060102") + snapLabel
	} else {
		oldSnapName = t.Add(-time.Hour*24*time.Duration(storagePeriod)).Format("20060102") + snapLabel
	}
	log.Printf("INFO: 'newSnapName' = %s", snapName)
	log.Printf("INFO: 'oldSnapName' = %s", oldSnapName)

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
		//
		// enumerate servers
		//
		for server := range viper.GetStringMap("groups." + group + ".servers") {
			zPath := path.Join(zPath, server)
			if _, err := zfs.GetDataset(zPath); err != nil {
				msg := fmt.Sprintf("  WARN: skip server '%s/%s', error: '%s'\n",
					group, server, err.Error())
				logWarnTotals(&totals, msg)
				continue
			}
			//
			// enumerate dirs
			//
			for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
				zPath := path.Join(zPath, dir)
				ds, err := zfs.GetDataset(zPath)
				if err != nil {
					msg := fmt.Sprintf("    WARN: skip dir '%s/%s/%s', error: '%s'\n",
						group, server, dir, err.Error())
					logWarnTotals(&totals, msg)
					totals.report += fmt.Sprintf("%-25s | %7s | %12s |\n",
						fmt.Sprintf("%s/%s/%s", group, server, dir), "ERROR", "ERROR")
					continue
				}
				logSnapErrTotals := func(totals *SnapTotals, msg string) {
					totals.ErrNum++
					totals.ErrMsg += msg
					log.Printf(msg)
				}
				totals.TotalDirs++
				newSnapResult := "SKIP"
				delSnapResult := "SKIP"
				// make snap
				if _, err := ds.Snapshot(snapName, false); err != nil {
					msg := fmt.Sprintf("\tERROR: '%s/%s/%s', error: '%s'\n",
						group, server, dir, err.Error())
					logSnapErrTotals(&totals, msg)
					newSnapResult = "ERROR"
					// goto end
				} else {
					// if snap without error
					log.Printf("\tSNAP: '%s/%s/%s' = OK\n", group, server, dir)
					newSnapResult = "OK"
					// storageTime set?
					if storagePeriod == 0 {
						delSnapResult = "Disabled"
						//goto end
					} else {
						// get snapShots
						snapShots, err := zfs.Snapshots(zPath)
						if err != nil {
							msg := fmt.Sprintf("\tERROR: '%s/%s/%s', error: '%s'\n",
								group, server, dir, err.Error())
							logSnapErrTotals(&totals, msg)
							delSnapResult = "ERROR"
							//goto end
						} else {
							delSnapResult = "CHECK"
							snapTotal := 0
							snapDeleting := 0
							snapDeleted := 0
							for _, sn := range snapShots {
								if strings.HasSuffix(sn.Name, snapLabel) {
									snapTotal++
									if zPath+"@"+oldSnapName > sn.Name {
										snapDeleting++
										if err := sn.Destroy(zfs.DestroyDefault); err != nil {
											msg := fmt.Sprintf("\tERROR: '%s/%s/%s', error: '%s'\n",
												group, server, dir, err.Error())
											logSnapErrTotals(&totals, msg)
										} else {
											snapDeleted++
											log.Printf("\t\tdeleting '%s' = OK", sn.Name)
										}
									}
								}
							}
							delSnapResult = fmt.Sprintf("%d/%d/%d",
								snapDeleted, snapDeleting, snapTotal)
						}
					}
				}
				totals.report += fmt.Sprintf("%-25s | %7s | %12s |\n",
					fmt.Sprintf("%s/%s/%s", group, server, dir), newSnapResult, delSnapResult)
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
