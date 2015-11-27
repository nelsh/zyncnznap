package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"

	"github.com/mistifyio/go-zfs"
	"github.com/spf13/viper"
)

func dosnap() {
	var (
		warnNum  int
		warnMsg  string
		errNum   int
		errMsg   string
		snapName string
	)

	// calculate snapshot name
	t := time.Now()
	snapName = t.Format("20060102")
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
	if _, err := checkZfs(viper.GetString("ZfsPath"), ""); err != nil {
		log.Printf("Exit with fatal error: %s\n", err)
		subj := fmt.Sprintf("Zsync %s: Exit with fatal error", hostname)
		if err := sendReport(subj, err.Error()); err != nil {
			log.Printf("WARN: '%s'", err)
		}
		os.Exit(1)
	}
	// enumerate backups and check path
	for group := range viper.GetStringMap("groups") {
		zPath := path.Join(viper.GetString("ZfsPath"), group)
		if _, err := checkZfs(zPath, ""); err != nil {
			warnNum++
			warnMsg += err.Error()
			continue
		}
		for server := range viper.GetStringMap("groups." + group + ".servers") {
			zPath := path.Join(zPath, server)
			if _, err := checkZfs(zPath, "\t"); err != nil {
				warnNum++
				warnMsg += err.Error()
				continue
			}
			for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
				zPath := path.Join(zPath, dir)
				ds, err := checkZfs(zPath, "\t\t")
				if err != nil {
					warnNum++
					warnMsg += err.Error()
					continue
				}
				// make snap
				if _, err = ds.Snapshot(snapName, false); err != nil {
					log.Printf("\t\tmake snap: %s", err.Error())
					errNum++
					errMsg += err.Error()
				} else {
					log.Println("\t\tmake snap: OK")
				}
			}
		}
	}
	if warnNum > 0 || errNum > 0 {
		log.Printf("WARN: %d, %s", warnNum, warnMsg)
		log.Printf("ERROR: %d, %s", errNum, errMsg)
		subj := fmt.Sprintf("Zsync %s:  Exit with warnins=%d, errors=%d",
			hostname, warnNum, errNum)
		if err := sendReport(subj, warnMsg+"\n\n"+errMsg); err != nil {
			log.Printf("WARN: '%s'", err)
		}
	}
}

func checkZfs(zPath string, level string) (ds *zfs.Dataset, err error) {
	msg := fmt.Sprintf("%s%s...", level, zPath)
	if ds, err = zfs.GetDataset(zPath); err != nil {
		log.Printf("%sERROR. %s", msg, err)
	} else {
		log.Printf("%s OK\n", msg)
	}
	return ds, err
}
