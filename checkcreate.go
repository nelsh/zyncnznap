package main

import (
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"strconv"
	"strings"

	"github.com/mistifyio/go-zfs"
	"github.com/spf13/viper"
)

var (
	zSyncUserID int
)

func checkcreate() {
	// check zSyncUser
	if u, err := user.Lookup(viper.GetString("ZyncUser")); err != nil {
		log.Panicf("'ZyncUser' not set in config. %s", err)
	} else {
		zSyncUserID, err = strconv.Atoi(u.Uid)
	}
	// check logpath
	if !viper.IsSet("LogPath") {
		log.Panic("'LogPath' not set in config")
	}
	logPath := viper.GetString("LogPath")
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		if checkonly {
			log.Printf("log dir '%s' not exist\n", logPath)
		} else {
			log.Printf("make dir '%s'...\n", logPath)
			if err := os.MkdirAll(logPath, 0777); err != nil {
				log.Panic(err)
			}
			if err := os.Chown(logPath, zSyncUserID, -1); err != nil {
				log.Panic(err)
			}
		}
	}
	// check zippath
	zipPath := viper.GetString("ZipPath")
	if !isExistZfsPartition(zipPath[1:], "") {
		if !checkonly {
			makeZfsPartition(zipPath[1:], "")
		}
	}
	// check root backup path
	if !isExistZfsPartition(viper.GetString("ZfsPath"), "") {
		log.Panic(fmt.Sprintf("ZFS Volume '%s' not exist", viper.GetString("ZfsPath")))
	}

	// enumerate backups and check path
	for group := range viper.GetStringMap("groups") {
		zPath := path.Join(viper.GetString("ZfsPath"), group)
		if !isExistZfsPartition(zPath, "") {
			if checkonly {
				continue
			} else {
				makeZfsPartition(zPath, "")
			}
		}
		for server := range viper.GetStringMap("groups." + group + ".servers") {
			zPath := path.Join(zPath, server)
			if !isExistZfsPartition(zPath, "\t") {
				if checkonly {
					continue
				} else {
					makeZfsPartition(zPath, "\t")
				}
			}
			for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
				zPath := path.Join(zPath, dir)
				if !isExistZfsPartition(zPath, "\t\t") {
					if checkonly {
						continue
					} else {
						makeZfsPartition(zPath, "\t\t")
					}
				}
			}
		}
	}
}

func isExistZfsPartition(zPath string, level string) bool {
	msg := fmt.Sprintf("%s%s...", level, zPath)
	if _, err := zfs.GetDataset(zPath); err != nil {
		msg += ("ERROR:" + err.Error())
		if checkonly {
			log.Println(msg)
		}
		if !strings.Contains(err.Error(), "dataset does not exist") {
			log.Panicf("%s %s", msg, err)
		}
		return false
	}
	log.Printf("%s OK\n", msg)
	return true
}

func makeZfsPartition(zPath string, level string) {
	ds, err := zfs.CreateFilesystem(zPath, nil)
	if err != nil {
		log.Panic(err)
	} else {
		log.Printf("%smake new zfs: %s...%s\n", level, ds.Mountpoint, "OK")
		if err := os.Chown(ds.Mountpoint, zSyncUserID, -1); err != nil {
			log.Panic(err)
		}
	}
}
