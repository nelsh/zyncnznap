package main

import (
	"fmt"
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
	if u, err := user.Lookup(viper.GetString("zSyncUser")); err != nil {
		panic(err)
	} else {
		zSyncUserID, err = strconv.Atoi(u.Uid)
	}
	// check root backup path
	if err := isExistZfsPartition(viper.GetString("ZfsPath"), ""); err != nil {
		panic(err)
	}
	// enumerate backups and check path
	for group := range viper.GetStringMap("groups") {
		zPath := path.Join(viper.GetString("ZfsPath"), group)
		if err := isExistZfsPartition(zPath, ""); err != nil {
			if checkonly {
				continue
			} else {
				makeZfsPartition(zPath)
			}
		}
		for server := range viper.GetStringMap("groups." + group + ".servers") {
			zPath := path.Join(zPath, server)
			if err := isExistZfsPartition(zPath, "\t"); err != nil {
				if checkonly {
					continue
				} else {
					makeZfsPartition(zPath)
				}
			}
			for dir := range viper.GetStringMap("groups." + group + ".servers." + server + ".dirs") {
				zPath := path.Join(zPath, dir)
				if err := isExistZfsPartition(zPath, "\t\t"); err != nil {
					if checkonly {
						continue
					} else {
						makeZfsPartition(zPath)
					}
				}
			}
		}
	}
}

func isExistZfsPartition(zPath string, level string) (err error) {
	fmt.Printf("%s%s...", level, zPath)
	if _, err = zfs.GetDataset(zPath); err != nil {
		fmt.Print("ERROR...")
		if checkonly {
			fmt.Println("")
		}
		if !strings.Contains(err.Error(), "dataset does not exist") {
			panic(err)
		}
	} else {
		fmt.Println("OK")
	}
	return err
}

func makeZfsPartition(zPath string) {
	ds, err := zfs.CreateFilesystem(zPath, nil)
	if err != nil {
		panic(err)
	} else {
		fmt.Printf("make new zfs: %s...%s\n", ds.Mountpoint, "OK")
		if err := os.Chown(ds.Mountpoint, zSyncUserID, -1); err != nil {
			panic(err)
		}
	}
}
