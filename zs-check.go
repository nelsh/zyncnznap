package main

import (
	"flag"
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
	checkOnly   bool
	zSyncUserID int
)

func init() {
	flag.BoolVar(&checkOnly, "checkonly", true, "Set 'false' for creating ZFS partitions from config")
	flag.Parse()
}

func main() {
	// Read configuration
	viper.SetConfigFile("zs.conf")
	viper.SetConfigType("toml")
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
	// check zSyncUser
	u, err := user.Lookup(viper.GetString("zSyncUser"))
	if err != nil {
		panic(err)
	}
	zSyncUserID, err = strconv.Atoi(u.Uid)
	// check backup path
	err = isExistZfsPartition(viper.GetString("ZfsPath"), "")
	if err != nil {
		panic(err)
	}

	/*	if _, err := os.Stat(viper.GetString("BackupPath")); os.IsNotExist(err) {
			panic(fmt.Errorf("Fatal error: %s \n", err))
		}
	*/
	// enumerate backups and check path
	for group := range viper.GetStringMap("group") {
		/*fmt.Printf("Group: %s, type=%s\n", s,
		viper.GetString("group."+s+".type"))
		*/
		zPath := path.Join(viper.GetString("ZfsPath"), group)
		err = isExistZfsPartition(zPath, "")

		if err != nil {
			if checkOnly {
				continue
			} else {
				makeZfsPartition(zPath)
			}
		}
		for server := range viper.GetStringMap("group." + group + ".servers") {
			zPath := path.Join(zPath, server)
			err = isExistZfsPartition(zPath, "\t")
			if err != nil {
				if checkOnly {
					continue
				} else {
					makeZfsPartition(zPath)
				}
			}
			for dir := range viper.GetStringMap("group." + group + ".servers." + server + ".dirs") {
				zPath := path.Join(zPath, dir)
				err = isExistZfsPartition(zPath, "\t\t")
				if err != nil {
					if checkOnly {
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
	_, err = zfs.GetDataset(zPath)
	if err != nil {
		fmt.Print("ERROR...")
		if checkOnly {
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
		err = os.Chown(ds.Mountpoint, zSyncUserID, -1)
		if err != nil {
			panic(err)
		}
	}
}
