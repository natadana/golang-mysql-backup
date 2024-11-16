package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"nda.backup.mysql/util"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	backupedDb := os.Getenv("BACKUPED_DB")
	backupedDbs := strings.Split(backupedDb, ";")

	for _, db := range backupedDbs {
		if db == "mysql" {
			BackupMysql()
		} else if db == "postgree" {
			BackupPostgree()
		} else {
			fmt.Println("Database type not supported: ", color.RedString(db))
		}
	}
}

func BackupPostgree() {
	postgreeHost := os.Getenv("POSTGREE_DB_HOST")
	postgreePort := os.Getenv("POSTGREE_DB_PORT")
	postgreeUser := os.Getenv("POSTGREE_DB_USERNAME")
	postgreePass := os.Getenv("DB_PASSWORD")

	backupDir := os.Getenv("BACKUP_DIR")
	s3Path := os.Getenv("S3_PATH")
	listDbDiscard := os.Getenv("LIST_DB_DISCARD")

	pgdumpPath := os.Getenv("PGDUMP_PATH")
	if pgdumpPath == "" {
		pgdumpPath = "pg_dump"
	}

	config := util.PostgreeConfig{
		User:     postgreeUser,
		Password: postgreePass,
		Host:     postgreeHost,
		DataBase: "postgres",
		Port:     postgreePort,
	}

	list, err := util.GetDBListPostgree(&config)
	if err != nil {
		log.Fatal(err)
	}

	if len(list) == 0 {
		fmt.Println("No database found")
	}

	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		os.Mkdir(backupDir, os.ModePerm)
	}

	backupDirWithTime := fmt.Sprint(backupDir, "/", time.Now().Format("2006-01-02-15:04:05"))
	if _, err := os.Stat(backupDirWithTime); os.IsNotExist(err) {
		os.Mkdir(backupDirWithTime, os.ModePerm)
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	listDbDiscardArr := strings.Split(listDbDiscard, ";")
	cleanedDir := strings.Replace(backupDirWithTime, " ", "\\ ", -1)
	fmt.Println("Backup directory: ", cleanedDir)

	for _, db := range list {
		if Contains(listDbDiscardArr, db) {
			fmt.Println("Skipping database: ", color.RedString(db), " because it's in the list of discarded databases")
			continue
		}

		command := fmt.Sprintf("PGPASSWORD='%s' %s -U %s -h %s -p %s %s --file %s", postgreePass, pgdumpPath, postgreeUser, postgreeHost, postgreePort, db, fmt.Sprint(currentFolder+"/"+backupDirWithTime, "/", db, ".sql"))
		fmt.Println(command)
		cmd := exec.Command("bash", "-c", command)
		fmt.Println("Backing up database: ", color.GreenString(db))
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Zipping backup folder")
	combineFolder := fmt.Sprint(currentFolder + "/" + backupDirWithTime)
	output := fmt.Sprint(time.Now().Format("2006-01-02 15:04:05"), ".zip")
	cmd := exec.Command("zip", "-r", output, combineFolder)
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	//move zip file to backup folder
	movedFile := backupDir + "/" + output
	fmt.Println("Backup folder zipped to: ", movedFile)
	os.Rename(output, movedFile)
	fmt.Println("Backup folder zipped")

	fmt.Println("Removing backup folder")
	os.RemoveAll(combineFolder)

	fmt.Println("Uploading backup to S3")
	successUpload := util.UploadToS3(currentFolder+"/"+movedFile, s3Path+"/"+output)
	if successUpload {
		fmt.Println("Backup uploaded to S3")
		fmt.Println("Removing backup zip file")
		os.Remove(currentFolder + "/" + movedFile)
		fmt.Println("Backup zip file removed")
	} else {
		fmt.Println("Failed to upload backup to S3")
	}

	fmt.Println("Backup completed")
}

func BackupMysql() {
	dbUser := os.Getenv("DB_USERNAME")
	dbPass := os.Getenv("DB_PASSWORD")
	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	backupDir := os.Getenv("BACKUP_DIR")
	s3Path := os.Getenv("S3_PATH")
	listDbDiscard := os.Getenv("LIST_DB_DISCARD")

	mysqldumpPath := os.Getenv("MYSQLDUMP_PATH")
	if mysqldumpPath == "" {
		mysqldumpPath = "mysqldump"

	}

	config := mysql.NewConfig()
	config.User = dbUser
	config.Passwd = dbPass
	config.Net = "tcp"
	config.Addr = fmt.Sprintf("%s:%s", dbHost, dbPort)

	dbs, err := util.GetDBListMysql(config)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		os.Mkdir(backupDir, os.ModePerm)
	}

	backupDirWithTime := fmt.Sprint(backupDir, "/", time.Now().Format("2006-01-02 15:04:05"))
	if _, err := os.Stat(backupDirWithTime); os.IsNotExist(err) {
		os.Mkdir(backupDirWithTime, os.ModePerm)
	}

	currentFolder, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	listDbDiscardArr := strings.Split(listDbDiscard, ";")

	for _, db := range dbs {
		if Contains(listDbDiscardArr, db) {
			fmt.Println("Skipping database: ", color.RedString(db), " because it's in the list of discarded databases")
			continue
		}

		cmd := exec.Command(mysqldumpPath, "-u", dbUser, "-p"+dbPass, "-h", dbHost, db, "--lock-tables=false", "--result-file", fmt.Sprint(currentFolder+"/"+backupDirWithTime, "/", db, ".sql"))
		fmt.Println("Backing up database: ", color.GreenString(db))
		err := cmd.Run()
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("Zipping backup folder")
	combineFolder := fmt.Sprint(currentFolder + "/" + backupDirWithTime)
	output := fmt.Sprint(time.Now().Format("2006-01-02 15:04:05"), ".zip")
	cmd := exec.Command("zip", "-r", output, combineFolder)
	err = cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	//move zip file to backup folder
	movedFile := backupDir + "/" + output
	fmt.Println("Backup folder zipped to: ", movedFile)
	os.Rename(output, movedFile)
	fmt.Println("Backup folder zipped")

	fmt.Println("Removing backup folder")
	os.RemoveAll(combineFolder)

	fmt.Println("Uploading backup to S3")
	successUpload := util.UploadToS3(currentFolder+"/"+movedFile, s3Path+"/"+output)
	if successUpload {
		fmt.Println("Backup uploaded to S3")
		fmt.Println("Removing backup zip file")
		os.Remove(currentFolder + "/" + movedFile)
		fmt.Println("Backup zip file removed")
	} else {
		fmt.Println("Failed to upload backup to S3")
	}

	fmt.Println("Backup completed")

}

func Contains(denyDbs []string, db string) bool {
	var found bool = false
	for _, denyDb := range denyDbs {
		if db == denyDb {
			found = true
			break
		}
	}
	return found
}
