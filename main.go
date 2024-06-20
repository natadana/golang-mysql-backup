package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/fatih/color"
	"github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

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

	dbs, err := GetDBList(config)
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
	successUpload := UploadToS3(currentFolder+"/"+movedFile, s3Path+"/"+output)
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

func GetDBList(config *mysql.Config) ([]string, error) {
	db, err := sql.Open("mysql", config.FormatDSN())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dbs []string
	for rows.Next() {
		var db string
		if err := rows.Scan(&db); err != nil {
			return nil, err
		}
		dbs = append(dbs, db)
	}
	return dbs, nil
}

func UploadToS3(path string, key string) bool {
	region := os.Getenv("AWS_REGION")
	bucket := os.Getenv("AWS_BUCKET")
	awsAccessKey := os.Getenv("AWS_ACCESS_KEY")
	awsSecretKey := os.Getenv("AWS_SECRET_KEY")
	awsEndpoint := os.Getenv("AWS_ENDPOINT")
	if awsEndpoint == "" {
		awsEndpoint = "https://s3.amazonaws.com"
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(awsAccessKey, awsSecretKey, ""),
		Endpoint:    aws.String(awsEndpoint),
	})
	if err != nil {
		log.Fatal(err)
		return false
	}

	file, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
		return false
	}
	defer file.Close()

	svc := s3.New(sess)

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, file); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading file:", err)
		return false
	}

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	if err != nil {
		log.Fatal(err)
		return false
	}

	return true

}
