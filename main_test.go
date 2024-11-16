package main

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/joho/godotenv"
	"nda.backup.mysql/util"
)

func TestBackupMysql(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	postgreeHost := os.Getenv("POSTGREE_DB_HOST")
	postgreePort := os.Getenv("POSTGREE_DB_PORT")
	postgreeUser := os.Getenv("POSTGREE_DB_USERNAME")
	postgreePass := os.Getenv("DB_PASSWORD")

	config := util.PostgreeConfig{
		User:     postgreeUser,
		Password: postgreePass,
		Host:     postgreeHost,
		DataBase: "postgres",
		Port:     postgreePort,
	}

	list, err := util.GetDBListPostgree(&config)
	if err != nil {
		t.Error(err)
	}

	if len(list) == 0 {
		fmt.Println("No database found")
	}

	for _, db := range list {
		fmt.Println(db)
	}

}
