package util

import (
	"database/sql"
	"log"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func GetDBListMysql(config *mysql.Config) ([]string, error) {
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

type PostgreeConfig struct {
	User     string
	Password string
	Host     string
	DataBase string
	Port     string
}

func GetDBListPostgree(config *PostgreeConfig) ([]string, error) {
	db, err := sqlx.Connect("postgres", `user=`+config.User+` password=`+config.Password+` host=`+config.Host+` dbname=`+config.DataBase+` sslmode=disable`)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT datname FROM pg_database WHERE datistemplate = false")
	if err != nil {
		return nil, err
	}

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
