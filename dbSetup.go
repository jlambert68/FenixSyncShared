package fenixTestDatashared

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
)

var DbPool *pgxpool.Pool

// mustGetEnv is a helper function for getting environment variables.
// Displays a warning if the environment variable is not set.
func MustGetEnvironmentVariable(k string) string {
	v := os.Getenv(k)
	if v == "" {
		log.Fatalf("Warning: %s environment variable not set.\n", k)
	}
	return v
}

func ConnectToDB() {

	var dbURI string
	var err error

	var (
		dbUser    = MustGetEnvironmentVariable("DB_USER") // e.g. 'my-db-user'
		dbPwd     = MustGetEnvironmentVariable("DB_PASS") // e.g. 'my-db-password'
		dbTCPHost = MustGetEnvironmentVariable("DB_HOST") // e.g. '127.0.0.1' ('172.17.0.1' if deployed to GAE Flex)
		dbPort    = MustGetEnvironmentVariable("DB_PORT") // e.g. '5432'
		dbName    = MustGetEnvironmentVariable("DB_NAME") // e.g. 'my-database'

	)

	// If the optional DB_HOST environment variable is set, it contains
	// the IP address and port number of a TCP connection pool to be created,
	// such as "127.0.0.1:5432". If DB_HOST is not set, a Unix socket
	// connection pool will be created instead.
	if dbTCPHost != "GCP" {
		dbURI = fmt.Sprintf("host=%s user=%s password=%s port=%s database=%s", dbTCPHost, dbUser, dbPwd, dbPort, dbName)

	} else {

		var dbInstanceConnectionName = MustGetEnvironmentVariable("DB_INSTANCE_CONNECTION_NAME")

		socketDir, isSet := os.LookupEnv("DB_SOCKET_DIR")
		if !isSet {
			socketDir = "/cloudsql"
		}

		dbURI = fmt.Sprintf("user=%s password=%s database=%s host=%s/%s", dbUser, dbPwd, dbName, socketDir, dbInstanceConnectionName)

	}

	DbPool, err = pgxpool.Connect(context.Background(), dbURI)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	//defer dbpool.Close()

	var version string
	err = DbPool.QueryRow(context.Background(), "SELECT VERSION()").Scan(&version)

	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println(version)
}
