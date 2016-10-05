package machinerypg

import (
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

// DB is the connection pool of Postgres database
var DB *gorm.DB

func init() {
	// Generated timestamps are in UTC (created_at, updated_at, etc.)
	gorm.NowFunc = func() time.Time {
		return time.Now().UTC()
	}
}

// GormInit initializes ORM with the given url
// postgres://username:password@localhost/dbname
func GormInit(url string) error {
	if DB != nil {
		// Already initialized
		return nil
	}

	db, err := gorm.Open("postgres", url)

	// Get database connection handle [*sql.DB](http://golang.org/pkg/database/sql/#DB)
	db.DB()

	// Then you could invoke `*sql.DB`'s functions with it
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(100)

	// db.LogMode(true) // TODO: add it in readme

	DB = db
	return err
}
