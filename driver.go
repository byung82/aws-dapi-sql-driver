package awsdapisqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

const driverName = "aws-rds-data-service"

type Driver struct {
	*options
}

func NewOpen(opts ...Option) (*sql.DB, error) {
	options := options{}

	for _, o := range opts {
		o.apply(&options)
	}

	if options.dataServiceApi == nil {
		return nil, fmt.Errorf("require RDSDataServiceAPI")
	}

	if !options.database.Valid {
		return nil, fmt.Errorf("require database")
	}

	if !options.resourceArn.Valid {
		return nil, fmt.Errorf("require resourceArn")
	}

	if !options.secretArn.Valid {
		return nil, fmt.Errorf("require secretArn")
	}

	dApi := &Driver{
		options: &options,
	}

	sql.Register(driverName, dApi)

	db, err := sql.Open(driverName, driverName)

	if err != nil {
		return nil, err
	}

	var checkTime int64
	now := time.Now().Unix()

	row := db.QueryRow(fmt.Sprintf("SELECT %d", now))

	err = row.Scan(&checkTime)

	if err != nil {
		return nil, err
	}

	fmt.Println(now, checkTime)

	return db, nil
}

func (d *Driver) Open(_ string) (driver.Conn, error) {
	return newConn(context.Background(), d.options), nil
}

//func (d *Driver) Open(dsn string) (driver.Conn, error) {
//	database, resourceARN, secretARN, dbType, ok := parseName(dsn)
//	if !ok {
//		return nil, fmt.Errorf("dsn must be of the form `secret={secret arn} resource={resource arn} database={database name}")
//	}
//
//	c := &config{
//		api:         d.api,
//		database:    database,
//		resourceARN: resourceARN,
//		secretARN:   secretARN,
//		dbType:      dbType,
//	}
//
//	return newConn(context.Background(), c), nil
//}
//
//func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
//	database, resourceARN, secretARN, dbType, ok := parseName(dsn)
//	if !ok {
//		return nil, fmt.Errorf("dsn must be of the form `secret={secret arn} resource={resource arn} database={database name}")
//	}
//
//	c := &config{
//		api:         d.api,
//		database:    database,
//		resourceARN: resourceARN,
//		secretARN:   secretARN,
//		dbType:      dbType,
//	}
//
//	return newConnector(c, d), nil
//}

//func parseName(name string) (database, resourceARN, secretARN, dbType string, ok bool) {
//	for _, kv := range strings.Split(name, " ") {
//		if parts := strings.SplitN(kv, "=", 2); len(parts) == 2 {
//			switch k, v := parts[0], parts[1]; k {
//			case "database":
//				database = v
//			case "resource":
//				resourceARN = v
//			case "secret":
//				secretARN = v
//			case "dbType":
//				dbType = v
//			}
//		}
//	}
//
//	return database, resourceARN, secretARN, dbType, database != "" && secretARN != "" && resourceARN != "" && dbType != ""
//}
