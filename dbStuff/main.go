package dbStuff

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"

	_ "github.com/mattn/go-sqlite3"
	"gopkg.in/doug-martin/goqu.v4"
	_ "gopkg.in/doug-martin/goqu.v4/adapters/sqlite3"
)

type metricJsonFileModel struct {
	Metric_name string
	Value       string
	Lat         float64
	Lon         float64
	Timestamp   int
	Driver_id   string
}

type metricModel struct {
	Metric_name *string  `json:"metric_name"`
	Value       *int     `json:"value"`
	Lat         *float64 `json:"lat"`
	Lon         *float64 `json:"lon"`
	Timestamp   *int     `json:"timestamp"`
	Driver_id   *int     `json:"driver_id"`
	Id          *int     `json:"id"`
}

type DriverJsonFileModel struct {
	Id             *int
	Name           *string
	License_number *string
}

type driverModel struct {
	Id             int            `json:"id"`
	Name           string         `json:"name"`
	License_number string         `json:"license_number"`
	Metrics        []*metricModel `json:"metrics"`
}

type customError struct {
	originalError error
	beforeText    string
}

func (err customError) Error() string {
	if err.beforeText != "" {
		return fmt.Sprintf("%v: %v", err.beforeText, err.originalError)
	}
	return err.originalError.Error()
}

var dbPath = "./data-store.db"
var db *goqu.Database
var sqlDb *sql.DB

func openDb() {
	var err error
	sqlDb, err = sql.Open("sqlite3", dbPath)
	db = goqu.New("postgres", sqlDb)
	checkErr(err, "open db error")
}

func isDbReady() bool {
	if exists(dbPath) {
		openDb()
		_, err := db.Query("select * from status")
		ready := err == nil
		if !ready {
			sqlDb.Close()
		}
		return ready
	}
	return false
}

func Populate() {
	if isDbReady() {
		return
	}
	os.Remove(dbPath)
	openDb()
	var metrics []metricJsonFileModel
	var drivers []DriverJsonFileModel
	metricsJson, err := ioutil.ReadFile("./metrics.json")
	checkErr(err, "Error reading metrics.json")
	json.Unmarshal(metricsJson, &metrics)

	driversJson, err := ioutil.ReadFile("./drivers.json")
	checkErr(err, "Error reading drivers.json")
	json.Unmarshal(driversJson, &drivers)

	_, err = db.Exec(`
		create table metrics (
			id integer primary key autoincrement,
			metric_name text,
			value integer,
			lat real,
			lon real,
			timestamp integer,
			driver_id integer
		);
		`)
	checkErr(err, "metrics table creation error")

	for _, m := range metrics {
		if m.Driver_id != "" {
			parsedVal, err := strconv.ParseInt(m.Value, 10, 64)
			val := int(parsedVal)
			checkErr(err, "parse int error")
			parsedDriverId, err := strconv.ParseInt(m.Driver_id, 10, 64)
			driverId := int(parsedDriverId)
			checkErr(err, "parse int error")
			model := &metricModel{Metric_name: &m.Metric_name, Value: &val, Lat: &m.Lat, Lon: &m.Lon, Timestamp: &m.Timestamp, Driver_id: &driverId}
			insertMetric(model)
		}
	}

	_, err = db.Exec(`
		create table drivers (
			id integer primary key autoincrement,
			name string,
			license_number string
		);
		`)
	checkErr(err, "drivers table creation error")

	for _, d := range drivers {
		insertDriver(&d)
	}

	_, err = db.Exec(`
		create table status (
			ready integer
		);
		`)
	checkErr(err, "status table creation error")
}

func checkErr(err error, beforeText string) {
	if err != nil {
		panic(customError{err, beforeText})
	}
}

// Exists reports whether the named file or directory exists.
func exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

func GetAllDrivers() []*DriverJsonFileModel {
	rows, err := db.Query(`SELECT id, name,license_number FROM drivers`)
	checkErr(err, "error selecting driver")
	defer rows.Close()
	var drivers []*DriverJsonFileModel
	for rows.Next() {
		d := &DriverJsonFileModel{}
		err := rows.Scan(&d.Id, &d.Name, &d.License_number)
		checkErr(err, "error fetching driver")
		drivers = append(drivers, d)
	}
	return drivers
}

func GetDriverById(id int) *driverModel {
	rows, err := db.Query(`
		SELECT metrics.id as metric_id, name,license_number,metric_name,value,lat,lon,timestamp FROM drivers
		left join metrics on drivers.id = metrics.driver_id
		where drivers.id = ?
	`, id)
	checkErr(err, "error selecting driver")
	var d *driverModel
	var metrics []*metricModel
	defer rows.Close()
	for rows.Next() {
		var name, licenseNumber string
		m := &metricModel{}
		err := rows.Scan(&m.Id, &name, &licenseNumber, &m.Metric_name, &m.Value, &m.Lat, &m.Lon, &m.Timestamp)
		m.Driver_id = &id
		checkErr(err, "error fetching driver")
		metrics = append(metrics, m)
		if d == nil {
			d = &driverModel{Id: id, Name: name, License_number: licenseNumber}
		}
	}
	if d != nil {
		d.Metrics = metrics
	}
	return d
}

func AddDriver(data []byte) {
	driver := &DriverJsonFileModel{}
	json.Unmarshal(data, driver)
	stmt, err := db.Prepare(`
		insert into drivers
		( name, license_number )
		values
		( ? , ? )
		`)
	checkErr(err, "drivers insert query prepare error")
	_, err = stmt.Exec(driver.Name, driver.License_number)
	checkErr(err, "drivers insert execution error")
}

func AddMetric(data []byte) error {
	metric := &metricModel{}
	json.Unmarshal(data, metric)
	if isDriverExist(*metric.Driver_id) {
		insertMetric(metric)
		return nil
	}
	return errors.New("Driver id doesn't exist")
}

func isDriverExist(id int) bool {
	rows, err := db.Query("select id from drivers where id = ?", id)
	defer rows.Close()
	checkErr(err, "error checking driver existence")
	for rows.Next() {
		return true
	}
	return false
}

func insertDriver(d *DriverJsonFileModel) {
	stmt, err := db.Prepare(`
		insert into drivers
		( id, name, license_number )
		values
		( ?, ? , ? )
		`)
	checkErr(err, "drivers insert query prepare error")
	_, err = stmt.Exec(d.Id, d.Name, d.License_number)
	checkErr(err, "drivers insert execution error")
}

func insertMetric(m *metricModel) {
	stmt, err := db.Prepare(`
		insert into metrics
		( metric_name, value, lat, lon, timestamp, driver_id )
		values
		( ?, ? , ?, ?, ?, ? )
		`)
	checkErr(err, "query prepare error")
	_, err = stmt.Exec(m.Metric_name, m.Value, m.Lat, m.Lon, m.Timestamp, m.Driver_id)
	checkErr(err, "insert execution error")
}

func UpdateDriver(id int, data []byte) {
	driver := &DriverJsonFileModel{}
	json.Unmarshal(data, driver)

	r := make(goqu.Record)
	if driver.Name != nil {
		r["name"] = *driver.Name
	}
	if driver.License_number != nil {
		r["license_number"] = *driver.License_number
	}

	update := db.From("drivers").
		Where(goqu.I("id").Eq(id)).
		Update(r)

	_, err := update.Exec()
	checkErr(err, "drivers update execution error")
}

func DeleteDriver(id int) {
	stmt, err := db.Prepare(`
		delete from drivers
		where
		id = ?
		`)
	checkErr(err, "drivers delete query prepare error")
	_, err = stmt.Exec(id)
	checkErr(err, "drivers delete execution error")

	stmt, err = db.Prepare(`
		delete from metrics
		where
		driver_id = ?
		`)
	checkErr(err, "driver metrics delete query prepare error")
	_, err = stmt.Exec(id)
	checkErr(err, "driver metrics delete execution error")
}

func DeleteMetric(id int) {
	stmt, err := db.Prepare(`
		delete from metrics
		where
		id = ?
		`)
	checkErr(err, "metrics delete query prepare error")
	_, err = stmt.Exec(id)
	checkErr(err, "metrics delete execution error")
}

func GetMaxMetric(metricName string) int {
	fmt.Println(metricName)
	rows, err := db.Query("select max(value) from metrics where metric_name = ?", metricName)
	checkErr(err, "get max metric error")
	for rows.Next() {
		var max int
		rows.Scan(&max)
		checkErr(err, "error fetching max metric")
		return max
	}
	return 0
}
