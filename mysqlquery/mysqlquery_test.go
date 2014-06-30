package mysqlquery

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql" // import mysql driver
	"net"
	"os"
	"testing"
	"time"
)

var (
	user    string
	pass    string
	prot    string
	addr    string
	dbname  string
	dsn     string
	netAddr string
)

func init() {

	env := func(key, defaultValue string) string {
		if value := os.Getenv(key); value != "" {
			return value
		}
		return defaultValue
	}

	user = env("MYSQL_TEST_USER", "root")
	pass = env("MYSQL_TEST_PASS", "")
	prot = env("MYSQL_TEST_PROT", "tcp")
	addr = env("MYSQL_TEST_ADDR", "localhost:3306")
	dbname = env("MYSQL_TEST_DBNAME", "gotest")
	netAddr = fmt.Sprintf("%s(%s)", prot, addr)
	dsn = fmt.Sprintf("%s:%s@%s/%s?timeout=30s&strict=true", user, pass, netAddr, dbname)
	c, err := net.Dial(prot, addr)
	if err == nil {
		//available = true
		c.Close()
	}

}

type gdumper struct {
	t string // test
	e string // expected
}

var genericDumpertests = []gdumper{
	{"int(11)", "int64"},
	{"int(255)", "int64"},
	{"smallint", "int32"},
	{"datetime", "datetime"},
	{"tinyint", "int32"},
	{"date", "date"},
	{"decimal", "float64"},
}

func TestGenericTypeDumper(t *testing.T) {

	for r := range genericDumpertests {
		v := genericDumpertests[r]
		res := genericTypeMapper(v.t)
		if res != v.e {
			t.Errorf("  %d not mapping well %s > %s", r, v.e, res)
		}
	}
}

type DBTest struct {
	*testing.T
	db *sql.DB
}

var CREATE_TABLE = `CREATE TABLE test (
  id int(11) NOT NULL AUTO_INCREMENT,
  nombre varchar(50) NOT NULL,
  codigo int(11) DEFAULT NULL,
  unidades decimal(3,2) NOT NULL,
  tiny tinyint(1) NOT NULL,
  texto longtext NOT NULL,
  fecha date NOT NULL,
  PRIMARY KEY (id)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=utf8`

var INSERT_SQL = "INSERT INTO `test` (`nombre`, `codigo`, `unidades`, `tiny`, `texto`, `fecha`) VALUES     ('xxxx', 1, 1.50, 1, 'asdaasdfasdfasdfasdf', '2012-01-01');"

func runTests(t *testing.T, dsn string, tests ...func(dbt *DBTest)) {

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("Error connecting %s", dsn)
	}
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS test")
	dbt := &DBTest{t, db}
	for _, test := range tests {
		test(dbt)
		dbt.db.Exec("DROP TABLE IF EXISTS test")
		dbt.db.Exec("DROP TABLE IF EXISTS test2")
	}

}

func createTableTest(dbt *DBTest) error {
	query := fmt.Sprintf(CREATE_TABLE)
	_, err := dbt.db.Exec(query)
	if err != nil {
		panic(err)
	}
	return nil
}

func TestGetTypesFromTable(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		createTableTest(dbt)
		jd := &MysqlQuery{
			Db:    dbt.db,
			Table: "test",
			Cols:  make(map[string]string),
		}

		err := jd.GetTypesFromTable()
		if err != nil {
			panic(err)
		}

		if jd.Cols["id"] != "int64" {
			t.Errorf("Incorrect col type detected")
		}
		if jd.Cols["fecha"] != "date" {
			t.Errorf("Incorrect col type detected")
		}

	})
}

func TestGetData(t *testing.T) {
	runTests(t, dsn, func(dbt *DBTest) {
		createTableTest(dbt)
		dbt.db.Exec(INSERT_SQL)

		jd := &MysqlQuery{
			Db:    dbt.db,
			Table: "test",
			Cols:  make(map[string]string),
		}

		err := jd.GetTypesFromTable()
		if err != nil {
			panic(err)
		}

		err = jd.GetData()
		if err != nil {
			panic(err)
		}

	})
}

func TestTypeConvert(t *testing.T) {

	var k int64
	res, _ := TypeConvert("12345", "int64")

	k = 12345
	if k != res {
		t.Errorf("Wrong conversion %d, %d", k, res)
	}

	var k1 int32
	k1 = 123
	res, _ = TypeConvert("123", "int32")
	if k1 != res {
		t.Errorf("Wrong conversion %d, %d", k1, res)
	}

	var k2 float64
	k2 = 1234.4567
	res, _ = TypeConvert("1234.4567", "float64")
	if k2 != res {
		t.Errorf("Wrong conversion %d, %d", k2, res)
	}

	var k3 time.Time
	k3, _ = time.Parse("2006-01-02 15:04:05", "2012-09-22 06:24:31")
	res, _ = TypeConvert("2012-09-22 06:24:31", "datetime")
	if k3 != res {
		t.Errorf("Wrong time conversion %s, %s", k3, res)
	}
	//fmt.Println(res)

	var k4 time.Time
	k4, _ = time.Parse("2006-01-02", "2012-09-22")
	res, _ = TypeConvert("2012-09-22", "date")
	if k4 != res {
		t.Errorf("Wrong time conversion %s, %s", k4, res)
	}
	//fmt.Println(res)

	var k5 time.Time
	k5, _ = time.Parse("15:04:05", "02:34:00")
	res, _ = TypeConvert("02:34:00", "time")
	if k5 != res {
		t.Errorf("Wrong time conversion %s, %s", k5, res)
	}
	//fmt.Println(res)

	var k6 time.Time
	k6 = time.Unix(1348115640, 0)
	res, _ = TypeConvert("1348115640000", "timestamp")
	if k6 != res {
		t.Errorf("Wrong time conversion %s, %s", k6, res)
	}
	fmt.Println(res)

}
