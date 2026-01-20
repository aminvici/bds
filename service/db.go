package service

import (
	"bufio"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	_ "github.com/greenplum-db/pq"
	"github.com/aminvici/bds/common/log"
	"io"
	"os"
	"reflect"
	"strings"
	"xorm.io/core"
)

var (
	engine  *xorm.Engine
	maxSize int
)

type DatabaseConfig struct {
	Type         string
	Host         string
	Port         string
	Database     string
	User         string
	Password     string
	MaxOpenConns int
	MaxIdleConns int
	SQLLogFile   string
	Debug        bool
}

func (c *DatabaseConfig) Valid() bool {
	switch strings.ToLower(c.Type) {
	case "mysql", "mssql", "postgres":
		break
	case "bmssql":
		c.Type = "mssql"
		break
	default:
		return false

	}
	return true
}

func InitDB(cfg *DatabaseConfig) error {
	var err error
	if !cfg.Valid() {
		return errors.New("database config invalid")
	}
	var dsn, msg string
	switch strings.ToLower(cfg.Type) {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		msg = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			cfg.User, "******", cfg.Host, cfg.Port, cfg.Database)
		maxSize = 60000
	case "mssql":
		dsn = fmt.Sprintf("Driver={SQL Server};User id=%s;Password=%s;Server=%s;Port=%s;Database=%s;",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		msg = fmt.Sprintf("Driver={SQL Server};User id=%s;Password=%s;Server=%s;Port=%s;Database=%s;",
			cfg.User, "******", cfg.Host, cfg.Port, cfg.Database)
		maxSize = 2100
		//core.RegisterDialect("mssql", func() core.Dialect { return &plugins.MSSQLDialect{} })
	case "postgres":
		dsn = fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
			cfg.User, cfg.Password, cfg.Database, cfg.Host, cfg.Port, "disable")
		msg = fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
			cfg.User, "******", cfg.Database, cfg.Host, cfg.Port, "disable")
		maxSize = 6000
	default:
		return errors.New("unsupport database type")
	}

	log.DetailDebug("database: dsn '%s'", msg)
	engine, err = xorm.NewEngine(cfg.Type, dsn)
	if err != nil {
		return err
	}

	err = engine.Ping()
	if err != nil {
		return err
	}

	if cfg.Debug {
		engine.ShowSQL(true)
		engine.ShowExecTime(true)
		log.Debug("database: sql log file %s", cfg.SQLLogFile)
		f, err := newSQLLogger(cfg.SQLLogFile)
		if err != nil {
			return err
		}
		engine.SetLogger(xorm.NewSimpleLogger(f))
		engine.SetLogLevel(core.LOG_INFO)
	}

	engine.SetMaxIdleConns(cfg.MaxIdleConns)
	engine.SetMaxOpenConns(cfg.MaxOpenConns)

	return nil
}

func NewEngine(cfg *DatabaseConfig) (*xorm.Engine, error) {
	var err error
	if !cfg.Valid() {
		return nil, errors.New("database config invalid")
	}
	var dsn, msg string
	switch strings.ToLower(cfg.Type) {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		msg = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8",
			cfg.User, "******", cfg.Host, cfg.Port, cfg.Database)
		maxSize = 60000
	case "mssql", "bmssql":
		dsn = fmt.Sprintf("Driver={SQL Server};User id=%s;Password=%s;Server=%s;Port=%s;Database=%s;Connection Timeout=0;",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
		msg = fmt.Sprintf("Driver={SQL Server};User id=%s;Password=%s;Server=%s;Port=%s;Database=%s;",
			cfg.User, "******", cfg.Host, cfg.Port, cfg.Database)
		maxSize = 2100
		//core.RegisterDialect("mssql", func() core.Dialect { return &plugins.MSSQLDialect{} })
	case "postgres":
		dsn = fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
			cfg.User, cfg.Password, cfg.Database, cfg.Host, cfg.Port, "disable")
		msg = fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=%s",
			cfg.User, "******", cfg.Database, cfg.Host, cfg.Port, "disable")
		maxSize = 6000
	default:
		return nil, errors.New("unsupport database type")
	}

	log.Debug("database: dsn '%s'", msg)
	e, err := xorm.NewEngine(cfg.Type, dsn)
	if err != nil {
		return nil, err
	}

	if cfg.Debug {
		e.ShowSQL(true)
		e.ShowExecTime(true)
		log.Debug("database: sql log file %s", cfg.SQLLogFile)
		f, err := newSQLLogger(cfg.SQLLogFile)
		if err != nil {
			return nil, err
		}
		e.SetLogger(xorm.NewSimpleLogger(f))
		e.SetLogLevel(core.LOG_INFO)
	}

	err = e.Ping()
	if err != nil {
		return nil, err
	}

	//engine.ShowExecTime(true)
	e.SetMaxIdleConns(cfg.MaxIdleConns)
	e.SetMaxOpenConns(cfg.MaxOpenConns)

	return e, nil
}

func newSQLLogger(s string) (io.Writer, error) {
	fd, err := os.OpenFile(s, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return bufio.NewWriter(fd), nil
}

type Database struct {
	*xorm.Engine
}

func NewDatabase(e ...*xorm.Engine) *Database {
	if len(e) == 1 {
		return &Database{e[0]}
	}
	return &Database{engine}
}

func (d *Database) BatchInsert(data interface{}) (int64, error) {
	var affected int64
	var err error
	var maxSize int

	sliceValue := reflect.Indirect(reflect.ValueOf(data))
	if sliceValue.Kind() == reflect.Slice {
		size := sliceValue.Len()
		log.Debug("database: beans length %d", size)
		for i := 0; i < size; i++ {
			if sliceValue.Index(i).IsNil() {
				msg := fmt.Sprintf("index %d is nil", i)
				return 0, errors.New(msg)
			}
		}
		if size > 1 {
			columnCount := len(d.TableInfo(sliceValue.Index(0).Interface()).Columns())
			placeholderNum := size * columnCount
			log.Debug("database: place holder number %d", placeholderNum)
			if placeholderNum > maxSize {
				batchSize := maxSize / columnCount
				posList, err := getBatchRange(data, batchSize)
				if err != nil {
					msg := "database: get batch range error"
					log.Error(msg)
					log.DetailError(err)
					return affected, err
				}
				for _, pos := range posList {
					log.Debug("database: batch from %d to %d", pos.Start, pos.End)
					c, err := d.Insert(sliceValue.Slice(pos.Start, pos.End).Interface())
					if err != nil {
						return affected, err
					}
					affected += c
				}
			} else {
				affected, err = d.Insert(data)
				if err != nil {
					return affected, err
				}
			}
		}
	} else {
		if data == nil {
			msg := fmt.Sprintf("data is nil")
			return 0, errors.New(msg)
		}
		affected, err = d.Insert(data)
		if err != nil {
			return affected, err
		}
	}
	return affected, nil
}

type Transaction struct {
	engine *xorm.Engine
	*xorm.Session
}

func NewTransaction(e ...*xorm.Engine) *Transaction {
	if len(e) == 1 {
		return &Transaction{e[0], e[0].NewSession()}
	}
	return &Transaction{engine, engine.NewSession()}
}

func (t *Transaction) BatchInsert(data interface{}) (int64, error) {
	var affected int64
	var err error

	sliceValue := reflect.Indirect(reflect.ValueOf(data))
	if sliceValue.Kind() == reflect.Slice {
		size := sliceValue.Len()
		log.Debug("database: beans length %d", size)
		for i := 0; i < size; i++ {
			if sliceValue.Index(i).IsNil() {
				msg := fmt.Sprintf("index %d is nil", i)
				return 0, errors.New(msg)
			}
		}
		if size >= 1 {
			columnCount := len(t.engine.TableInfo(sliceValue.Index(0).Interface()).Columns())
			placeholderNum := size * columnCount
			log.Debug("database: place holder number %d", placeholderNum)
			if placeholderNum > maxSize {
				batchSize := maxSize / columnCount
				posList, err := getBatchRange(data, batchSize)
				if err != nil {
					msg := "database: get batch range error"
					log.Error(msg)
					log.DetailError(err)
					return affected, err
				}
				for _, pos := range posList {
					log.Debug("database: batch from %d to %d", pos.Start, pos.End)
					c, err := t.Insert(sliceValue.Slice(pos.Start, pos.End).Interface())
					if err != nil {
						return affected, err
					}
					affected += c
				}
			} else {
				affected, err = t.Insert(data)
				if err != nil {
					return affected, err
				}
			}
		}
	} else {
		if data == nil {
			msg := fmt.Sprintf("data is nil")
			return 0, errors.New(msg)
		}
		affected, err = t.Insert(data)
		if err != nil {
			return affected, err
		}
	}
	return affected, nil
}

func (t *Transaction) Execute(sqlStr string, args ...interface{}) (int64, error) {
	var result sql.Result
	var err error

	if len(args) > 1 {
		result, err = t.Exec(sqlStr, args)
	} else {
		result, err = t.Exec(sqlStr)
	}
	if err != nil {
		return 0, err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return affected, nil
}

type BatchRange struct {
	Start int
	End   int
}

func getBatchRange(data interface{}, count int) ([]*BatchRange, error) {
	b := make([]*BatchRange, 0)

	if count <= 0 {
		return b, errors.New("count must >= 1")
	}

	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i += count {
			if i+count <= v.Len() {
				b = append(b, &BatchRange{i, i + count})
			} else {
				b = append(b, &BatchRange{i, v.Len()})
			}
		}
	} else {
		return b, errors.New("data must be slice(T)")
	}

	return b, nil
}
