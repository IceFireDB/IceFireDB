package sqlite

import (
	"context"
	"database/sql"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/config"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/p2p"
	"github.com/IceFireDB/IceFireDB-SQLite/utils"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"
)

var (
	db        *sql.DB
	p2pHost   *p2p.P2P
	p2pPubSub *p2p.PubSub
)

func InitSQLite(ctx context.Context, filename string) {
	var err error
	// var driver sqlite3.SQLiteDriver
	// conn, err := driver.Open(filename)
	db, err = sql.Open("sqlite3", filename)
	if err != nil {
		panic(err)
	}
	if config.Get().P2P.Enable {
		// create p2p element
		p2pHost = p2p.NewP2P(config.Get().P2P.ServiceDiscoveryID) // create p2p
		logrus.Info("Completed P2P Setup")

		// Connect to peers with the chosen discovery method
		switch strings.ToLower(config.Get().P2P.ServiceDiscoverMode) {
		case "announce":
			p2pHost.AnnounceConnect() // KadDHT p2p net create
		case "advertise":
			p2pHost.AdvertiseConnect()
		default:
			p2pHost.AdvertiseConnect()
		}

		logrus.Info("Connected to P2P Service Peers")
		var err error
		p2pPubSub, err = p2p.JoinPubSub(p2pHost, "redis-client", config.Get().P2P.ServiceCommandTopic)
		if err != nil {
			panic(err)
		}
		logrus.Infof("Successfully joined [%s] P2P channel. \n", config.Get().P2P.ServiceCommandTopic)
		asyncSQL(ctx)
	}
}

// 这些语句开头的都需要通过节点同步
var DMLSQL = []string{
	"BEGIN",
	"BEGIN TRANSACTION",
	"COMMIT",
	"END TRANSACTION",
	"ROLLBACK",
	"INSERT",
	"UPDATE",
	"DELETE",
	"CREATE",
	"ALERT",
	"DROP",
}

// 执行DDL、DML语句
func Exec(sql string) (*mysql.Result, error) {
	prefix := sql
	if len(sql) > 20 {
		prefix = sql[:20]
	}
	prefix = strings.ToUpper(prefix)
	for _, v := range DMLSQL {
		if strings.HasPrefix(prefix, v) {
			// dml 语句
			result, err := db.Exec(sql)
			if err != nil {
				return nil, err
			}
			if config.Get().P2P.Enable {
				// 通知对等节点
				p2pPubSub.Outbound <- sql
				logrus.Infof("Outbound sql: %s", sql)
			}

			rf, _ := result.RowsAffected()
			li, _ := result.LastInsertId()
			res := &mysql.Result{
				Status:       2,
				Warnings:     0,
				InsertId:     uint64(li),
				AffectedRows: uint64(rf),
			}
			return res, nil
		}
	}

	// 是查询语句
	resp, err := db.Query(sql, nil)
	if err != nil {
		return nil, err
	}

	column, err := resp.Columns()
	if err != nil {
		return nil, err
	}
	columnType, err := resp.ColumnTypes()
	if err != nil {
		return nil, err
	}
	res := &mysql.Result{
		Resultset: mysql.NewResultset(len(column)),
	}
	res.Status = 31
	// writer Fields
	tableName := []byte(getTableName(sql))
	for k, v := range column {
		f := &mysql.Field{}
		f.Name = []byte(v)
		f.Schema = tableName
		f.Table = tableName
		f.OrgTable = tableName
		f.OrgName = tableName
		f.Charset = 33 // utf8
		// 列类型
		// https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
		f.Type, f.ColumnLength = getColumnTypeAndLen(columnType[k].DatabaseTypeName())
		//mysql.MYSQL_TYPE_VARCHAR
		res.Fields[k] = f
	}

	c := make([]interface{}, len(column)) // 临时存储每行数据
	for index := range c {                // 为每一列初始化一个指针
		var a interface{}
		c[index] = &a
	}

	// writer rows
	for resp.Next() {
		_ = resp.Scan(c...)
		rowData := make([]byte, 0)
		for _, data := range c {
			cv, err := utils.GetString(*data.(*interface{}))
			if err != nil {
				return nil, err
			}
			rowData = append(rowData, uint8(len(cv)))
			rowData = append(rowData, cv...)
		}
		res.RowDatas = append(res.RowDatas, rowData)
	}
	res.AffectedRows = uint64(len(res.RowDatas))
	return res, nil
}

func asyncSQL(ctx context.Context) {
	utils.GoWithRecover(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-p2pPubSub.Inbound:
				_, err := db.Exec(s.Message)
				if err != nil {
					logrus.Infof("Inbound sql: %s err: %v", s, err)
					continue
				}
				logrus.Infof("Inbound sql: %s", s.Message)
			}
		}
	}, func(r interface{}) {
		time.Sleep(time.Second)
		asyncSQL(ctx)
	})
}

func getTableName(sql string) string {
	s := strings.ToLower(sql)

	fromIndex := strings.Index(s, "from")
	if fromIndex == -1 {
		return ""
	}
	behindSQL := strings.Trim(s[fromIndex+4:], " \t")
	spaceIndex := strings.Index(behindSQL, " ")
	if spaceIndex == -1 {
		return behindSQL
	}
	return behindSQL[:spaceIndex]
}

func getColumnTypeAndLen(columnType string) (byte, uint32) {
	index := strings.Index(columnType, "(")
	types := columnType
	var length uint32
	if index > -1 {
		types = columnType[:index]
		l := columnType[index+1 : len(columnType)-1]
		le, _ := strconv.Atoi(l)
		length = uint32(le)
	}
	switch types {
	case "INT":
		return mysql.MYSQL_TYPE_INT24, length
	case "TEXT":
		return mysql.MYSQL_TYPE_BLOB, length
	case "CHAR", "VARCHAR":
		return mysql.MYSQL_TYPE_VARCHAR, length
	case "REAL", "DOUBLE":
		return mysql.MYSQL_TYPE_DOUBLE, length
	case "FLOAT":
		return mysql.MYSQL_TYPE_FLOAT, length
	case "DATE":
		return mysql.MYSQL_TYPE_DATE, length
	case "DATETIME":
		return mysql.MYSQL_TYPE_DATETIME, length
	case "TIMESTAMP":
		return mysql.MYSQL_TYPE_TIMESTAMP, length
	case "DECIMAL", "NUMERIC":
		return mysql.MYSQL_TYPE_DECIMAL, length
	case "BOOLEAN":
		return mysql.MYSQL_TYPE_BIT, length
	case "NULL":
		return mysql.MYSQL_TYPE_NULL, length
	}
	return mysql.MYSQL_TYPE_VAR_STRING, length
}
