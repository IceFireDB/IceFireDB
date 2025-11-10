package mysql

import (
	"errors"
	"log"
	"strings"

	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/internal/sqlite"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB/IceFireDB-SQLite/pkg/mysql/server"
)

func (h *mysqlProxy) CloseConn(c *server.Conn) error {
	if c.IsInTransaction() || c.IsAutoCommit() {
		_, err := sqlite.Exec("COMMIT")
		return err
	}
	return nil
}

func (h *mysqlProxy) UseDB(c *server.Conn, dbName string) error {
	log.Println(dbName)
	return nil
}

func (h *mysqlProxy) HandleQuery(c *server.Conn, query string) (res *mysql.Result, err error) {
	if len(query) >= 3 && strings.EqualFold(query[:3], "SET") {
		res = &mysql.Result{
			Status: 2,
		}
		return
	}
	if strings.EqualFold(query, "SELECT VERSION()") {
		query = "SELECT SQLITE_VERSION()"
	}
	if strings.EqualFold(query, "SELECT NOW()") {
		query = "SELECT STRFTIME('%Y-%m-%d %H:%M:%S','now') AS 'NOW()'"
	}
	return sqlite.Exec(query)
}

func (h *mysqlProxy) HandleFieldList(c *server.Conn, table string, fieldWildcard string) ([]*mysql.Field, error) {
	//return h.conn.FieldList(table, fieldWildcard)
	return nil, nil
}

func (h *mysqlProxy) HandleStmtPrepare(c *server.Conn, query string) (int, int, interface{}, error) {
	//stmt, err := h.db.Prepare(query)
	//if err != nil {
	//	return 0, 0, nil, err
	//}
	//return 1, 0, stmt, nil
	return 0, 0, nil, nil
}

func (h *mysqlProxy) HandleStmtExecute(c *server.Conn, context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	stmt, ok := context.(*client.Stmt)
	//stmt, ok := context.(*sql.Stmt)
	if !ok {
		return nil, errors.New("other error")
	}
	res, err := stmt.Execute(args...)
	if err == nil {
		sqlite.Broadcast(query)
	}
	return res, err
}

func (h *mysqlProxy) HandleStmtClose(c *server.Conn, context interface{}) error {
	stmt, ok := context.(*client.Stmt)
	if !ok {
		return errors.New("other error")
	}
	return stmt.Close()
	//return nil
}

func (h *mysqlProxy) HandleOtherCommand(c *server.Conn, cmd byte, data []byte) error {
	return errors.New("command %d is not supported now" + string(cmd))
}
