package mysql

import (
	"errors"
	"github.com/IceFireDB/IceFireDB-SQLite/internal/sqlite"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/client"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/server"
	"log"
	"strings"
)

func (h *mysqlProxy) CloseConn(c *server.Conn) error {
	if c.IsInTransaction() || c.IsAutoCommit() {
		// 自动提交事务
		_, err := sqlite.Exec("COMMIT")
		return err
	}
	return nil
}

// 选择数据库
func (h *mysqlProxy) UseDB(c *server.Conn, dbName string) error {
	// 标记客户端选择数据库
	log.Println(dbName)
	return nil
}

// 直接查询操作
func (h *mysqlProxy) HandleQuery(c *server.Conn, query string) (res *mysql.Result, err error) {
	if strings.ToUpper(query[:3]) == "SET" {
		res = &mysql.Result{
			Status: 2,
		}
		return
	}
	if strings.ToUpper(query) == "SELECT VERSION()" {
		query = "SELECT SQLITE_VERSION()"
		//query = "SELECT '5.7.30-log' AS 'VERSION()'"
	}
	if strings.ToUpper(query) == "SELECT NOW()" {
		query = "SELECT STRFTIME('%Y-%m-%d %H:%M:%S','now') AS 'NOW()'"
	}
	//if strings.ToUpper(query) == "START TRANSACTION" {
	//	res = &mysql.Result{
	//		Status: 2,
	//	}
	//	return
	//}
	return sqlite.Exec(query)
}

// 获取数据表字段
func (h *mysqlProxy) HandleFieldList(c *server.Conn, table string, fieldWildcard string) ([]*mysql.Field, error) {
	//return h.conn.FieldList(table, fieldWildcard)
	return nil, nil
}

// 预处理查询语句 初始化
func (h *mysqlProxy) HandleStmtPrepare(c *server.Conn, query string) (int, int, interface{}, error) {
	//stmt, err := h.db.Prepare(query)
	//if err != nil {
	//	return 0, 0, nil, err
	//}
	//return 1, 0, stmt, nil
	return 0, 0, nil, nil
}

// 执行预处理查询
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

// 关闭预处理占用的内存
func (h *mysqlProxy) HandleStmtClose(c *server.Conn, context interface{}) error {
	stmt, ok := context.(*client.Stmt)
	if !ok {
		return errors.New("other error")
	}
	return stmt.Close()
	//return nil
}

// 其他sql命令
func (h *mysqlProxy) HandleOtherCommand(c *server.Conn, cmd byte, data []byte) error {
	return errors.New("command %d is not supported now" + string(cmd))
}
