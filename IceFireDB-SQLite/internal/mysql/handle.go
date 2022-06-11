package mysql

import (
	"errors"
	"github.com/IceFireDB/IceFireDB-SQLite/internal/sqlite"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/mysql"
	"github.com/IceFireDB/IceFireDB-SQLite/pkg/mysql/server"
	"log"
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
	return sqlite.Exec(query)
}

// 获取数据表字段
func (h *mysqlProxy) HandleFieldList(c *server.Conn, table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

// 预处理查询语句 初始化
func (h *mysqlProxy) HandleStmtPrepare(c *server.Conn, query string) (int, int, interface{}, error) {
	return 0, 0, nil, nil
}

// 执行预处理查询
func (h *mysqlProxy) HandleStmtExecute(c *server.Conn, context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	return nil, nil
}

// 关闭预处理占用的内存
func (h *mysqlProxy) HandleStmtClose(c *server.Conn, context interface{}) error {
	return nil
}

// 其他sql命令
func (h *mysqlProxy) HandleOtherCommand(c *server.Conn, cmd byte, data []byte) error {
	return errors.New("command %d is not supported now" + string(cmd))
}
