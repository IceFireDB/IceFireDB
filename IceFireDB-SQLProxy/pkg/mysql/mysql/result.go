package mysql

type Result struct {
	Status   uint16 // 服务器状态
	Warnings uint16 // 告警计数

	InsertId     uint64 // 索引id值
	AffectedRows uint64 // 影响行数

	*Resultset
}

type Executer interface {
	Execute(query string, args ...interface{}) (*Result, error)
}

func (r *Result) Close() {
	if r.Resultset != nil {
		r.Resultset.returnToPool()
		r.Resultset = nil
	}
}
