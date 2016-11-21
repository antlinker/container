package container

// CBuckets 多桶容器
type CBuckets interface {
	Open() (<-chan CBucket, error)
	Push(ele interface{}) error
	Close()
	Len() int
}

// CBucket 单桶容器
type CBucket interface {
	Pop() (interface{}, error)
	Len() int
	ToSlice() ([]interface{}, error)
}
