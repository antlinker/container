package container

type CBuckets interface {
	Open() (<-chan CBucket, error)
	Push(ele interface{}) error
	Close()
	Len() int
}

type CBucket interface {
	Pop() (interface{}, error)
	Len() int
	ToSlice() ([]interface{}, error)
}
