package kvql

type Txn interface {
	Get(key []byte) (value []byte, err error)
	Put(key []byte, value []byte) error
	BatchPut(kvs []KVPair) error
	Delete(key []byte) error
	BatchDelete(keys [][]byte) error
	Cursor() (cursor Cursor, err error)
}

type Cursor interface {
	Seek(prefix []byte) error
	Next() (key []byte, value []byte, err error)
}

type KVPair struct {
	Key   []byte
	Value []byte
}

func NewKVP(key []byte, val []byte) KVPair {
	return KVPair{
		Key:   key,
		Value: val,
	}
}

func NewKVPStr(key string, val string) KVPair {
	return KVPair{
		Key:   []byte(key),
		Value: []byte(val),
	}
}
