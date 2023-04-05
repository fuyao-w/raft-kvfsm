package raft_kvfsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	. "github.com/fuyao-w/common-util"
	raft "github.com/fuyao-w/go-raft"
	"github.com/vmihailenco/msgpack/v5"
	"io"
	"math"
	"sync"
)

var version1 = []byte("1")

// KvSchema [key ,value]
type KvSchema [3]string

func (s KvSchema) Encode(k, v string) []byte {
	s[0], s[1] = k, v
	b, _ := json.Marshal(s)
	return b
}
func (s KvSchema) Decode(data []byte) (k, v string) {
	_ = json.Unmarshal(data, &s)
	return s[0], s[1]
}

// KvFSM 内存的 kv ，通过紧凑格式存储快照
type KvFSM struct {
	kv            *sync.Map
	configuration []raft.Configuration
}

type KvSnapshot struct {
	fsm *KvFSM
}

func NewKvFsm() *KvFSM {
	return &KvFSM{
		kv: new(sync.Map),
	}
}
func (k *KvFSM) StoreConfiguration(_ uint64, configuration interface{}) {
	conf := configuration.(raft.Configuration)
	k.configuration = append(k.configuration, conf)
	if len(k.configuration) > 2 {
		k.configuration = k.configuration[len(k.configuration)-2:]
	}
}

// Persist 协议格式 ：版本号 1 byte + 配置长度 4 byte + 配置 + key 长度 4 byte + key + value 长度 4 byte + value
func (k *KvSnapshot) Persist(sink raft.SnapShotSink) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err, _ = p.(error)
		}
	}()
	var (
		buffer = bufio.NewWriter(sink)
		write  = func(buf []byte) {
			_, err = buffer.Write(buf)
			if err != nil {
				panic(err)
			}
		}
		fsm = k.fsm
	)
	write(version1)
	data, _ := msgpack.Marshal(func() interface{} {
		if length := len(fsm.configuration); length > 0 {
			return &fsm.configuration[length-1]
		}
		return nil
	}())
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(uint64(len(data))))
	write(buf)
	write(data)

	keyBuf, valBuf := make([]byte, 4), make([]byte, 4)

	k.fsm.kv.Range(func(key, value any) bool {
		binary.BigEndian.PutUint32(keyBuf, uint32(len(key.(string))))
		write(keyBuf)
		write(Str2Bytes(key.(string)))
		binary.BigEndian.PutUint32(valBuf, uint32(len(value.(string))))
		write(valBuf)
		write(Str2Bytes(value.(string)))
		return true
	})
	return buffer.Flush()
}

func (k *KvSnapshot) Release() {
	return
}

func (k *KvFSM) Apply(entry *raft.LogEntry) interface{} {
	key, val := KvSchema{}.Decode(entry.Data)
	if len(key) > math.MaxUint32 {
		return errors.New("key size over flow")
	}
	if len(val) > math.MaxUint32 {
		return errors.New("value size over flow")
	}
	k.kv.Store(key, val)
	return nil
}

func (k *KvFSM) Snapshot() (raft.FSMSnapShot, error) {
	return &KvSnapshot{k}, nil
}

func (k *KvFSM) ReStore(rc io.ReadCloser) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err, _ = p.(error)
		}
	}()
	var (
		r     = bufio.NewReader(rc)
		toStr = func(buf []byte) string {
			return Bytes2Str(bytes.Clone(buf))
		}
		read = func(buf []byte) (n int) {
			n, err = r.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					panic(nil)
				}
				panic(err)
			}
			if n != len(buf) {
				err = errors.New("read not enough")
				panic(err)
			}
			return
		}
		filedBuf  = make([]byte, 4)
		readFiled = func() []byte {
			read(filedBuf)
			fieldLength := binary.BigEndian.Uint32(filedBuf)
			data := make([]byte, fieldLength)
			read(data)
			return data
		}
	)

	_, err = r.ReadByte()
	if err != nil {
		panic(err)
	}

	data := readFiled()
	var conf raft.Configuration
	if msgpack.Unmarshal(data, &conf) != nil {
		k.configuration = append(k.configuration, conf)
	}

	for {
		key := readFiled()
		val := readFiled()
		k.kv.Store(toStr(key), toStr(val))
	}
}
