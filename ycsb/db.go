package ycsb

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	db   *leveldb.DB
	karr []string
	// kmap map[string]int
)

func init() {
	var err error
	db, err = leveldb.OpenFile("levdb", nil)
	if err != nil {
		panic("open db failed!")
	}
	// kmap = make(map[string]int, KConfig.OriginKeys)
	karr = make([]string, 0, KConfig.OriginKeys)
	t0 := time.Now()
	for i := 0; i <= KConfig.OriginKeys; i++ {
		key := uuid.NewString()
		Write(key, "")
		// kmap[key] = len(karr)
		karr = append(karr, key)
	}
	take := time.Since(t0)
	fmt.Println("take:", take)
}

func Read(key string) string {
	val, _ := db.Get([]byte(key), nil)
	return string(val)
}

func Write(key, val string) {
	db.Put([]byte(key), []byte(val), nil)
}

func Update(key, val string) {
	db.Put([]byte(key), []byte(val), nil)
}

type OpType int

const (
	OpRead OpType = iota
	OpWrite
	NumOfOpType
)

type Op struct {
	Type OpType
	Key  string
	Val  string
}

type Tx struct {
	Ops []Op
}

func getRandomKeyWithHot() string {
	// r := rand.Float64()
	// n := int(float64(len(karr)) * KConfig.HotKey)
	// idx := 0
	// if r < KConfig.HotKeyRate {
	// 	idx = rand.Intn(n)
	// } else {
	// 	idx = rand.Intn(len(karr)-n) + n
	// }
	idx := getNormalRandom()
	return karr[idx]
}

func getNormalRandom() int {
	u := len(karr) / 2
	for {
		x := int(rand.NormFloat64()*KConfig.StdDiff) + u
		if x >= 0 && x < len(karr) {
			return x
		}
	}
}

func GenTxSet() []*Tx {
	n := KConfig.BatchTxNum
	m := KConfig.OpsPerTx
	valFormat := "%0" + strconv.Itoa(KConfig.ValueSize) + "%s"
	wrate := KConfig.WRate
	txs := make([]*Tx, n)
	for i := range txs {
		ops := make([]Op, m)
		for j := range ops {
			r := rand.Float64()
			if r < wrate {
				ops[j].Type = OpWrite
				ops[j].Key = getRandomKeyWithHot()
				ops[j].Val = fmt.Sprintf(valFormat, uuid.NewString())
			} else {
				ops[j].Type = OpRead
				ops[j].Key = getRandomKeyWithHot()
			}
		}
		txs[i] = &Tx{Ops: ops}
	}
	return txs
}

func EncodeTxSet(txs []*Tx) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(txs)
	return buf.Bytes()
}

func ExecTxSet(txSet []byte) int {
	var txs []*Tx
	buf := bytes.NewBuffer(txSet)
	dec := gob.NewDecoder(buf)
	dec.Decode(&txs)

	for _, tx := range txs {
		for _, op := range tx.Ops {
			if op.Type == OpRead {
				Read(op.Key)
			} else {
				Write(op.Key, op.Val)
			}
		}
	}
	return len(txs)
}
