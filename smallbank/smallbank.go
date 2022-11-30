package smallbank

import (
	"math/rand"
	"strconv"
	"zbenchmark/ycsb"

	"github.com/google/uuid"
	"github.com/syndtr/goleveldb/leveldb"
)

type Smallbank struct {
	savings   []string
	checkings []string
	db        *leveldb.DB
}

func (s *Smallbank) TransactSavings(account string, amount int) *ycsb.Tx {
	r := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  account,
	}
	w := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  account,
		Val:  strconv.Itoa(amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{r, w},
	}
}

func (s *Smallbank) DepositChecking(account string, amount int) *ycsb.Tx {
	r := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  account,
	}
	w := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  account,
		Val:  strconv.Itoa(amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{r, w},
	}
}

func (s *Smallbank) SendPayment(accountA string, accountB string, amount int) *ycsb.Tx {
	ra := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  accountA,
	}
	rb := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  accountB,
	}
	wa := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  accountA,
		Val:  strconv.Itoa(-amount),
	}
	wb := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  accountB,
		Val:  strconv.Itoa(amount),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{ra, rb, wa, wb},
	}
}

func (s *Smallbank) Amalgamate(saving string, checking string) *ycsb.Tx {
	ra := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  saving,
	}
	rb := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  checking,
	}
	wa := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  saving,
		Val:  strconv.Itoa(0),
	}
	wb := ycsb.Op{
		Type: ycsb.OpWrite,
		Key:  checking,
		Val:  strconv.Itoa(rand.Intn(10e4) + 10e4),
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{ra, rb, wa, wb},
	}
}

func (s *Smallbank) Query(saving string, checking string) *ycsb.Tx {
	ra := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  saving,
	}
	rb := ycsb.Op{
		Type: ycsb.OpRead,
		Key:  checking,
	}
	return &ycsb.Tx{
		Ops: []ycsb.Op{ra, rb},
	}
}

func (s *Smallbank) GetRandomAccount() string {
	return ""
}

func (s *Smallbank) GetRandomAmount() int {
	return 0
}


func (s *Smallbank) GenTxSet(n int) []*ycsb.Tx {


	return nil
}


// [l, r)
func RandomRange(l, r int) int {
	return rand.Intn(r-l) + l
}

func NewSmallbank(path string, n int) *Smallbank {
	s := &Smallbank{
		savings:   make([]string, n),
		checkings: make([]string, n),
	}
	var err error
	s.db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		panic("open leveldb failed!")
	}
	for i := range s.savings {
		s.savings[i] = uuid.NewString()
		s.checkings[i] = uuid.NewString()
		savingAmount := RandomRange(1e4, 1e5)
		checkingAmount := RandomRange(1e3, 1e4)
		s.db.Put([]byte(s.savings[i]), []byte(strconv.Itoa(savingAmount)), nil)
		s.db.Put([]byte(s.checkings[i]), []byte(strconv.Itoa(checkingAmount)), nil)
	}
	return s
}
